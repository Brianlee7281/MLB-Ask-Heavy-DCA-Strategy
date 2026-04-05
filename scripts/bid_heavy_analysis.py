"""bid_heavy reverse signal analysis on 121-game replay cache.

Tests the symmetric hypothesis: if ask_heavy (depth_ratio < 0.4) produces
edge buying YES, then bid_heavy (depth_ratio > 0.6) should produce edge
buying NO.

Signal definitions:
    ask_heavy:  depth_ratio < 0.4  -> buy YES at mid  -> PnL = settlement - mid
    bid_heavy:  depth_ratio > 0.6  -> buy NO  at mid  -> PnL = mid - settlement

Usage:
    PYTHONPATH=. python scripts/bid_heavy_analysis.py
    PYTHONPATH=. python scripts/bid_heavy_analysis.py --cache data/models/replay_cache_v2025.1.joblib
    PYTHONPATH=. python scripts/bid_heavy_analysis.py --threshold 0.7
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.strategy_backtest import (
    ASK_HEAVY_THRESHOLD,
    DEFAULT_CACHE,
    N_BOOTSTRAP,
    bootstrap_ci,
    build_game_df,
    get_game_date,
    get_inning_at_ts,
    load_games,
    _ts_to_unix,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BID_HEAVY_THRESHOLD = 0.6
BID_HEAVY_STRICT = 0.7
BID_HEAVY_LOOSE = 0.5
SWEEP_THRESHOLDS = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80]
DCA_CAP_LEVELS = [50, 100, 500, 1000, float("inf")]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sig(ci_lo: float) -> str:
    return "YES" if ci_lo > 0 else "NO"


def _sort_key(g: dict) -> tuple[int, int]:
    ticks = g.get("ticks", [])
    if ticks:
        return (_ts_to_unix(ticks[0]["game_state"]["timestamp"]), g["game_pk"])
    return (0, g["game_pk"])


def clustered_bootstrap_ci(
    values: np.ndarray,
    cluster_ids: np.ndarray,
    n_boot: int = N_BOOTSTRAP,
) -> tuple[float, float]:
    """95% CI via cluster bootstrap (resample clusters, not observations)."""
    if len(values) < 3:
        return (0.0, 0.0)

    unique_ids = np.unique(cluster_ids)
    n_clusters = len(unique_ids)
    if n_clusters < 3:
        return (0.0, 0.0)

    # Pre-group observations by cluster for speed
    cluster_means = {}
    for cid in unique_ids:
        mask = cluster_ids == cid
        cluster_means[cid] = float(values[mask].mean())

    cluster_ids_arr = np.array(list(cluster_means.keys()))
    cluster_means_arr = np.array(list(cluster_means.values()))

    rng = np.random.default_rng(42)
    boot_means = np.empty(n_boot)
    for i in range(n_boot):
        idx = rng.integers(0, n_clusters, size=n_clusters)
        boot_means[i] = cluster_means_arr[idx].mean()

    return (float(np.percentile(boot_means, 2.5)),
            float(np.percentile(boot_means, 97.5)))


def build_all_observations(
    games: list[dict],
) -> pd.DataFrame:
    """Build a single DataFrame of all valid observations across all games."""
    frames = []
    for game in sorted(games, key=_sort_key):
        df = build_game_df(game)
        if df is None:
            continue
        df["game_pk"] = game["game_pk"]
        df["home_won"] = game["home_won"]
        df["settlement"] = 1.0 if game["home_won"] else 0.0
        frames.append(df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Section 1: Observation-Level Edge Comparison
# ---------------------------------------------------------------------------


def section_1(obs: pd.DataFrame, threshold: float) -> None:
    print("=" * 78)
    print("SECTION 1: Observation-Level Edge Comparison")
    print("=" * 78)
    print()

    rows = []

    # ask_heavy: buy YES -> PnL = settlement - mid
    ah_mask = obs["depth_ratio"] < ASK_HEAVY_THRESHOLD
    ah = obs[ah_mask]
    ah_pnl = (ah["settlement"] - ah["mid"]).values
    ah_ci = clustered_bootstrap_ci(ah_pnl, ah["game_pk"].values)
    rows.append(("ask_heavy", f"dr < {ASK_HEAVY_THRESHOLD}", len(ah),
                 ah_pnl.mean(), ah_ci, _sig(ah_ci[0])))

    # bid_heavy: buy NO -> PnL = mid - settlement
    bh_mask = obs["depth_ratio"] > threshold
    bh = obs[bh_mask]
    bh_pnl = (bh["mid"] - bh["settlement"]).values
    bh_ci = clustered_bootstrap_ci(bh_pnl, bh["game_pk"].values)
    rows.append(("bid_heavy", f"dr > {threshold}", len(bh),
                 bh_pnl.mean(), bh_ci, _sig(bh_ci[0])))

    # bid_heavy strict
    bhs_mask = obs["depth_ratio"] > BID_HEAVY_STRICT
    bhs = obs[bhs_mask]
    bhs_pnl = (bhs["mid"] - bhs["settlement"]).values
    bhs_ci = clustered_bootstrap_ci(bhs_pnl, bhs["game_pk"].values)
    rows.append(("bid_heavy_strict", f"dr > {BID_HEAVY_STRICT}", len(bhs),
                 bhs_pnl.mean() if len(bhs) else 0.0, bhs_ci, _sig(bhs_ci[0])))

    # bid_heavy loose
    bhl_mask = obs["depth_ratio"] > BID_HEAVY_LOOSE
    bhl = obs[bhl_mask]
    bhl_pnl = (bhl["mid"] - bhl["settlement"]).values
    bhl_ci = clustered_bootstrap_ci(bhl_pnl, bhl["game_pk"].values)
    rows.append(("bid_heavy_loose", f"dr > {BID_HEAVY_LOOSE}", len(bhl),
                 bhl_pnl.mean(), bhl_ci, _sig(bhl_ci[0])))

    # neutral: 0.4 <= dr <= 0.6, show YES PnL for reference
    neut_mask = (obs["depth_ratio"] >= ASK_HEAVY_THRESHOLD) & (obs["depth_ratio"] <= threshold)
    neut = obs[neut_mask]
    neut_pnl = (neut["settlement"] - neut["mid"]).values
    neut_ci = clustered_bootstrap_ci(neut_pnl, neut["game_pk"].values)
    rows.append(("neutral", f"{ASK_HEAVY_THRESHOLD}-{threshold}", len(neut),
                 neut_pnl.mean(), neut_ci, _sig(neut_ci[0])))

    header = (f"{'Signal':<18s} {'Threshold':<14s} {'N obs':>9s} "
              f"{'Mean PnL':>10s} {'95% CI':>23s} {'Sig':>3s}")
    print(header)
    print("\u2500" * len(header))
    for name, thresh_str, n, mean_pnl, ci, sig in rows:
        print(f"{name:<18s} {thresh_str:<14s} {n:>9,d} "
              f"{mean_pnl:>+10.4f} [{ci[0]:+.4f}, {ci[1]:+.4f}] {sig:>3s}")

    print()
    print("ask_heavy PnL = settlement - mid  (buy YES)")
    print("bid_heavy PnL = mid - settlement  (buy NO)")
    print("neutral PnL   = not applicable (no trade, shown for reference as YES PnL)")
    print()


# ---------------------------------------------------------------------------
# Section 2: bid_heavy Threshold Sweep
# ---------------------------------------------------------------------------


def section_2(obs: pd.DataFrame) -> None:
    print("=" * 78)
    print("SECTION 2: bid_heavy Threshold Sweep")
    print("=" * 78)
    print()

    total_obs = len(obs)

    header = (f"{'Threshold':<12s} {'N obs':>9s} {'% of total':>10s} "
              f"{'Mean PnL':>10s} {'95% CI':>23s} {'Sig':>3s}")
    print(header)
    print("\u2500" * len(header))

    for thresh in SWEEP_THRESHOLDS:
        mask = obs["depth_ratio"] > thresh
        sub = obs[mask]
        n = len(sub)
        if n == 0:
            print(f"dr > {thresh:.2f}   {0:>9d} {0:>9.1f}%    {'N/A':>10s}")
            continue
        pnl = (sub["mid"] - sub["settlement"]).values
        ci = clustered_bootstrap_ci(pnl, sub["game_pk"].values)
        pct = n / total_obs * 100
        print(f"dr > {thresh:.2f}   {n:>9,d} {pct:>9.1f}% "
              f"{pnl.mean():>+10.4f} [{ci[0]:+.4f}, {ci[1]:+.4f}] {_sig(ci[0]):>3s}")

    print()


# ---------------------------------------------------------------------------
# Section 3: Home Bias Independence (bid_heavy)
# ---------------------------------------------------------------------------


def section_3(obs: pd.DataFrame, threshold: float) -> None:
    print("=" * 78)
    print(f"SECTION 3: Home Bias Independence (bid_heavy, dr > {threshold})")
    print("=" * 78)
    print()

    bh_mask = obs["depth_ratio"] > threshold
    home_fav_mask = obs["mid"] > 0.50

    cells = [
        ("bid_heavy + home_favored (mid>0.50)", bh_mask & home_fav_mask),
        ("bid_heavy + away_favored (mid<0.50)", bh_mask & ~home_fav_mask),
        ("NOT bid_heavy + home_favored", ~bh_mask & home_fav_mask),
        ("NOT bid_heavy + away_favored", ~bh_mask & ~home_fav_mask),
    ]

    header = (f"{'Cell':<44s} {'N obs':>7s} {'NO Buyer PnL':>12s} "
              f"{'95% CI':>23s} {'Sig':>3s}")
    print(header)
    print("\u2500" * len(header))

    for label, mask in cells:
        sub = obs[mask]
        n = len(sub)
        if n == 0:
            print(f"{label:<44s} {0:>7d}")
            continue
        # NO buyer PnL = mid - settlement for bid_heavy cells
        # For NOT bid_heavy cells, show same metric for comparison
        pnl = (sub["mid"] - sub["settlement"]).values
        ci = clustered_bootstrap_ci(pnl, sub["game_pk"].values)
        print(f"{label:<44s} {n:>7,d} {pnl.mean():>+12.4f} "
              f"[{ci[0]:+.4f}, {ci[1]:+.4f}] {_sig(ci[0]):>3s}")

    print()
    print("If bid_heavy is pure home bias:")
    print("  -> bid_heavy + home_favored should be POSITIVE (home overpriced, NO is good)")
    print("  -> bid_heavy + away_favored should be ZERO or NEGATIVE")
    print("If bid_heavy is microstructure:")
    print("  -> Both cells should be POSITIVE")
    print()


# ---------------------------------------------------------------------------
# Section 4: Game-Level DCA Backtest (bid_heavy)
# ---------------------------------------------------------------------------


def run_bid_heavy_dca(
    games: list[dict],
    threshold: float = BID_HEAVY_THRESHOLD,
    per_entry: float = 1.0,
    game_cap: float = 1000.0,
    quiet: bool = False,
) -> list[dict]:
    """DCA strategy for bid_heavy: buy 1 NO contract per bid_heavy observation.

    PnL per contract = mid - settlement.
    """
    games_sorted = sorted(games, key=_sort_key)

    trades: list[dict] = []
    skipped_no_depth = 0
    skipped_no_signal = 0

    for game in games_sorted:
        gpk = game["game_pk"]
        settlement = 1.0 if game["home_won"] else 0.0
        home_won = int(game["home_won"])

        df = build_game_df(game)
        if df is None:
            skipped_no_depth += 1
            continue

        entries: list[dict] = []
        total_contracts = 0.0

        for _, row in df.iterrows():
            if row["depth_ratio"] > threshold:
                if total_contracts + per_entry > game_cap:
                    break
                entries.append({
                    "mid": float(row["mid"]),
                    "timestamp": int(row["timestamp"]),
                    "spread": float(row["spread"]),
                    "depth_ratio": float(row["depth_ratio"]),
                })
                total_contracts += per_entry

        if not entries:
            skipped_no_signal += 1
            continue

        avg_mid = float(np.mean([e["mid"] for e in entries]))
        pnl_per_dollar = avg_mid - settlement
        game_pnl = pnl_per_dollar * total_contracts

        first_entry_ts = entries[0]["timestamp"]
        last_entry_ts = entries[-1]["timestamp"]
        entry_inning = get_inning_at_ts(game, first_entry_ts)

        trades.append({
            "game_pk": gpk,
            "game_date": get_game_date(game),
            "n_entries": len(entries),
            "total_invested": total_contracts,
            "game_cap": game_cap,
            "hit_cap": total_contracts >= game_cap - per_entry,
            "avg_entry_mid": avg_mid,
            "first_entry_mid": entries[0]["mid"],
            "last_entry_mid": entries[-1]["mid"],
            "min_entry_mid": min(e["mid"] for e in entries),
            "max_entry_mid": max(e["mid"] for e in entries),
            "avg_spread": float(np.mean([e["spread"] for e in entries])),
            "avg_depth_ratio": float(np.mean([e["depth_ratio"] for e in entries])),
            "entry_duration_seconds": last_entry_ts - first_entry_ts,
            "entry_inning": entry_inning,
            "home_favored_at_avg": avg_mid > 0.50,
            "home_won": home_won,
            "settlement": settlement,
            "pnl_per_dollar": pnl_per_dollar,
            "game_pnl": game_pnl,
        })

    if not quiet:
        cap_str = f"${game_cap:.0f}" if game_cap < float("inf") else "unlimited"
        print(f"bid_heavy DCA (cap={cap_str}, dr>{threshold}): "
              f"{len(trades)} trades from {len(games)} games")
        print(f"  Skipped (no depth data): {skipped_no_depth}")
        print(f"  Skipped (no bid_heavy signal): {skipped_no_signal}")
        print()

    return trades


def section_4(games: list[dict], threshold: float) -> dict[float, list[dict]]:
    print("=" * 78)
    print(f"SECTION 4: Game-Level DCA Backtest (bid_heavy, dr > {threshold})")
    print("=" * 78)
    print()

    print(f"Entry: 1 contract per bid_heavy observation (buy NO at 1-mid)")
    print(f"PnL per contract = mid - settlement")
    print(f"DCA: accumulate while bid_heavy active, cap at N contracts")
    print()

    sweep: dict[float, list[dict]] = {}
    for cap in DCA_CAP_LEVELS:
        sweep[cap] = run_bid_heavy_dca(games, threshold=threshold,
                                       game_cap=cap, quiet=True)

    header = (f"{'Cap':>7s}  {'N':>3s}  {'Mean inv':>9s}  {'PnL/$1':>8s}  "
              f"{'Sharpe':>6s}  {'Max DD$':>8s}  {'95% CI':>19s}  {'Sig':>3s}")
    print(header)
    print("-" * len(header))

    for cap in DCA_CAP_LEVELS:
        trades = sweep[cap]
        n = len(trades)
        if n == 0:
            continue

        pnls = np.array([t["pnl_per_dollar"] for t in trades])
        game_pnls = np.array([t["game_pnl"] for t in trades])
        invested = np.array([t["total_invested"] for t in trades])
        cum_dollar = np.cumsum(game_pnls)
        peak = np.maximum.accumulate(cum_dollar)
        max_dd = float((cum_dollar - peak).min())

        sharpe = float(pnls.mean() / pnls.std()) if pnls.std() > 0 else 0.0
        ci_lo, ci_hi = bootstrap_ci(pnls)
        sig = _sig(ci_lo)

        label = "unlim" if cap == float("inf") else f"${cap:.0f}"
        print(
            f"{label:>7s}  {n:>3d}  ${invested.mean():>7.2f}  {pnls.mean():>+8.4f}  "
            f"{sharpe:>6.3f}  {max_dd:>+8.2f}  "
            f"[{ci_lo:+.4f}, {ci_hi:+.4f}]  {sig:>3s}"
        )

    print()
    return sweep


# ---------------------------------------------------------------------------
# Section 5: Signal Overlap Analysis
# ---------------------------------------------------------------------------


def section_5(
    games: list[dict],
    obs: pd.DataFrame,
    threshold: float,
) -> None:
    print("=" * 78)
    print("SECTION 5: Signal Overlap Analysis")
    print("=" * 78)
    print()

    all_game_pks = set()
    for game in games:
        df = build_game_df(game)
        if df is not None:
            all_game_pks.add(game["game_pk"])

    total_games = len(all_game_pks)

    ah_games = set(obs[obs["depth_ratio"] < ASK_HEAVY_THRESHOLD]["game_pk"].unique())
    bh_games = set(obs[obs["depth_ratio"] > threshold]["game_pk"].unique())

    both = ah_games & bh_games
    ah_only = ah_games - bh_games
    bh_only = bh_games - ah_games
    neither = all_game_pks - ah_games - bh_games

    print(f"Games with ask_heavy only:          {len(ah_only):>3d} ({len(ah_only)/total_games*100:.1f}%)")
    print(f"Games with bid_heavy only:          {len(bh_only):>3d} ({len(bh_only)/total_games*100:.1f}%)")
    print(f"Games with both signals:            {len(both):>3d} ({len(both)/total_games*100:.1f}%)")
    print(f"Games with neither signal:          {len(neither):>3d} ({len(neither)/total_games*100:.1f}%)")
    print()

    if both:
        print("In games with both signals:")

        # Per-game stats for overlap games
        ah_obs_counts = []
        bh_obs_counts = []
        overlap_obs = 0
        total_bh_obs_in_both = 0

        for gpk in both:
            game_obs = obs[obs["game_pk"] == gpk]
            ah_obs = game_obs[game_obs["depth_ratio"] < ASK_HEAVY_THRESHOLD]
            bh_obs = game_obs[game_obs["depth_ratio"] > threshold]
            ah_obs_counts.append(len(ah_obs))
            bh_obs_counts.append(len(bh_obs))
            total_bh_obs_in_both += len(bh_obs)

            # Check for temporal overlap: obs that are both ask_heavy AND bid_heavy
            # (impossible since depth_ratio can't be < 0.4 AND > 0.6 simultaneously)
            # Instead, check if signals interleave in time
            ah_ts = set(ah_obs["timestamp"].values)
            bh_ts = set(bh_obs["timestamp"].values)
            # Simultaneous = same timestamp (can't happen with exclusive thresholds)
            overlap_obs += len(ah_ts & bh_ts)

        # Time gap between signals
        time_gaps = []
        for gpk in both:
            game_obs = obs[obs["game_pk"] == gpk]
            ah_obs = game_obs[game_obs["depth_ratio"] < ASK_HEAVY_THRESHOLD]
            bh_obs = game_obs[game_obs["depth_ratio"] > threshold]
            if len(ah_obs) > 0 and len(bh_obs) > 0:
                ah_center = ah_obs["timestamp"].median()
                bh_center = bh_obs["timestamp"].median()
                time_gaps.append(abs(bh_center - ah_center))

        print(f"  Mean ask_heavy obs:               {np.mean(ah_obs_counts):.0f}")
        print(f"  Mean bid_heavy obs:               {np.mean(bh_obs_counts):.0f}")
        overlap_pct = overlap_obs / total_bh_obs_in_both * 100 if total_bh_obs_in_both > 0 else 0
        overlap_yn = "YES" if overlap_obs > 0 else "NO"
        print(f"  Do signals overlap in time?       {overlap_yn} "
              f"({overlap_pct:.1f}% of obs where both active simultaneously)")

        if time_gaps:
            mean_gap = np.mean(time_gaps)
            gap_min = int(mean_gap // 60)
            gap_sec = int(mean_gap % 60)
            print(f"  Mean time gap between signals:    {gap_min}m {gap_sec:02d}s")
        print()

        # PnL in overlap games
        # ask_heavy DCA PnL in these games
        ah_pnls_both = []
        bh_pnls_both = []
        for gpk in both:
            game_obs = obs[obs["game_pk"] == gpk]
            settlement = game_obs["settlement"].iloc[0]
            ah_obs = game_obs[game_obs["depth_ratio"] < ASK_HEAVY_THRESHOLD]
            bh_obs = game_obs[game_obs["depth_ratio"] > threshold]
            if len(ah_obs) > 0:
                ah_pnls_both.append(settlement - ah_obs["mid"].mean())
            if len(bh_obs) > 0:
                bh_pnls_both.append(bh_obs["mid"].mean() - settlement)

        ah_mean = np.mean(ah_pnls_both) if ah_pnls_both else 0.0
        bh_mean = np.mean(bh_pnls_both) if bh_pnls_both else 0.0
        combined = ah_mean + bh_mean

        print(f"  ask_heavy DCA PnL in these games: {ah_mean:+.4f}")
        print(f"  bid_heavy DCA PnL in these games: {bh_mean:+.4f}")
        print(f"  Combined PnL:                     {combined:+.4f}")

    print()


# ---------------------------------------------------------------------------
# Section 6: Combined Strategy Simulation
# ---------------------------------------------------------------------------


def section_6(
    games: list[dict],
    obs: pd.DataFrame,
    threshold: float,
) -> None:
    print("=" * 78)
    print("SECTION 6: Combined Strategy (ask_heavy YES + bid_heavy NO)")
    print("=" * 78)
    print()

    # Build per-game PnL for each strategy (uncapped for simplicity / use $1000 cap)
    from scripts.strategy_backtest import run_strategy_dca_variable

    ah_trades = run_strategy_dca_variable(games, game_cap=1000.0, quiet=True)
    bh_trades = run_bid_heavy_dca(games, threshold=threshold, game_cap=1000.0, quiet=True)

    ah_by_game = {t["game_pk"]: t for t in ah_trades}
    bh_by_game = {t["game_pk"]: t for t in bh_trades}

    all_gpks = set(ah_by_game.keys()) | set(bh_by_game.keys())

    # Build combined PnL per game
    combined_pnls = []
    ah_only_pnls = []
    bh_only_pnls = []

    for gpk in sorted(all_gpks):
        ah_pnl = ah_by_game[gpk]["game_pnl"] if gpk in ah_by_game else 0.0
        bh_pnl = bh_by_game[gpk]["game_pnl"] if gpk in bh_by_game else 0.0
        combined_pnls.append(ah_pnl + bh_pnl)
        if gpk in ah_by_game:
            ah_only_pnls.append(ah_by_game[gpk]["game_pnl"])
        if gpk in bh_by_game:
            bh_only_pnls.append(bh_by_game[gpk]["game_pnl"])

    ah_arr = np.array(ah_only_pnls) if ah_only_pnls else np.array([0.0])
    bh_arr = np.array(bh_only_pnls) if bh_only_pnls else np.array([0.0])
    comb_arr = np.array(combined_pnls) if combined_pnls else np.array([0.0])

    def _sharpe(a: np.ndarray) -> float:
        return float(a.mean() / a.std()) if a.std() > 0 else 0.0

    def _max_dd(a: np.ndarray) -> float:
        cum = np.cumsum(a)
        peak = np.maximum.accumulate(cum)
        return float((cum - peak).min())

    # Correlation between ah and bh game PnLs for games with both
    both_gpks = set(ah_by_game.keys()) & set(bh_by_game.keys())
    if len(both_gpks) >= 3:
        ah_both = np.array([ah_by_game[g]["game_pnl"] for g in sorted(both_gpks)])
        bh_both = np.array([bh_by_game[g]["game_pnl"] for g in sorted(both_gpks)])
        corr = float(np.corrcoef(ah_both, bh_both)[0, 1])
    else:
        corr = float("nan")

    header = f"{'':28s} {'ask_heavy only':>15s} {'bid_heavy only':>15s} {'Combined':>15s}"
    print(header)

    print(f"{'Games entered':<28s} {len(ah_trades):>15d} {len(bh_trades):>15d} {len(all_gpks):>15d}")
    print(f"{'Mean PnL per game ($)':<28s} ${ah_arr.mean():>+13.2f} ${bh_arr.mean():>+13.2f} ${comb_arr.mean():>+13.2f}")
    print(f"{'Total PnL (121 games)':<28s} ${ah_arr.sum():>+13.2f} ${bh_arr.sum():>+13.2f} ${comb_arr.sum():>+13.2f}")
    print(f"{'Sharpe (game-level)':<28s} {_sharpe(ah_arr):>15.3f} {_sharpe(bh_arr):>15.3f} {_sharpe(comb_arr):>15.3f}")

    corr_str = f"{corr:.3f}" if not np.isnan(corr) else "N/A"
    print(f"{'Correlation (game PnLs)':<28s} {'':>15s} {'':>15s} {corr_str:>15s}")
    print(f"{'Max drawdown':<28s} ${_max_dd(ah_arr):>+13.2f} ${_max_dd(bh_arr):>+13.2f} ${_max_dd(comb_arr):>+13.2f}")

    print()
    print("NOTE: ask_heavy buys YES, bid_heavy buys NO. In the same game, these")
    print("partially hedge each other. If both signals fire and the game outcome")
    print("matches one, the other side loses -- but entry prices differ so PnL")
    print("doesn't perfectly cancel.")
    print()


# ---------------------------------------------------------------------------
# Section 7: Kalshi NO-Side Feasibility Check
# ---------------------------------------------------------------------------


def section_7() -> None:
    print("=" * 78)
    print("SECTION 7: Kalshi NO-Side Feasibility")
    print("=" * 78)
    print()

    print("Questions to verify (from docs/data_sources.md and Kalshi API docs):")
    print("  1. Can we place limit orders to buy NO?           YES")
    print("     -> Kalshi supports YES and NO limit orders on all binary markets.")
    print("  2. Maker fee for NO limit orders?                  $0.00")
    print("     -> Maker fee is $0 for all limit orders (YES and NO).")
    print("  3. NO orderbook depth available via API?           YES")
    print("     -> REST: orderbook_fp.no_dollars / WS: no_dollars_fp")
    print("  4. Is the NO orderbook the mirror of YES orderbook? YES")
    print("     -> YES bid at $X = NO ask at $(1.00 - X)")
    print()
    print("Taker fee (for reference): 0.07 * price * (1 - price)")
    print("  At mid=0.50: $0.0175/contract")
    print("  At mid=0.40: $0.0168/contract")
    print("  At mid=0.60: $0.0168/contract")
    print()
    print("Conclusion: bid_heavy IS implementable via NO limit orders at $0 maker fee.")
    print("  -> The bid_heavy PnL estimates require NO adjustment for fees.")
    print()


# ---------------------------------------------------------------------------
# Section 8: Verdict
# ---------------------------------------------------------------------------


def section_8(
    obs: pd.DataFrame,
    bh_dca_sweep: dict[float, list[dict]],
    threshold: float,
    ah_trades: list[dict],
    bh_trades_1k: list[dict],
) -> None:
    print("=" * 78)
    print("VERDICT: bid_heavy Signal")
    print("=" * 78)
    print()

    # Observation-level edge
    bh_mask = obs["depth_ratio"] > threshold
    bh = obs[bh_mask]
    bh_pnl = (bh["mid"] - bh["settlement"]).values
    obs_ci = clustered_bootstrap_ci(bh_pnl, bh["game_pk"].values)
    obs_edge = bh_pnl.mean()
    obs_sig = obs_ci[0] > 0

    # Game-level DCA ($1000 cap)
    if 1000.0 in bh_dca_sweep and bh_dca_sweep[1000.0]:
        dca_pnls = np.array([t["pnl_per_dollar"] for t in bh_dca_sweep[1000.0]])
        dca_ci = bootstrap_ci(dca_pnls)
        dca_edge = dca_pnls.mean()
        dca_sig = dca_ci[0] > 0
    else:
        dca_edge = 0.0
        dca_sig = False

    # Home bias independence
    bh_home = obs[bh_mask & (obs["mid"] > 0.50)]
    bh_away = obs[bh_mask & (obs["mid"] <= 0.50)]
    home_pnl = (bh_home["mid"] - bh_home["settlement"]).values
    away_pnl = (bh_away["mid"] - bh_away["settlement"]).values
    home_ci = clustered_bootstrap_ci(home_pnl, bh_home["game_pk"].values) if len(home_pnl) >= 3 else (0.0, 0.0)
    away_ci = clustered_bootstrap_ci(away_pnl, bh_away["game_pk"].values) if len(away_pnl) >= 3 else (0.0, 0.0)
    # Independent if both have positive mean (not just home_favored)
    home_bias_independent = home_pnl.mean() > 0 and away_pnl.mean() > 0

    print(f"Observation-level edge (dr > {threshold}):    {obs_edge:+.4f}, {'SIG' if obs_sig else 'NOT SIG'}")
    print(f"Game-level DCA PnL/$1 ($1000 cap):    {dca_edge:+.4f}, {'SIG' if dca_sig else 'NOT SIG'}")
    print(f"Home bias independent:                {'YES' if home_bias_independent else 'NO'}")
    print(f"Kalshi NO-side feasible:              YES")
    print()

    # Recommendation
    if obs_sig and dca_sig:
        recommendation = "ADOPT"
        marker = "[X]"
    elif obs_edge > 0 or dca_edge > 0:
        recommendation = "INVESTIGATE"
        marker = "[X]"
    else:
        recommendation = "REJECT"
        marker = "[X]"

    adopt_mark = "[X]" if recommendation == "ADOPT" else "[ ]"
    invest_mark = "[X]" if recommendation == "INVESTIGATE" else "[ ]"
    reject_mark = "[X]" if recommendation == "REJECT" else "[ ]"

    print("Recommendation:")
    print(f"  {adopt_mark} ADOPT: Add bid_heavy to live strategy")
    print(f"  {invest_mark} INVESTIGATE: Edge exists but implementation unclear")
    print(f"  {reject_mark} REJECT: No significant edge found")
    print()

    if recommendation in ("ADOPT", "INVESTIGATE"):
        # Estimated combined improvement
        ah_n = len(ah_trades)
        bh_n = len(bh_trades_1k)

        ah_total_pnl = sum(t["game_pnl"] for t in ah_trades)
        bh_total_pnl = sum(t["game_pnl"] for t in bh_trades_1k)

        # Scale to 162-game season (from 121 sample)
        scale = 162.0 / 121.0

        ah_season_games = int(ah_n * scale)
        bh_season_games = int(bh_n * scale)
        combined_games = int((len(set(t["game_pk"] for t in ah_trades) |
                                  set(t["game_pk"] for t in bh_trades_1k))) * scale)

        ah_season_pnl = ah_total_pnl * scale
        bh_season_pnl = bh_total_pnl * scale
        combined_season_pnl = (ah_total_pnl + bh_total_pnl) * scale

        improvement = (bh_season_pnl / ah_season_pnl * 100) if ah_season_pnl != 0 else 0

        print("Estimated combined strategy improvement (scaled to 162-game season):")
        print(f"  ask_heavy only:   {ah_season_games} games/season, PnL ${ah_season_pnl:+,.0f}")
        print(f"  + bid_heavy:      {bh_season_games} games/season, PnL ${bh_season_pnl:+,.0f}")
        print(f"  Total:            {combined_games} games/season, PnL ${combined_season_pnl:+,.0f} "
              f"({improvement:+.1f}% improvement)")

    print("=" * 78)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="bid_heavy reverse signal analysis on 121-game replay cache",
    )
    parser.add_argument(
        "--cache", type=str, default=str(DEFAULT_CACHE),
        help="Path to replay cache joblib file",
    )
    parser.add_argument(
        "--threshold", type=float, default=BID_HEAVY_THRESHOLD,
        help=f"bid_heavy depth_ratio threshold (default: {BID_HEAVY_THRESHOLD})",
    )
    args = parser.parse_args()

    cache_path = Path(args.cache)
    threshold = args.threshold

    games = load_games(cache_path)

    print(f"\nbid_heavy threshold: depth_ratio > {threshold}")
    print(f"ask_heavy threshold: depth_ratio < {ASK_HEAVY_THRESHOLD}")
    print()

    # Build observation-level DataFrame
    print("Building observation DataFrame ...")
    obs = build_all_observations(games)
    print(f"Total observations: {len(obs):,d} across {obs['game_pk'].nunique()} games")
    print()

    section_1(obs, threshold)
    section_2(obs)
    section_3(obs, threshold)
    bh_dca_sweep = section_4(games, threshold)
    section_5(games, obs, threshold)
    section_6(games, obs, threshold)
    section_7()

    # For verdict: get ask_heavy trades and bid_heavy trades at $1000 cap
    from scripts.strategy_backtest import run_strategy_dca_variable
    ah_trades = run_strategy_dca_variable(games, game_cap=1000.0, quiet=True)
    bh_trades_1k = bh_dca_sweep.get(1000.0, [])

    section_8(obs, bh_dca_sweep, threshold, ah_trades, bh_trades_1k)


if __name__ == "__main__":
    main()
