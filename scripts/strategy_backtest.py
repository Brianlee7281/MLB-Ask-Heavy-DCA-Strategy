"""Strategy backtest — ask_heavy signal on 98-game replay cache.

DCA entry: $1 per ask_heavy observation, hold to settlement.
PnL per $1 = settlement - avg_mid.  No fees (maker order at mid).

Usage:
    PYTHONPATH=. python scripts/strategy_backtest.py
    PYTHONPATH=. python scripts/strategy_backtest.py --cache data/models/replay_cache_v2025.1.joblib
    PYTHONPATH=. python scripts/strategy_backtest.py --min-mid 0.15
"""

from __future__ import annotations

import argparse
import csv
import time
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_CACHE = Path("data/models/replay_cache_v2025.1.joblib")
ASK_HEAVY_THRESHOLD = 0.4
N_BOOTSTRAP = 10_000
CSV_OUT_V3 = Path("results/backtest/ask_heavy_trades_dca_v3.csv")
DEFAULT_V3_CAP = 1000.0
CAP_SWEEP_LEVELS = [50, 100, 200, 500, 1000, 2000, 5000, float("inf")]
BANKROLL = 25_000
GAMES_PER_DAY = 15
SEASON_DAYS = 162
N_SEASON_SIMS = 10_000
KELLY_FRACTIONS = [
    (0.05, "1/20 K"),
    (0.10, "1/10 K"),
    (0.125, "1/8  K"),
    (0.25, "1/4  K"),
    (0.333, "1/3  K"),
    (0.50, "1/2  K"),
]
KELLY_CAP_FLOOR = 50.0
KELLY_CAP_CEILING_PCT = 0.20
CSV_OUT_KELLY_CONC = Path("results/backtest/ask_heavy_trades_kelly_concurrent.csv")
MAX_TOTAL_EXPOSURE_PCT = 0.40

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ts_to_unix(ts_raw: object) -> int:
    if isinstance(ts_raw, (int, float)):
        return int(ts_raw)
    if hasattr(ts_raw, "timestamp"):
        return int(ts_raw.timestamp())  # type: ignore[union-attr]
    try:
        return int(datetime.fromisoformat(
            str(ts_raw).replace("Z", "+00:00")).timestamp())
    except (ValueError, TypeError):
        return 0


def _fmt_p(v: float) -> str:
    return f"{v * 100:.1f}%"


def _sig(ci_lo: float) -> str:
    return "YES" if ci_lo > 0 else "NO"


def bootstrap_ci(
    values: np.ndarray, n_boot: int = N_BOOTSTRAP,
) -> tuple[float, float]:
    """95% CI via bootstrap resampling."""
    if len(values) < 3:
        return (0.0, 0.0)
    rng = np.random.default_rng(42)
    means = np.array([
        rng.choice(values, size=len(values), replace=True).mean()
        for _ in range(n_boot)
    ])
    return (float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5)))


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_games(cache_path: Path) -> list[dict]:
    """Load replay cache and return game dicts."""
    t0 = time.time()
    print(f"Loading {cache_path} ...")
    cache = joblib.load(cache_path)
    games = cache.get("game_data", [])
    print(f"Loaded {len(games)} games ({time.time() - t0:.1f}s)")
    return games


def build_game_df(game: dict) -> pd.DataFrame | None:
    """Build a per-observation DataFrame for a single game.

    Returns None if the game has no valid depth data.
    """
    ps = game["price_series"]
    ds = game.get("depth_series", [])
    if not ps:
        return None

    pa = np.array(ps)
    n = len(pa)

    df = pd.DataFrame({
        "timestamp": pa[:, 0].astype(np.int64),
        "bid": pa[:, 1],
        "ask": pa[:, 2],
        "mid": pa[:, 3],
    })
    df["spread"] = df["ask"] - df["bid"]

    if ds and len(ds) == n:
        da = np.array(ds)
        df["bid_depth"] = da[:, 1].astype(np.float64)
        df["ask_depth"] = da[:, 2].astype(np.float64)
    else:
        return None  # need depth data

    # Valid observations only
    mask = (
        (df["bid"] > 0) & (df["ask"] > 0.01)
        & (df["spread"] >= 0) & (df["spread"] <= 0.20)
        & (df["mid"] >= 0.02) & (df["mid"] <= 0.98)
        & (df["bid_depth"].notna())
        & ((df["bid_depth"] + df["ask_depth"]) > 0)
    )
    df = df[mask].reset_index(drop=True)
    if df.empty:
        return None

    total_depth = df["bid_depth"] + df["ask_depth"]
    df["depth_ratio"] = df["bid_depth"] / total_depth

    # Sort by timestamp (should already be, but enforce)
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def get_inning_at_ts(game: dict, ts: int) -> int | None:
    """Look up inning from ticks at a given timestamp."""
    ticks = game.get("ticks", [])
    if not ticks:
        return None
    best_inning = None
    for tick in ticks:
        gs = tick["game_state"]
        tick_ts = _ts_to_unix(gs["timestamp"])
        if tick_ts <= ts:
            best_inning = gs["inning"]
        else:
            break
    return best_inning


def get_game_date(game: dict) -> str:
    """Derive game date from first tick timestamp."""
    ticks = game.get("ticks", [])
    if ticks:
        ts = _ts_to_unix(ticks[0]["game_state"]["timestamp"])
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    return "unknown"


# ---------------------------------------------------------------------------
# DCA Strategy (v2 — fixed $100 budget)
# ---------------------------------------------------------------------------


def run_strategy_dca(
    games: list[dict],
    game_budget: float = 100.0,
) -> list[dict]:
    """DCA strategy: buy $1 at mid for every ask_heavy observation, up to game_budget.

    Converges to the observation-level mean entry price across the entire
    ask_heavy window rather than depending on the first observation only.
    """

    def _sort_key(g: dict) -> tuple[int, int]:
        ticks = g.get("ticks", [])
        if ticks:
            return (_ts_to_unix(ticks[0]["game_state"]["timestamp"]), g["game_pk"])
        return (0, g["game_pk"])

    games_sorted = sorted(games, key=_sort_key)

    trades: list[dict] = []
    skipped_no_depth = 0
    skipped_no_signal = 0

    per_entry = 1.0  # $1 per observation

    for game in games_sorted:
        gpk = game["game_pk"]
        settlement = 1.0 if game["home_won"] else 0.0
        home_won = int(game["home_won"])

        df = build_game_df(game)
        if df is None:
            skipped_no_depth += 1
            continue

        entries: list[dict] = []
        total_invested = 0.0

        for _, row in df.iterrows():
            if row["depth_ratio"] < ASK_HEAVY_THRESHOLD:
                if total_invested + per_entry > game_budget:
                    break
                entries.append({
                    "mid": float(row["mid"]),
                    "timestamp": int(row["timestamp"]),
                    "spread": float(row["spread"]),
                    "depth_ratio": float(row["depth_ratio"]),
                })
                total_invested += per_entry

        if not entries:
            skipped_no_signal += 1
            continue

        avg_mid = float(np.mean([e["mid"] for e in entries]))
        pnl_per_dollar = settlement - avg_mid
        game_pnl = pnl_per_dollar * total_invested

        first_entry_ts = entries[0]["timestamp"]
        last_entry_ts = entries[-1]["timestamp"]
        entry_inning = get_inning_at_ts(game, first_entry_ts)

        trades.append({
            "game_pk": gpk,
            "game_date": get_game_date(game),
            "n_entries": len(entries),
            "total_invested": total_invested,
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

    print(f"DCA strategy complete: {len(trades)} trades from {len(games)} games")
    print(f"  Skipped (no depth data): {skipped_no_depth}")
    print(f"  Skipped (no ask_heavy signal): {skipped_no_signal}")
    print()
    return trades


# ---------------------------------------------------------------------------
# Variable-Cap DCA Strategy (v3)
# ---------------------------------------------------------------------------


def run_strategy_dca_variable(
    games: list[dict],
    per_entry: float = 1.0,
    game_cap: float = DEFAULT_V3_CAP,
    min_mid: float = 0.0,
    quiet: bool = False,
) -> list[dict]:
    """DCA with variable total -- invest $per_entry per ask_heavy obs, up to game_cap.

    Same mechanics as v2 but with a configurable cap. Setting game_cap=inf
    gives unlimited investment (converges to observation-level mean).

    min_mid: symmetric mid price floor. Skips observations where
    mid < min_mid OR mid > (1.0 - min_mid).
    """

    def _sort_key(g: dict) -> tuple[int, int]:
        ticks = g.get("ticks", [])
        if ticks:
            return (_ts_to_unix(ticks[0]["game_state"]["timestamp"]), g["game_pk"])
        return (0, g["game_pk"])

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
        total_invested = 0.0

        for _, row in df.iterrows():
            if row["depth_ratio"] < ASK_HEAVY_THRESHOLD:
                if min_mid > 0.0:
                    if row["mid"] < min_mid:
                        continue
                    if row["mid"] > (1.0 - min_mid):
                        continue
                if total_invested + per_entry > game_cap:
                    break
                entries.append({
                    "mid": float(row["mid"]),
                    "timestamp": int(row["timestamp"]),
                    "spread": float(row["spread"]),
                    "depth_ratio": float(row["depth_ratio"]),
                })
                total_invested += per_entry

        if not entries:
            skipped_no_signal += 1
            continue

        avg_mid = float(np.mean([e["mid"] for e in entries]))
        pnl_per_dollar = settlement - avg_mid
        game_pnl = pnl_per_dollar * total_invested

        first_entry_ts = entries[0]["timestamp"]
        last_entry_ts = entries[-1]["timestamp"]
        entry_inning = get_inning_at_ts(game, first_entry_ts)

        trades.append({
            "game_pk": gpk,
            "game_date": get_game_date(game),
            "n_entries": len(entries),
            "total_invested": total_invested,
            "game_cap": game_cap,
            "hit_cap": total_invested >= game_cap - per_entry,
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
        print(f"DCA v3 (cap={cap_str}): {len(trades)} trades from {len(games)} games")
    return trades


# ---------------------------------------------------------------------------
# Concurrent Kelly DCA Strategy (v5)
# ---------------------------------------------------------------------------


def _compute_f_star(mu: float, sigma_sq: float) -> float:
    """Kelly optimal fraction f* = mu / sigma^2, floored at 0."""
    if sigma_sq <= 0:
        return 0.0
    return max(0.0, mu / sigma_sq)


def _precompute_game_infos(
    games: list[dict], min_mid: float = 0.0,
) -> list[dict]:
    """Pre-compute ask_heavy observations and timing for each game.

    min_mid: symmetric mid price floor — excludes observations where
    mid < min_mid OR mid > (1.0 - min_mid).
    """
    infos = []
    for game in games:
        df = build_game_df(game)
        if df is None:
            continue
        ah = df[df["depth_ratio"] < ASK_HEAVY_THRESHOLD]
        if min_mid > 0.0:
            ah = ah[(ah["mid"] >= min_mid) & (ah["mid"] <= (1.0 - min_mid))]
        if ah.empty:
            continue
        ticks = game.get("ticks", [])
        if not ticks:
            continue
        start_ts = _ts_to_unix(ticks[0]["game_state"]["timestamp"])
        end_ts = _ts_to_unix(ticks[-1]["game_state"]["timestamp"])
        infos.append({
            "game": game,
            "game_pk": game["game_pk"],
            "start_ts": start_ts,
            "end_ts": end_ts,
            "home_won": game["home_won"],
            "ah_mids": ah["mid"].values.copy(),
            "n_ah_obs": len(ah),
        })
    return infos


def run_strategy_kelly_concurrent(
    games: list[dict],
    initial_bankroll: float = BANKROLL,
    kelly_fraction: float = 0.125,
    per_entry: float = 1.0,
    max_total_exposure_pct: float = MAX_TOTAL_EXPOSURE_PCT,
    min_mid: float = 0.0,
    quiet: bool = False,
) -> list[dict]:
    """Kelly DCA with concurrent game awareness.

    Divides Kelly allocation across simultaneously active games so total
    exposure stays bounded by max_total_exposure_pct.

    min_mid: symmetric mid price floor — excludes observations where
    mid < min_mid OR mid > (1.0 - min_mid).
    """
    game_infos = _precompute_game_infos(games, min_mid=min_mid)
    if not game_infos:
        return []

    # Fixed f* from full sample
    all_pnl = [
        float((1.0 if gi["home_won"] else 0.0) - gi["ah_mids"].mean())
        for gi in game_infos
    ]
    mu = float(np.mean(all_pnl))
    sigma_sq = float(np.var(all_pnl))
    f_star = _compute_f_star(mu, sigma_sq)

    # Build event timeline — process ends before starts at same timestamp
    events: list[tuple[str, int, dict]] = []
    for gi in game_infos:
        events.append(("start", gi["start_ts"], gi))
        events.append(("end", gi["end_ts"], gi))
    events.sort(key=lambda e: (e[1], 0 if e[0] == "end" else 1))

    bankroll = initial_bankroll
    active: dict[int, dict] = {}
    reserved = 0.0
    trades: list[dict] = []

    for event_type, _, gi in events:
        gpk = gi["game_pk"]

        if event_type == "start":
            n_concurrent = len(active) + 1

            # Total Kelly allocation split across concurrent games
            total_kelly = bankroll * kelly_fraction * f_star
            per_game_kelly = total_kelly / n_concurrent

            # Safety: cap at 50% of unreserved capital
            available = bankroll - reserved
            per_game_kelly = min(per_game_kelly, available * 0.50)
            per_game_kelly = max(KELLY_CAP_FLOOR, per_game_kelly)

            # Total exposure check
            if reserved + per_game_kelly > bankroll * max_total_exposure_pct:
                per_game_kelly = max(
                    0.0,
                    bankroll * max_total_exposure_pct - reserved,
                )

            if per_game_kelly < KELLY_CAP_FLOOR:
                continue

            n_entries = min(gi["n_ah_obs"], int(per_game_kelly / per_entry))
            if n_entries == 0:
                continue

            mids_used = gi["ah_mids"][:n_entries]
            avg_mid = float(mids_used.mean())
            total_invested = float(n_entries * per_entry)

            active[gpk] = {
                "cap": per_game_kelly,
                "invested": total_invested,
                "avg_mid": avg_mid,
                "n_entries": n_entries,
                "home_won": gi["home_won"],
                "n_concurrent_at_start": n_concurrent,
                "game": gi["game"],
            }
            reserved += per_game_kelly

        elif event_type == "end":
            if gpk not in active:
                continue

            info = active.pop(gpk)
            settlement = 1.0 if info["home_won"] else 0.0
            pnl_per_dollar = settlement - info["avg_mid"]
            game_pnl = pnl_per_dollar * info["invested"]

            bankroll_before = bankroll
            bankroll += game_pnl
            reserved = max(0.0, reserved - info["cap"])

            trades.append({
                "game_pk": gpk,
                "game_date": get_game_date(info["game"]),
                "kelly_fraction": kelly_fraction,
                "f_star": f_star,
                "bankroll_before": bankroll_before,
                "bankroll_after": bankroll,
                "game_cap": info["cap"],
                "n_entries": info["n_entries"],
                "total_invested": info["invested"],
                "avg_entry_mid": info["avg_mid"],
                "home_won": int(info["home_won"]),
                "settlement": settlement,
                "pnl_per_dollar": pnl_per_dollar,
                "game_pnl": game_pnl,
                "n_concurrent_at_start": info["n_concurrent_at_start"],
            })

    if not quiet:
        print(
            f"Kelly v5 concurrent ({kelly_fraction:.3f}): {len(trades)} trades, "
            f"BR ${initial_bankroll:,.0f} -> ${bankroll:,.2f}"
        )
    return trades


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def _dca_breakdown_row(
    label: str,
    trades_sub: list[dict],
    indent: str = "",
) -> None:
    """Print one breakdown row for DCA trades (uses pnl_per_dollar)."""
    n = len(trades_sub)
    if n == 0:
        print(f"{indent}{label:<30s}     0")
        return
    pnls = np.array([t["pnl_per_dollar"] for t in trades_sub])
    wins = np.array([t["home_won"] for t in trades_sub])
    ci_lo, ci_hi = bootstrap_ci(pnls)
    print(
        f"{indent}{label:<30s}  {n:>4d}  {_fmt_p(wins.mean()):>6s}  "
        f"{pnls.mean():>+8.4f}  [{ci_lo:+.4f}, {ci_hi:+.4f}]  {_sig(ci_lo):>3s}"
    )


def _cap_label(cap: float) -> str:
    return "unlim" if cap == float("inf") else f"${cap:.0f}"


def _kelly_stats(trades: list[dict], initial_br: float) -> dict:
    """Compute summary stats for a Kelly simulation."""
    if not trades:
        return {"final_br": initial_br, "roi": 0, "sharpe": 0, "max_dd_pct": 0,
                "max_dd_dollar": 0, "worst_game": 0}
    brs = np.array([initial_br] + [t["bankroll_after"] for t in trades])
    peak = np.maximum.accumulate(brs)
    dd_pct = (brs - peak) / np.where(peak > 0, peak, 1.0)
    game_pnls = np.array([t["game_pnl"] for t in trades])
    returns = np.array([t["game_pnl"] / t["bankroll_before"] for t in trades])
    sharpe = float(returns.mean() / returns.std()) if returns.std() > 0 else 0.0
    return {
        "final_br": brs[-1],
        "roi": (brs[-1] - initial_br) / initial_br * 100,
        "sharpe": sharpe,
        "max_dd_pct": float(dd_pct.min()) * 100,
        "max_dd_dollar": float((brs - peak).min()),
        "worst_game": float(game_pnls.min()),
    }


# ---------------------------------------------------------------------------
# Section 1: DCA Cap Sweep  (was Section 12)
# ---------------------------------------------------------------------------


def section_1(games: list[dict]) -> None:
    """Run cap sweep and print results."""
    print("=" * 78)
    print("SECTION 1: DCA Cap Sweep")
    print("=" * 78)
    print()

    sweep: dict[float, list[dict]] = {}
    for cap in CAP_SWEEP_LEVELS:
        sweep[cap] = run_strategy_dca_variable(games, game_cap=cap, quiet=True)

    header = (
        f"{'Cap':>7s}  {'N':>3s}  {'Mean inv':>9s}  {'PnL/$1':>8s}  "
        f"{'Sharpe':>6s}  {'Max DD$':>8s}  {'95% CI':>19s}  {'Sig':>3s}  {'%cap':>5s}"
    )
    print(header)
    print("-" * len(header))

    for cap in CAP_SWEEP_LEVELS:
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
        pct_capped = np.mean([t["hit_cap"] for t in trades]) * 100

        label = _cap_label(cap)
        print(
            f"{label:>7s}  {n:>3d}  ${invested.mean():>7.2f}  {pnls.mean():>+8.4f}  "
            f"{sharpe:>6.3f}  {max_dd:>+8.2f}  "
            f"[{ci_lo:+.4f}, {ci_hi:+.4f}]  {sig:>3s}  {pct_capped:>4.1f}%"
        )

    print()
    print("Observation-level reference: +0.1079")
    print()


# ---------------------------------------------------------------------------
# Section 2: DCA Breakdown at Chosen Cap  (was Section 13)
# ---------------------------------------------------------------------------


def section_2(v3_trades: list[dict], cap: float) -> None:
    print("=" * 78)
    print(f"SECTION 2: DCA ({_cap_label(cap)} cap) Breakdown")
    print("=" * 78)
    print()

    if not v3_trades:
        print("No trades.")
        return

    header = f"{'':30s}  {'N':>4s}  {'Win%':>6s}  {'Mean PnL':>8s}  {'95% CI':>17s}  {'Sig':>3s}"
    print(header)
    print("-" * len(header))

    _dca_breakdown_row(f"All DCA ({_cap_label(cap)} cap)", v3_trades)

    home_fav = [t for t in v3_trades if t["home_favored_at_avg"]]
    away_fav = [t for t in v3_trades if not t["home_favored_at_avg"]]
    _dca_breakdown_row("Home favored (avg_mid>0.50)", home_fav, "  ")
    _dca_breakdown_row("Away favored (avg_mid<0.50)", away_fav, "  ")
    print()

    # By total invested
    for label, lo, hi in [
        ("<$100", 0, 100),
        ("$100-$500", 100, 500),
        ("$500-$1000", 500, 1000),
        (f"Hit cap ({_cap_label(cap)})", -1, -1),  # special
    ]:
        if lo == -1:
            sub = [t for t in v3_trades if t["hit_cap"]]
        else:
            sub = [t for t in v3_trades if lo <= t["total_invested"] < hi and not t["hit_cap"]]
        _dca_breakdown_row(label, sub)
    print()


# ---------------------------------------------------------------------------
# Section 3: Risk Analysis  (was Section 14)
# ---------------------------------------------------------------------------


def section_3(v3_trades: list[dict], cap: float) -> None:
    print("=" * 78)
    print(f"SECTION 3: Risk Analysis ({_cap_label(cap)} cap)")
    print("=" * 78)
    print()

    if not v3_trades:
        print("No trades.")
        return

    game_pnls = np.array([t["game_pnl"] for t in v3_trades])
    invested = np.array([t["total_invested"] for t in v3_trades])

    best_idx = int(np.argmax(game_pnls))
    worst_idx = int(np.argmin(game_pnls))
    best = v3_trades[best_idx]
    worst = v3_trades[worst_idx]

    print("Per-game PnL distribution (in dollars):")
    print(f"  Mean:          ${game_pnls.mean():+.2f}")
    print(f"  Median:        ${np.median(game_pnls):+.2f}")
    print(f"  Std:           ${game_pnls.std():.2f}")
    print(
        f"  Worst game:    ${game_pnls[worst_idx]:+.2f} "
        f"(game_pk={worst['game_pk']}, invested ${worst['total_invested']:.0f}, "
        f"avg_mid={worst['avg_entry_mid']:.2f})"
    )
    print(
        f"  Best game:     ${game_pnls[best_idx]:+.2f} "
        f"(game_pk={best['game_pk']}, invested ${best['total_invested']:.0f}, "
        f"avg_mid={best['avg_entry_mid']:.2f})"
    )
    print()

    # Consecutive losses
    wins = np.array([t["home_won"] for t in v3_trades])
    max_consec_loss = 0
    cur = 0
    for w in wins:
        if not w:
            cur += 1
            max_consec_loss = max(max_consec_loss, cur)
        else:
            cur = 0

    cum = np.cumsum(game_pnls)
    peak = np.maximum.accumulate(cum)
    max_dd = float((cum - peak).min())

    print("Max drawdown scenario:")
    print(f"  Max consecutive losses:     {max_consec_loss} games")
    print(f"  Worst-case drawdown:        ${max_dd:+.2f}")
    print(f"  As % of ${BANKROLL:,} bankroll:      {abs(max_dd) / BANKROLL * 100:.1f}%")
    print()

    # Portfolio simulation
    rng = np.random.default_rng(42)
    season_pnls = np.zeros(N_SEASON_SIMS)
    for i in range(N_SEASON_SIMS):
        daily_games = rng.choice(game_pnls, size=(SEASON_DAYS, GAMES_PER_DAY), replace=True)
        season_pnls[i] = daily_games.sum()

    p_neg = (season_pnls < 0).mean()

    print(f"Portfolio simulation ({GAMES_PER_DAY} games/day, {SEASON_DAYS} days):")
    print(f"  Expected annual PnL:        ${season_pnls.mean():+,.0f}")
    if season_pnls.std() > 0:
        print(f"  Expected annual Sharpe:     {season_pnls.mean() / season_pnls.std():.2f}")
    lo_95 = np.percentile(season_pnls, 2.5)
    hi_95 = np.percentile(season_pnls, 97.5)
    print(f"  95% annual PnL range:       [${lo_95:+,.0f}, ${hi_95:+,.0f}]")
    print(f"  P(negative year):           {_fmt_p(p_neg)}")
    print()


# ---------------------------------------------------------------------------
# Section 4: Game Concurrency Profile  (was Section 20)
# ---------------------------------------------------------------------------


def section_4(games: list[dict]) -> None:
    print("=" * 78)
    print("SECTION 4: Game Concurrency Profile")
    print("=" * 78)
    print()

    infos = _precompute_game_infos(games)
    if not infos:
        print("No games with ask_heavy data.")
        return

    # For each game start, count how many other games overlap
    concurrencies: list[int] = []
    for gi in infos:
        n = sum(
            1 for other in infos
            if other["game_pk"] != gi["game_pk"]
            and other["start_ts"] <= gi["end_ts"]
            and other["end_ts"] >= gi["start_ts"]
        ) + 1  # include self
        concurrencies.append(n)

    conc = np.array(concurrencies)

    print("Concurrency distribution (at game start):")
    print(f"  1 game active:      {np.sum(conc == 1)} games")
    print(f"  2-3 games:          {np.sum((conc >= 2) & (conc <= 3))} games")
    print(f"  4-6 games:          {np.sum((conc >= 4) & (conc <= 6))} games")
    print(f"  7+ games:           {np.sum(conc >= 7)} games")
    print(f"  Mean concurrent:    {conc.mean():.1f} games")
    print(f"  Max concurrent:     {conc.max()} games")
    print()

    # Impact on Kelly cap
    all_pnl = [
        float((1.0 if gi["home_won"] else 0.0) - gi["ah_mids"].mean())
        for gi in infos
    ]
    f_star = _compute_f_star(float(np.mean(all_pnl)), float(np.var(all_pnl)))
    solo_cap = BANKROLL * 0.125 * f_star

    print("Impact on Kelly cap (1/8 Kelly, $25K bankroll):")
    print(f"  Solo game cap (1 concurrent):           ${solo_cap:,.0f}")
    for n in [5, 10]:
        print(f"  With {n} concurrent games:                ${solo_cap / n:,.0f}")
    print()


# ---------------------------------------------------------------------------
# Section 5: Concurrent Kelly Summary  (was Section 21)
# ---------------------------------------------------------------------------


def section_5(v5_trades: list[dict], frac_label: str) -> None:
    print("=" * 78)
    print(f"SECTION 5: Concurrent Kelly Summary ({frac_label})")
    print("=" * 78)
    print()

    if not v5_trades:
        print("No trades.")
        return

    s = _kelly_stats(v5_trades, BANKROLL)
    caps = np.array([t["game_cap"] for t in v5_trades])

    # Max total exposure estimate
    max_exp = 0.0
    for t in v5_trades:
        exp = t["game_cap"] * t.get("n_concurrent_at_start", 1)
        frac_br = exp / t["bankroll_before"] * 100 if t["bankroll_before"] > 0 else 0
        max_exp = max(max_exp, frac_br)

    print(f"  Initial bankroll:           ${BANKROLL:,}")
    print(f"  Final bankroll:             ${s['final_br']:,.2f}")
    print(f"  ROI:                        {s['roi']:+.1f}%")
    print(f"  Sharpe:                     {s['sharpe']:.3f}")
    print(f"  Max DD%:                    {s['max_dd_pct']:.1f}%")
    print(f"  Max DD$:                    ${s['max_dd_dollar']:,.2f}")
    print(f"  Mean cap/game:              ${caps.mean():,.0f}")
    print(f"  Max total exposure:         {max_exp:.1f}%")
    print(f"  Worst game loss:            ${s['worst_game']:,.2f}")
    print()


# ---------------------------------------------------------------------------
# Section 6: Concurrent Kelly Fraction Sweep  (was Section 22)
# ---------------------------------------------------------------------------


def section_6(games: list[dict]) -> dict[float, list[dict]]:
    """Concurrent Kelly fraction sweep. Returns {fraction: trades}."""
    print("=" * 78)
    print("SECTION 6: Concurrent Kelly Fraction Sweep")
    print("=" * 78)
    print()

    results: dict[float, list[dict]] = {}

    header = (
        f"{'Fraction':>10s}  {'Final BR':>11s}  {'ROI':>7s}  "
        f"{'Sharpe':>6s}  {'Max DD%':>7s}  {'Max DD$':>9s}  {'Mean cap':>8s}"
    )
    print(header)
    print("-" * len(header))

    for frac, label in KELLY_FRACTIONS:
        trades = run_strategy_kelly_concurrent(
            games, kelly_fraction=frac, quiet=True,
        )
        results[frac] = trades
        s = _kelly_stats(trades, BANKROLL)
        caps = np.array([t["game_cap"] for t in trades]) if trades else np.array([0.0])
        print(
            f"{label:>10s}  ${s['final_br']:>10,.2f}  {s['roi']:>+6.1f}%  "
            f"{s['sharpe']:>6.3f}  {s['max_dd_pct']:>+6.1f}%  "
            f"${s['max_dd_dollar']:>+8,.2f}  ${caps.mean():>7,.0f}"
        )

    print()
    return results


# ---------------------------------------------------------------------------
# Trade Log Exports
# ---------------------------------------------------------------------------


def export_dca_trades(v3_trades: list[dict]) -> None:
    CSV_OUT_V3.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "game_pk", "game_date", "n_entries", "total_invested", "game_cap",
        "avg_entry_mid", "first_entry_mid", "min_entry_mid", "max_entry_mid",
        "avg_spread", "avg_depth_ratio", "entry_duration_seconds",
        "home_favored_at_avg", "home_won", "pnl_per_dollar", "game_pnl",
    ]

    with open(CSV_OUT_V3, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for t in v3_trades:
            row = {k: t.get(k) for k in fieldnames}
            row["home_favored_at_avg"] = int(t["home_favored_at_avg"])
            row["game_cap"] = t["game_cap"] if t["game_cap"] < float("inf") else "unlimited"
            writer.writerow(row)

    print(f"Saved {len(v3_trades)} DCA trades to {CSV_OUT_V3}")
    print()


def export_kelly_trades(v5_trades: list[dict]) -> None:
    CSV_OUT_KELLY_CONC.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "game_pk", "game_date", "kelly_fraction", "f_star",
        "bankroll_before", "game_cap", "n_entries", "total_invested",
        "avg_entry_mid", "home_won", "pnl_per_dollar", "game_pnl",
        "bankroll_after", "n_concurrent_at_start",
    ]

    with open(CSV_OUT_KELLY_CONC, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for t in v5_trades:
            writer.writerow({k: t.get(k) for k in fieldnames})

    print(f"Saved {len(v5_trades)} concurrent Kelly trades to {CSV_OUT_KELLY_CONC}")
    print()


# ---------------------------------------------------------------------------
# Validation Verdict
# ---------------------------------------------------------------------------


def print_verdict(
    v3_trades: list[dict],
    v5_trades: list[dict],
    kelly_conc_sweep: dict[float, list[dict]],
) -> None:
    print("=" * 78)
    print("VALIDATION RESULT")
    print("=" * 78)

    if not v3_trades:
        print("No trades -- cannot validate.")
        print("=" * 78)
        return

    v3_pnls = np.array([t["pnl_per_dollar"] for t in v3_trades])
    v3_ci = bootstrap_ci(v3_pnls)
    v3_sig = v3_ci[0] > 0
    print(f"DCA $1000 cap:       PnL/$1 {v3_pnls.mean():+.4f}, {'SIGNIFICANT' if v3_sig else 'NOT significant'}")

    if v5_trades:
        v5_s = _kelly_stats(v5_trades, BANKROLL)
        print(f"Conc. Kelly:         Final BR ${v5_s['final_br']:,.2f}, ROI {v5_s['roi']:+.1f}%, Max DD {v5_s['max_dd_pct']:.1f}%")
    print()

    # Find best concurrent Kelly fraction
    best_frac = 0.0
    best_sharpe = -999.0
    best_label = ""
    for frac, trades_at_frac in kelly_conc_sweep.items():
        if not trades_at_frac:
            continue
        s = _kelly_stats(trades_at_frac, BANKROLL)
        if s["sharpe"] > best_sharpe:
            best_sharpe = s["sharpe"]
            best_frac = frac
            labels = [l for f, l in KELLY_FRACTIONS if f == frac]
            best_label = labels[0].strip() if labels else f"{frac:.3f}"

    if best_frac > 0 and best_frac in kelly_conc_sweep:
        best_trades = kelly_conc_sweep[best_frac]
        bs = _kelly_stats(best_trades, BANKROLL)

        print("Recommended config:")
        print(f"  Entry: DCA ($1/obs while ask_heavy)")
        print(f"  Sizing: {best_label} concurrent Kelly")
        print(f"  Max total exposure: {MAX_TOTAL_EXPOSURE_PCT:.0%} of bankroll")
        print(f"  Starting bankroll: ${BANKROLL:,}")
        print(f"  Expected ROI: {bs['roi']:+.1f}%, Max DD: {bs['max_dd_pct']:.1f}%")
        print()

    validated = v3_pnls.mean() > 0
    print(f"Strategy code is:                 {'VALIDATED' if validated else 'NEEDS INVESTIGATION'}")
    print("=" * 78)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Strategy backtest -- ask_heavy on 98-game replay cache",
    )
    parser.add_argument(
        "--cache", type=str, default=str(DEFAULT_CACHE),
        help="Path to replay cache joblib file",
    )
    parser.add_argument(
        "--v3-cap", type=float, default=DEFAULT_V3_CAP,
        help="DCA: max $ invested per game (default: 1000)",
    )
    parser.add_argument(
        "--kelly-fraction", type=float, default=0.125,
        help="Kelly fraction (default: 0.125 = 1/8 Kelly)",
    )
    parser.add_argument(
        "--min-mid", type=float, default=0.0,
        help="Mid price floor filter (default: 0.0 = no filter). "
             "Symmetric: skips mid < X and mid > 1-X.",
    )
    args = parser.parse_args()

    cache_path = Path(args.cache)
    games = load_games(cache_path)

    # DCA Variable-cap
    v3_trades = run_strategy_dca_variable(
        games, game_cap=args.v3_cap, min_mid=args.min_mid,
    )

    section_1(games)
    section_2(v3_trades, args.v3_cap)
    section_3(v3_trades, args.v3_cap)
    export_dca_trades(v3_trades)

    # Concurrent Kelly DCA
    frac_labels = [l for f, l in KELLY_FRACTIONS if f == args.kelly_fraction]
    frac_label = frac_labels[0].strip() if frac_labels else f"{args.kelly_fraction:.3f}"

    v5_trades = run_strategy_kelly_concurrent(
        games, kelly_fraction=args.kelly_fraction, min_mid=args.min_mid,
    )

    section_4(games)
    section_5(v5_trades, frac_label)
    conc_sweep = section_6(games)
    export_kelly_trades(v5_trades)

    print_verdict(v3_trades, v5_trades, conc_sweep)


if __name__ == "__main__":
    main()
