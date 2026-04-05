"""Strategy backtest — ask_heavy signal on 98-game replay cache.

Implements the EXACT entry logic the live bot will use:
  - depth_ratio < 0.4 → ask_heavy
  - ONE entry per game (first ask_heavy observation)
  - Hold to settlement, PnL = settlement - entry_mid
  - No fees (maker order at mid)

Usage:
    PYTHONPATH=. python scripts/strategy_backtest.py
    PYTHONPATH=. python scripts/strategy_backtest.py --cache data/models/replay_cache_v2025.1.joblib
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
SIM_FILL_TOLERANCE = 0.005
FILL_WINDOW = 60  # seconds
N_BOOTSTRAP = 10_000
CSV_OUT = Path("results/backtest/ask_heavy_trades.csv")
CSV_OUT_DCA = Path("results/backtest/ask_heavy_trades_dca.csv")
CSV_OUT_V3 = Path("results/backtest/ask_heavy_trades_dca_v3.csv")
DEFAULT_GAME_BUDGET = 100.0
DEFAULT_V3_CAP = 1000.0
CAP_SWEEP_LEVELS = [50, 100, 200, 500, 1000, 2000, 5000, float("inf")]
BANKROLL = 25_000
GAMES_PER_DAY = 15
SEASON_DAYS = 162
N_SEASON_SIMS = 10_000
CSV_OUT_KELLY = Path("results/backtest/ask_heavy_trades_kelly.csv")
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
KELLY_ROLLING_MIN_GAMES = 10
KELLY_PRIOR_MU = 0.05
KELLY_PRIOR_SIGMA_SQ = 0.25
CSV_OUT_KELLY_CONC = Path("results/backtest/ask_heavy_trades_kelly_concurrent.csv")
MAX_TOTAL_EXPOSURE_PCT = 0.40

# ---------------------------------------------------------------------------
# Helpers (from deep_orderbook_micro.py)
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
# Data loading (from deep_orderbook_micro.py / fill_rate_estimation.py)
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
    # Find tick with largest timestamp <= ts
    best_inning = None
    for tick in ticks:
        gs = tick["game_state"]
        tick_ts = _ts_to_unix(gs["timestamp"])
        if tick_ts <= ts:
            best_inning = gs["inning"]
        else:
            break
    return best_inning


def get_score_diff_at_ts(game: dict, ts: int) -> int | None:
    """Look up home_score - away_score from ticks at a given timestamp."""
    ticks = game.get("ticks", [])
    if not ticks:
        return None
    best = None
    for tick in ticks:
        gs = tick["game_state"]
        tick_ts = _ts_to_unix(gs["timestamp"])
        if tick_ts <= ts:
            best = gs["score_home"] - gs["score_away"]
        else:
            break
    return best


def get_game_date(game: dict) -> str:
    """Derive game date from first tick timestamp."""
    ticks = game.get("ticks", [])
    if ticks:
        ts = _ts_to_unix(ticks[0]["game_state"]["timestamp"])
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    return "unknown"


# ---------------------------------------------------------------------------
# Fill simulation (from fill_rate_estimation.py)
# ---------------------------------------------------------------------------


def simulate_fill_for_entry(
    game: dict,
    entry_ts: int,
    entry_mid: float,
    window: int = FILL_WINDOW,
) -> tuple[bool, float | None, float | None]:
    """Simulate whether a YES limit at entry_mid would fill within window.

    Uses both trade data and price series snapshots.

    Returns:
        (filled, fill_price, time_to_fill_seconds)
    """
    tolerance = SIM_FILL_TOLERANCE

    # Method 1: Trade-based
    trades = game.get("trade_rows_parsed", [])
    for tr in trades:
        tr_ts = int(tr[0])
        if tr_ts < entry_ts:
            continue
        if tr_ts > entry_ts + window:
            break
        # tr = (ts, price, side, count)
        # YES price = price if side=='yes', else 1-price
        yes_price = tr[1] if tr[2] == "yes" else 1.0 - tr[1]
        if yes_price <= entry_mid + tolerance:
            return (True, yes_price, float(tr_ts - entry_ts))

    # Method 2: Snapshot-based (ask drops to our level)
    ps = game.get("price_series", [])
    for p in ps:
        p_ts = int(p[0])
        if p_ts < entry_ts:
            continue
        if p_ts > entry_ts + window:
            break
        p_ask = p[2]  # YES ask
        if p_ask <= entry_mid + tolerance:
            return (True, p_ask, float(p_ts - entry_ts))

    return (False, None, None)


# ---------------------------------------------------------------------------
# Strategy execution
# ---------------------------------------------------------------------------


def run_strategy(games: list[dict]) -> list[dict]:
    """Execute the ask_heavy strategy on all games.

    One entry per game: first observation where depth_ratio < 0.4.
    Hold to settlement. PnL = settlement - entry_mid.
    """
    # Sort games chronologically by first tick timestamp
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

        # Find first ask_heavy observation
        ah_mask = df["depth_ratio"] < ASK_HEAVY_THRESHOLD
        if not ah_mask.any():
            skipped_no_signal += 1
            continue

        first_idx = ah_mask.idxmax()
        row = df.loc[first_idx]

        entry_ts = int(row["timestamp"])
        entry_mid = float(row["mid"])
        entry_spread = float(row["spread"])
        entry_depth_ratio = float(row["depth_ratio"])
        entry_bid_depth = int(row["bid_depth"])
        entry_ask_depth = int(row["ask_depth"])
        home_favored = entry_mid > 0.50

        entry_inning = get_inning_at_ts(game, entry_ts)
        entry_score_diff = get_score_diff_at_ts(game, entry_ts)

        # Game end timestamp
        game_end_ts = int(df["timestamp"].iloc[-1])

        # Simulate fill
        filled, fill_price, fill_time = simulate_fill_for_entry(
            game, entry_ts, entry_mid,
        )

        # Count additional entry opportunities (mid drops 2c+ below first entry)
        additional_entries = 0
        additional_mids: list[float] = []
        subsequent = df.loc[first_idx + 1:]
        for _, srow in subsequent.iterrows():
            if srow["depth_ratio"] < ASK_HEAVY_THRESHOLD and srow["mid"] <= entry_mid - 0.02:
                additional_entries += 1
                additional_mids.append(float(srow["mid"]))
                break  # count distinct opportunities, not every tick

        # Check for more distinct drops (non-overlapping, further 2c drops)
        if additional_mids:
            last_add_mid = additional_mids[-1]
            for _, srow in subsequent.iterrows():
                if (srow["depth_ratio"] < ASK_HEAVY_THRESHOLD
                        and srow["mid"] <= last_add_mid - 0.02
                        and srow["mid"] <= entry_mid - 0.04):
                    additional_entries += 1
                    additional_mids.append(float(srow["mid"]))
                    last_add_mid = float(srow["mid"])

        pnl = settlement - entry_mid

        trades.append({
            "game_pk": gpk,
            "game_date": get_game_date(game),
            "entry_ts": entry_ts,
            "entry_mid": entry_mid,
            "entry_spread": entry_spread,
            "entry_depth_ratio": entry_depth_ratio,
            "entry_bid_depth": entry_bid_depth,
            "entry_ask_depth": entry_ask_depth,
            "entry_inning": entry_inning,
            "entry_score_diff": entry_score_diff,
            "home_favored": home_favored,
            "home_won": home_won,
            "settlement": settlement,
            "pnl": pnl,
            "hold_duration": game_end_ts - entry_ts,
            "sim_filled_60s": filled,
            "sim_fill_price": fill_price,
            "sim_fill_time": fill_time,
            "additional_entries": additional_entries,
            "additional_mids": additional_mids,
        })

    print(f"Strategy complete: {len(trades)} trades from {len(games)} games")
    print(f"  Skipped (no depth data): {skipped_no_depth}")
    print(f"  Skipped (no ask_heavy signal): {skipped_no_signal}")
    print()
    return trades


# ---------------------------------------------------------------------------
# v2 DCA Strategy
# ---------------------------------------------------------------------------


def run_strategy_dca(
    games: list[dict],
    game_budget: float = DEFAULT_GAME_BUDGET,
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
# v3 Variable-Cap DCA Strategy
# ---------------------------------------------------------------------------


def run_strategy_dca_variable(
    games: list[dict],
    per_entry: float = 1.0,
    game_cap: float = DEFAULT_V3_CAP,
    quiet: bool = False,
) -> list[dict]:
    """DCA with variable total -- invest $per_entry per ask_heavy obs, up to game_cap.

    Same mechanics as v2 but with a configurable cap. Setting game_cap=inf
    gives unlimited investment (converges to observation-level mean).
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
# v4 Kelly Criterion DCA Strategy
# ---------------------------------------------------------------------------


def _sort_games(games: list[dict]) -> list[dict]:
    """Sort games chronologically by first tick timestamp."""

    def _key(g: dict) -> tuple[int, int]:
        ticks = g.get("ticks", [])
        if ticks:
            return (_ts_to_unix(ticks[0]["game_state"]["timestamp"]), g["game_pk"])
        return (0, g["game_pk"])

    return sorted(games, key=_key)


def _compute_f_star(mu: float, sigma_sq: float) -> float:
    """Kelly optimal fraction f* = mu / sigma^2, floored at 0."""
    if sigma_sq <= 0:
        return 0.0
    return max(0.0, mu / sigma_sq)


def run_strategy_kelly(
    games: list[dict],
    initial_bankroll: float = BANKROLL,
    kelly_fraction: float = 0.125,
    per_entry: float = 1.0,
    use_rolling: bool = False,
    quiet: bool = False,
) -> list[dict]:
    """DCA with Kelly-sized game cap.

    Cap per game = bankroll * kelly_fraction * f*.
    f* is either fixed (from full sample) or rolling (updated each game).
    """
    games_sorted = _sort_games(games)

    # Pre-compute fixed f* from all games (look-ahead for fixed mode)
    all_pnl_per_dollar: list[float] = []
    for game in games_sorted:
        df = build_game_df(game)
        if df is None:
            continue
        ah = df[df["depth_ratio"] < ASK_HEAVY_THRESHOLD]
        if ah.empty:
            continue
        avg_mid = float(ah["mid"].mean())
        settlement = 1.0 if game["home_won"] else 0.0
        all_pnl_per_dollar.append(settlement - avg_mid)

    if not all_pnl_per_dollar:
        return []

    fixed_mu = float(np.mean(all_pnl_per_dollar))
    fixed_sigma_sq = float(np.var(all_pnl_per_dollar))
    fixed_f_star = _compute_f_star(fixed_mu, fixed_sigma_sq)

    # Rolling state
    rolling_pnls: list[float] = []

    bankroll = initial_bankroll
    trades: list[dict] = []

    for game in games_sorted:
        if bankroll < KELLY_CAP_FLOOR:
            break  # busted

        gpk = game["game_pk"]
        settlement = 1.0 if game["home_won"] else 0.0
        home_won = int(game["home_won"])

        df = build_game_df(game)
        if df is None:
            continue

        # Determine f*
        if use_rolling and len(rolling_pnls) >= KELLY_ROLLING_MIN_GAMES:
            r_mu = float(np.mean(rolling_pnls))
            r_sigma_sq = float(np.var(rolling_pnls))
            f_star = _compute_f_star(r_mu, r_sigma_sq)
        elif use_rolling:
            f_star = _compute_f_star(KELLY_PRIOR_MU, KELLY_PRIOR_SIGMA_SQ)
        else:
            f_star = fixed_f_star

        # Game cap with floor/ceiling
        raw_cap = bankroll * kelly_fraction * f_star
        game_cap = max(KELLY_CAP_FLOOR, min(bankroll * KELLY_CAP_CEILING_PCT, raw_cap))

        # DCA within cap
        entries: list[dict] = []
        total_invested = 0.0

        for _, row in df.iterrows():
            if row["depth_ratio"] < ASK_HEAVY_THRESHOLD:
                if total_invested + per_entry > game_cap:
                    break
                entries.append({
                    "mid": float(row["mid"]),
                    "timestamp": int(row["timestamp"]),
                })
                total_invested += per_entry

        if not entries:
            continue

        avg_mid = float(np.mean([e["mid"] for e in entries]))
        pnl_per_dollar = settlement - avg_mid
        game_pnl = pnl_per_dollar * total_invested

        bankroll_before = bankroll
        bankroll += game_pnl
        rolling_pnls.append(pnl_per_dollar)

        trades.append({
            "game_pk": gpk,
            "game_date": get_game_date(game),
            "kelly_fraction": kelly_fraction,
            "f_star": f_star,
            "bankroll_before": bankroll_before,
            "bankroll_after": bankroll,
            "game_cap": game_cap,
            "n_entries": len(entries),
            "total_invested": total_invested,
            "avg_entry_mid": avg_mid,
            "first_entry_mid": entries[0]["mid"],
            "home_won": home_won,
            "settlement": settlement,
            "pnl_per_dollar": pnl_per_dollar,
            "game_pnl": game_pnl,
        })

    if not quiet:
        mode = "rolling" if use_rolling else "fixed"
        frac_label = f"{kelly_fraction:.3f}"
        final_br = bankroll
        print(
            f"Kelly v4 ({frac_label}, {mode}): {len(trades)} trades, "
            f"BR ${initial_bankroll:,.0f} -> ${final_br:,.2f}"
        )
    return trades


# ---------------------------------------------------------------------------
# v5 Concurrent Kelly DCA Strategy
# ---------------------------------------------------------------------------


def _precompute_game_infos(games: list[dict]) -> list[dict]:
    """Pre-compute ask_heavy observations and timing for each game."""
    infos = []
    for game in games:
        df = build_game_df(game)
        if df is None:
            continue
        ah = df[df["depth_ratio"] < ASK_HEAVY_THRESHOLD]
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
    quiet: bool = False,
) -> list[dict]:
    """Kelly DCA with concurrent game awareness.

    Divides Kelly allocation across simultaneously active games so total
    exposure stays bounded by max_total_exposure_pct.
    """
    game_infos = _precompute_game_infos(games)
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
# Section 1: Trade Summary
# ---------------------------------------------------------------------------


def section_1(trades: list[dict], total_games: int) -> None:
    print("=" * 78)
    print("SECTION 1: Trade Summary")
    print("=" * 78)
    print()

    n_trades = len(trades)
    n_no_signal = total_games - n_trades

    pnls = np.array([t["pnl"] for t in trades])
    wins = np.array([t["home_won"] for t in trades])

    if n_trades == 0:
        print("No trades taken.")
        print()
        return

    cum_pnl = np.cumsum(pnls)
    peak = np.maximum.accumulate(cum_pnl)
    drawdowns = cum_pnl - peak
    max_dd = float(drawdowns.min())

    # Streaks
    longest_win = longest_loss = current_win = current_loss = 0
    for w in wins:
        if w:
            current_win += 1
            current_loss = 0
            longest_win = max(longest_win, current_win)
        else:
            current_loss += 1
            current_win = 0
            longest_loss = max(longest_loss, current_loss)

    best_idx = int(np.argmax(pnls))
    worst_idx = int(np.argmin(pnls))

    sharpe = float(pnls.mean() / pnls.std()) if pnls.std() > 0 else 0.0

    print(f"Total games in dataset:        {total_games}")
    print(f"Games with ask_heavy signal:   {n_trades} ({n_trades / total_games * 100:.1f}%)")
    print(f"Games without signal:          {n_no_signal}")
    print()
    print(f"Trades taken:                  {n_trades}")
    print(f"Win rate:                      {_fmt_p(wins.mean())}")
    print(f"Mean PnL per trade:            {pnls.mean():+.4f}")
    print(f"Median PnL per trade:          {np.median(pnls):+.4f}")
    print(f"Std PnL:                       {pnls.std():.4f}")
    print(f"Sharpe (trade-level):          {sharpe:.3f}")
    print(f"Max drawdown:                  ${max_dd:.2f} (per-unit basis)")
    print(f"Best trade:                    {pnls[best_idx]:+.4f} (game_pk={trades[best_idx]['game_pk']})")
    print(f"Worst trade:                   {pnls[worst_idx]:+.4f} (game_pk={trades[worst_idx]['game_pk']})")
    print(f"Longest win streak:            {longest_win} games")
    print(f"Longest loss streak:           {longest_loss} games")
    print()


# ---------------------------------------------------------------------------
# Section 2: Breakdown by Entry Conditions
# ---------------------------------------------------------------------------


def _print_breakdown_row(
    label: str,
    trades_sub: list[dict],
    indent: str = "",
) -> None:
    """Print one breakdown row with N, win%, mean PnL, CI, sig."""
    n = len(trades_sub)
    if n == 0:
        print(f"{indent}{label:<30s}     0")
        return
    pnls = np.array([t["pnl"] for t in trades_sub])
    wins = np.array([t["home_won"] for t in trades_sub])
    ci_lo, ci_hi = bootstrap_ci(pnls)
    print(
        f"{indent}{label:<30s}  {n:>4d}  {_fmt_p(wins.mean()):>6s}  "
        f"{pnls.mean():>+8.4f}  [{ci_lo:+.4f}, {ci_hi:+.4f}]  {_sig(ci_lo):>3s}"
    )


def section_2(trades: list[dict]) -> None:
    print("=" * 78)
    print("SECTION 2: Breakdown by Entry Conditions")
    print("=" * 78)
    print()

    header = f"{'':30s}  {'N':>4s}  {'Win%':>6s}  {'Mean PnL':>8s}  {'95% CI':>17s}  {'Sig':>3s}"
    print(header)
    print("-" * len(header))

    _print_breakdown_row("All trades", trades)

    home_fav = [t for t in trades if t["home_favored"]]
    away_fav = [t for t in trades if not t["home_favored"]]
    _print_breakdown_row("Home favored (mid>0.50)", home_fav, "  ")
    _print_breakdown_row("Away favored (mid<0.50)", away_fav, "  ")
    print()

    # By inning
    for lo, hi, label in [(1, 3, "Entry inning 1-3"), (4, 6, "Entry inning 4-6"), (7, 9, "Entry inning 7-9")]:
        sub = [t for t in trades if t["entry_inning"] is not None and lo <= t["entry_inning"] <= hi]
        _print_breakdown_row(label, sub)
    inning_extra = [t for t in trades if t["entry_inning"] is not None and t["entry_inning"] > 9]
    if inning_extra:
        _print_breakdown_row("Entry inning 10+", inning_extra)
    print()

    # By spread
    for label, lo, hi in [
        ("Entry spread 1c", 0.005, 0.015),
        ("Entry spread 2c", 0.015, 0.025),
        ("Entry spread 3+c", 0.025, 1.0),
    ]:
        sub = [t for t in trades if lo <= t["entry_spread"] < hi]
        _print_breakdown_row(label, sub)
    print()

    # By depth ratio
    for label, lo, hi in [
        ("Entry depth_ratio <0.2", 0.0, 0.2),
        ("Entry depth_ratio 0.2-0.3", 0.2, 0.3),
        ("Entry depth_ratio 0.3-0.4", 0.3, 0.4),
    ]:
        sub = [t for t in trades if lo <= t["entry_depth_ratio"] < hi]
        _print_breakdown_row(label, sub)
    print()


# ---------------------------------------------------------------------------
# Section 3: Fill Rate Integration
# ---------------------------------------------------------------------------


def section_3(trades: list[dict]) -> None:
    print("=" * 78)
    print("SECTION 3: Fill Rate Integration")
    print("=" * 78)
    print()

    n = len(trades)
    if n == 0:
        print("No trades.")
        return

    filled = [t for t in trades if t["sim_filled_60s"]]
    fill_rate = len(filled) / n

    fill_times = [t["sim_fill_time"] for t in filled if t["sim_fill_time"] is not None]
    fill_slippages = [
        (t["sim_fill_price"] - t["entry_mid"]) * 100
        for t in filled if t["sim_fill_price"] is not None
    ]

    print(f"Simulated fill rate (60s):     {_fmt_p(fill_rate)}")
    if fill_times:
        print(f"Mean time to fill:             {np.mean(fill_times):.1f}s")
    if fill_slippages:
        print(f"Mean slippage:                 {np.mean(fill_slippages):+.2f}c")
    print()

    if filled:
        filled_pnls = np.array([t["pnl"] for t in filled])
        filled_wins = np.array([t["home_won"] for t in filled])
        print("With fill constraint:")
        print(f"  Filled trades:               {len(filled)}")
        print(f"  Filled win rate:             {_fmt_p(filled_wins.mean())}")
        print(f"  Filled mean PnL:             {filled_pnls.mean():+.4f}")
        realistic_pnl = fill_rate * filled_pnls.mean()
        print(f"  Realistic PnL per signal:    {realistic_pnl:+.4f} (fill_rate x filled_PnL)")
    print()


# ---------------------------------------------------------------------------
# Section 4: Cumulative PnL Curve
# ---------------------------------------------------------------------------


def section_4(trades: list[dict]) -> None:
    print("=" * 78)
    print("SECTION 4: Cumulative PnL Curve")
    print("=" * 78)
    print()

    if not trades:
        print("No trades.")
        return

    pnls = np.array([t["pnl"] for t in trades])
    cum = np.cumsum(pnls)

    # Sampled table
    header = f"{'Trade':>5s}  {'Game PK':>10s}  {'Entry Mid':>9s}  {'Home Won':>8s}  {'PnL':>8s}  {'Cumulative':>10s}"
    print(header)
    print("-" * len(header))

    step = max(1, len(trades) // 20)
    indices = list(range(0, len(trades), step))
    if (len(trades) - 1) not in indices:
        indices.append(len(trades) - 1)

    for i in indices:
        t = trades[i]
        print(
            f"{i + 1:>5d}  {t['game_pk']:>10d}  {t['entry_mid']:>9.4f}  "
            f"{t['home_won']:>8d}  {t['pnl']:>+8.4f}  {cum[i]:>+10.4f}"
        )

    print()
    peak_idx = int(np.argmax(cum))
    trough_idx = int(np.argmin(cum))
    print(f"Peak cumulative PnL:           {cum[peak_idx]:+.4f} (after trade {peak_idx + 1})")
    print(f"Trough cumulative PnL:         {cum[trough_idx]:+.4f} (after trade {trough_idx + 1})")
    print(f"Final cumulative PnL:          {cum[-1]:+.4f}")
    print()


# ---------------------------------------------------------------------------
# Section 5: Cross-Validation with Prior Analyses
# ---------------------------------------------------------------------------


def section_5(trades: list[dict]) -> None:
    print("=" * 78)
    print("SECTION 5: Cross-Validation with Prior Analyses")
    print("=" * 78)
    print()

    if not trades:
        print("No trades.")
        return

    pnls = np.array([t["pnl"] for t in trades])
    wins = np.array([t["home_won"] for t in trades])
    cum = np.cumsum(pnls)
    peak = np.maximum.accumulate(cum)
    max_dd = float((cum - peak).min())

    print(f"{'Metric':<30s}  {'This backtest':>14s}  {'deep_orderbook A6':>18s}")
    print("-" * 68)
    print(f"{'Games with trades':<30s}  {len(trades):>14d}  {'93':>18s}")
    print(f"{'Mean PnL':<30s}  {pnls.mean():>+14.4f}  {'+0.0556':>18s}")
    print(f"{'Median PnL':<30s}  {np.median(pnls):>+14.4f}  {'+0.2850':>18s}")
    print(f"{'Win rate':<30s}  {_fmt_p(wins.mean()):>14s}  {'59.1%':>18s}")
    print(f"{'Final cumulative PnL':<30s}  {cum[-1]:>+14.4f}  {'+5.17':>18s}")
    print(f"{'Max drawdown':<30s}  {max_dd:>+14.4f}  {'-3.21':>18s}")
    print()
    print("NOTE: Entry rules differ — Analysis 6 used 'wide spread + model YES',")
    print("this backtest uses 'first ask_heavy per game'. Directional consistency")
    print("is the validation target, not exact match.")
    print()


# ---------------------------------------------------------------------------
# Section 6: Additional Entry Opportunity Analysis
# ---------------------------------------------------------------------------


def section_6(trades: list[dict]) -> None:
    print("=" * 78)
    print("SECTION 6: Additional Entry Opportunity Analysis")
    print("=" * 78)
    print()

    if not trades:
        print("No trades.")
        return

    zero = [t for t in trades if t["additional_entries"] == 0]
    one = [t for t in trades if t["additional_entries"] == 1]
    two_plus = [t for t in trades if t["additional_entries"] >= 2]

    print(f"Games with 0 additional opportunities:    {len(zero)}")
    print(f"Games with 1 additional opportunity:      {len(one)}")
    print(f"Games with 2+ additional opportunities:   {len(two_plus)}")
    print()

    # Analyze additional entries
    all_add_mids: list[float] = []
    all_add_pnls: list[float] = []
    for t in trades:
        for add_mid in t["additional_mids"]:
            all_add_mids.append(add_mid)
            all_add_pnls.append(t["settlement"] - add_mid)

    if all_add_mids:
        print("If additional entries at 2c improvement were taken:")
        print(f"  Total additional entries:                {len(all_add_mids)}")
        print(f"  Mean additional entry mid:              {np.mean(all_add_mids):.4f}")
        print(f"  Mean additional entry PnL:              {np.mean(all_add_pnls):+.4f}")

        # Compare: original-only vs original+additional
        orig_pnls = [t["pnl"] for t in trades]
        combined_pnls = orig_pnls + all_add_pnls
        orig_mean = np.mean(orig_pnls)
        combined_mean = np.mean(combined_pnls)
        improvement = combined_mean > orig_mean
        print(f"  Original mean PnL:                     {orig_mean:+.4f}")
        print(f"  Combined mean PnL (orig + additional):  {combined_mean:+.4f}")
        print(f"  Would additional entries improve PnL?   {'YES' if improvement else 'NO'}")
    else:
        print("No additional entry opportunities found.")
    print()


# ---------------------------------------------------------------------------
# Section 7: Trade Log Export
# ---------------------------------------------------------------------------


def section_7(trades: list[dict]) -> None:
    print("=" * 78)
    print("SECTION 7: Trade Log Export")
    print("=" * 78)
    print()

    CSV_OUT.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "game_pk", "game_date", "entry_ts", "entry_mid", "entry_spread",
        "entry_depth_ratio", "entry_inning", "entry_score_diff",
        "home_favored", "home_won", "pnl",
        "sim_filled_60s", "sim_fill_price", "sim_fill_time",
    ]

    with open(CSV_OUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for t in trades:
            row = {k: t.get(k) for k in fieldnames}
            row["home_favored"] = int(t["home_favored"])
            row["sim_filled_60s"] = int(t["sim_filled_60s"])
            writer.writerow(row)

    print(f"Saved {len(trades)} trades to {CSV_OUT}")
    print()


# ---------------------------------------------------------------------------
# Section 8: v1 vs v2 Head-to-Head
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


def section_8(v1_trades: list[dict], v2_trades: list[dict]) -> None:
    print("=" * 78)
    print("SECTION 8: v1 (Single Entry) vs v2 (DCA) Comparison")
    print("=" * 78)
    print()

    if not v1_trades or not v2_trades:
        print("Insufficient trades for comparison.")
        print()
        return

    v1_pnls = np.array([t["pnl"] for t in v1_trades])
    v2_pnls = np.array([t["pnl_per_dollar"] for t in v2_trades])
    v1_wins = np.array([t["home_won"] for t in v1_trades])
    v2_wins = np.array([t["home_won"] for t in v2_trades])

    v1_cum = np.cumsum(v1_pnls)
    v2_cum = np.cumsum(v2_pnls)
    v1_dd = float((v1_cum - np.maximum.accumulate(v1_cum)).min())
    v2_dd = float((v2_cum - np.maximum.accumulate(v2_cum)).min())

    v1_sharpe = float(v1_pnls.mean() / v1_pnls.std()) if v1_pnls.std() > 0 else 0.0
    v2_sharpe = float(v2_pnls.mean() / v2_pnls.std()) if v2_pnls.std() > 0 else 0.0

    v1_ci = bootstrap_ci(v1_pnls)
    v2_ci = bootstrap_ci(v2_pnls)

    col1 = "v1 (first entry)"
    col2 = "v2 (DCA)"
    w = 20

    print(f"{'':36s}  {col1:>{w}s}  {col2:>{w}s}")
    print("-" * (36 + 2 + w + 2 + w))
    print(f"{'Trades (games entered)':<36s}  {len(v1_trades):>{w}d}  {len(v2_trades):>{w}d}")
    print(f"{'Mean PnL per $1 invested':<36s}  {v1_pnls.mean():>{w}.4f}  {v2_pnls.mean():>{w}.4f}")
    print(f"{'Median PnL per $1':<36s}  {np.median(v1_pnls):>{w}.4f}  {np.median(v2_pnls):>{w}.4f}")
    print(f"{'Std PnL per $1':<36s}  {v1_pnls.std():>{w}.4f}  {v2_pnls.std():>{w}.4f}")
    print(f"{'Sharpe':<36s}  {v1_sharpe:>{w}.3f}  {v2_sharpe:>{w}.3f}")
    print(f"{'Win rate':<36s}  {_fmt_p(v1_wins.mean()):>{w}s}  {_fmt_p(v2_wins.mean()):>{w}s}")
    print(
        f"{'95% CI':<36s}  "
        f"{'[' + f'{v1_ci[0]:+.4f}, {v1_ci[1]:+.4f}' + ']':>{w}s}  "
        f"{'[' + f'{v2_ci[0]:+.4f}, {v2_ci[1]:+.4f}' + ']':>{w}s}"
    )
    print(f"{'Significant':<36s}  {_sig(v1_ci[0]):>{w}s}  {_sig(v2_ci[0]):>{w}s}")
    print(f"{'Final cumulative (per $1)':<36s}  {v1_cum[-1]:>{w}.4f}  {v2_cum[-1]:>{w}.4f}")
    print(f"{'Max drawdown (per $1)':<36s}  {v1_dd:>{w}.4f}  {v2_dd:>{w}.4f}")
    print()

    # DCA specifics
    n_entries = np.array([t["n_entries"] for t in v2_trades])
    invested = np.array([t["total_invested"] for t in v2_trades])
    durations = np.array([t["entry_duration_seconds"] for t in v2_trades])
    avg_mids = np.array([t["avg_entry_mid"] for t in v2_trades])
    v1_first_mids = np.array([t["entry_mid"] for t in v1_trades])

    dur_mean = durations.mean()
    dur_min = int(dur_mean // 60)
    dur_sec = int(dur_mean % 60)

    print("v2 DCA specifics:")
    print(f"  Mean entries per game:                           {n_entries.mean():.0f}")
    print(f"  Median entries per game:                         {np.median(n_entries):.0f}")
    print(f"  Mean total invested per game:                   ${invested.mean():.2f}")
    print(f"  Mean entry duration:                            {dur_min}m {dur_sec}s")
    print(f"  Mean avg_mid (DCA entry price):                 {avg_mids.mean():.4f}")
    print(f"  vs v1 mean first_entry_mid:                     {v1_first_mids.mean():.4f}")
    improvement = (v1_first_mids.mean() - avg_mids.mean()) * 100
    print(f"  Price improvement from DCA:                     {improvement:+.2f}c")
    print()


# ---------------------------------------------------------------------------
# Section 9: v2 DCA Breakdown
# ---------------------------------------------------------------------------


def section_9(v2_trades: list[dict]) -> None:
    print("=" * 78)
    print("SECTION 9: v2 DCA Breakdown")
    print("=" * 78)
    print()

    if not v2_trades:
        print("No DCA trades.")
        print()
        return

    header = f"{'':30s}  {'N':>4s}  {'Win%':>6s}  {'Mean PnL':>8s}  {'95% CI':>17s}  {'Sig':>3s}"
    print(header)
    print("-" * len(header))

    _dca_breakdown_row("All DCA trades", v2_trades)

    home_fav = [t for t in v2_trades if t["home_favored_at_avg"]]
    away_fav = [t for t in v2_trades if not t["home_favored_at_avg"]]
    _dca_breakdown_row("Home favored (avg_mid>0.50)", home_fav, "  ")
    _dca_breakdown_row("Away favored (avg_mid<0.50)", away_fav, "  ")
    print()

    # By n_entries quartile
    n_entries_arr = sorted([t["n_entries"] for t in v2_trades])
    if len(n_entries_arr) >= 4:
        q1 = np.percentile(n_entries_arr, 25)
        q3 = np.percentile(n_entries_arr, 75)
        q1_trades = [t for t in v2_trades if t["n_entries"] <= q1]
        q4_trades = [t for t in v2_trades if t["n_entries"] >= q3]
        _dca_breakdown_row(f"Q1 (n_entries <= {q1:.0f})", q1_trades)
        _dca_breakdown_row(f"Q4 (n_entries >= {q3:.0f})", q4_trades)
        print()

    # By entry duration
    for label, lo, hi in [
        ("<1 min", 0, 60),
        ("1-10 min", 60, 600),
        ("10-60 min", 600, 3600),
        ("60+ min", 3600, float("inf")),
    ]:
        sub = [t for t in v2_trades if lo <= t["entry_duration_seconds"] < hi]
        _dca_breakdown_row(f"Duration {label}", sub)
    print()


# ---------------------------------------------------------------------------
# Section 10: v2 Cumulative PnL Curve
# ---------------------------------------------------------------------------


def section_10(v2_trades: list[dict]) -> None:
    print("=" * 78)
    print("SECTION 10: v2 DCA Cumulative PnL Curve")
    print("=" * 78)
    print()

    if not v2_trades:
        print("No DCA trades.")
        return

    pnls = np.array([t["pnl_per_dollar"] for t in v2_trades])
    cum = np.cumsum(pnls)

    header = (
        f"{'Trade':>5s}  {'Game PK':>10s}  {'N Entries':>9s}  "
        f"{'Avg Mid':>7s}  {'Won':>3s}  {'PnL/$1':>8s}  {'Cumulative':>10s}"
    )
    print(header)
    print("-" * len(header))

    step = max(1, len(v2_trades) // 20)
    indices = list(range(0, len(v2_trades), step))
    if (len(v2_trades) - 1) not in indices:
        indices.append(len(v2_trades) - 1)

    for i in indices:
        t = v2_trades[i]
        print(
            f"{i + 1:>5d}  {t['game_pk']:>10d}  {t['n_entries']:>9d}  "
            f"{t['avg_entry_mid']:>7.4f}  {t['home_won']:>3d}  "
            f"{t['pnl_per_dollar']:>+8.4f}  {cum[i]:>+10.4f}"
        )

    print()
    peak_idx = int(np.argmax(cum))
    trough_idx = int(np.argmin(cum))
    print(f"Peak cumulative PnL/$1:        {cum[peak_idx]:+.4f} (after trade {peak_idx + 1})")
    print(f"Trough cumulative PnL/$1:      {cum[trough_idx]:+.4f} (after trade {trough_idx + 1})")
    print(f"Final cumulative PnL/$1:       {cum[-1]:+.4f}")
    print()


# ---------------------------------------------------------------------------
# Section 11: v2 Entry Price Distribution
# ---------------------------------------------------------------------------


def section_11(v1_trades: list[dict], v2_trades: list[dict]) -> None:
    print("=" * 78)
    print("SECTION 11: v2 Entry Price Distribution")
    print("=" * 78)
    print()

    if not v2_trades:
        print("No DCA trades.")
        print()
        return

    avg_mids = np.array([t["avg_entry_mid"] for t in v2_trades])
    first_mids = np.array([t["first_entry_mid"] for t in v2_trades])
    min_mids = np.array([t["min_entry_mid"] for t in v2_trades])
    max_mids = np.array([t["max_entry_mid"] for t in v2_trades])
    intra_spreads = max_mids - min_mids

    print("Price distribution across DCA entries:")
    print(f"  Mean(avg_mid):       {avg_mids.mean():.4f}")
    print(f"  Mean(first_mid):     {first_mids.mean():.4f}")
    print(f"  Mean(min_mid):       {min_mids.mean():.4f}")
    print(f"  Mean(max_mid):       {max_mids.mean():.4f}")
    print(f"  Mean intra-game spread (max - min):  {intra_spreads.mean():.4f}")
    print()

    # Match v1 and v2 trades by game_pk for per-game comparison
    v1_by_gpk = {t["game_pk"]: t for t in v1_trades}
    dca_helped = 0
    dca_hurt = 0
    improvements: list[float] = []
    damages: list[float] = []

    for t2 in v2_trades:
        t1 = v1_by_gpk.get(t2["game_pk"])
        if t1 is None:
            continue
        # Lower avg_mid is better for YES buyer (pays less)
        diff = t1["entry_mid"] - t2["avg_entry_mid"]  # positive = DCA improved
        if diff > 0:
            dca_helped += 1
            improvements.append(diff)
        elif diff < 0:
            dca_hurt += 1
            damages.append(diff)

    total_matched = len([t2 for t2 in v2_trades if t2["game_pk"] in v1_by_gpk])

    if total_matched > 0:
        print(f"Games where DCA improved vs first entry:     {dca_helped}/{total_matched} ({dca_helped / total_matched * 100:.1f}%)")
    if improvements:
        print(f"Mean improvement when DCA helped:            {np.mean(improvements) * 100:+.2f}c")
    if damages:
        print(f"Mean damage when DCA hurt:                   {np.mean(damages) * 100:+.2f}c")
    print()


# ---------------------------------------------------------------------------
# Section 7b: v2 Trade Log Export
# ---------------------------------------------------------------------------


def section_7b(v2_trades: list[dict]) -> None:
    CSV_OUT_DCA.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "game_pk", "game_date", "n_entries", "total_invested",
        "avg_entry_mid", "first_entry_mid", "min_entry_mid", "max_entry_mid",
        "avg_spread", "avg_depth_ratio", "entry_duration_seconds",
        "home_favored_at_avg", "home_won", "pnl_per_dollar", "game_pnl",
    ]

    with open(CSV_OUT_DCA, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for t in v2_trades:
            row = {k: t.get(k) for k in fieldnames}
            row["home_favored_at_avg"] = int(t["home_favored_at_avg"])
            writer.writerow(row)

    print(f"Saved {len(v2_trades)} DCA trades to {CSV_OUT_DCA}")
    print()


# ---------------------------------------------------------------------------
# Section 12: Cap Sweep
# ---------------------------------------------------------------------------


def _cap_label(cap: float) -> str:
    return "unlim" if cap == float("inf") else f"${cap:.0f}"


def section_12(games: list[dict]) -> dict[float, list[dict]]:
    """Run cap sweep and print results. Returns {cap: trades} for reuse."""
    print("=" * 78)
    print("SECTION 12: DCA Cap Sweep")
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
    return sweep


# ---------------------------------------------------------------------------
# Section 13: v3 Detail Breakdown
# ---------------------------------------------------------------------------


def section_13(v3_trades: list[dict], cap: float) -> None:
    print("=" * 78)
    print(f"SECTION 13: v3 ({_cap_label(cap)} cap) Breakdown")
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
# Section 14: Risk Analysis
# ---------------------------------------------------------------------------


def section_14(v3_trades: list[dict], cap: float) -> None:
    print("=" * 78)
    print(f"SECTION 14: Risk Analysis ({_cap_label(cap)} cap)")
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
    print(f"  Expected annual Sharpe:     {season_pnls.mean() / season_pnls.std():.2f}" if season_pnls.std() > 0 else "")
    lo_95 = np.percentile(season_pnls, 2.5)
    hi_95 = np.percentile(season_pnls, 97.5)
    print(f"  95% annual PnL range:       [${lo_95:+,.0f}, ${hi_95:+,.0f}]")
    print(f"  P(negative year):           {_fmt_p(p_neg)}")
    print()


# ---------------------------------------------------------------------------
# Section 15: v1 vs v2 vs v3 Final Comparison
# ---------------------------------------------------------------------------


def section_15(
    v1_trades: list[dict],
    v2_trades: list[dict],
    v3_trades: list[dict],
) -> None:
    print("=" * 78)
    print("SECTION 15: All Versions Comparison")
    print("=" * 78)
    print()

    def _stats(trades_list: list[dict], pnl_key: str) -> dict:
        pnls = np.array([t[pnl_key] for t in trades_list])
        invested = np.array([t.get("total_invested", 1.0) for t in trades_list])
        ci_lo, _ = bootstrap_ci(pnls)
        cum = np.cumsum(pnls)
        peak = np.maximum.accumulate(cum)
        max_dd = float((cum - peak).min())
        sharpe = float(pnls.mean() / pnls.std()) if pnls.std() > 0 else 0.0
        # Worst single game loss in dollars
        game_pnls = pnls * invested
        worst_loss = float(game_pnls.min())
        return {
            "pnl": float(pnls.mean()),
            "sharpe": sharpe,
            "ci_lo": ci_lo,
            "sig": ci_lo > 0,
            "max_dd": max_dd,
            "mean_inv": float(invested.mean()),
            "worst_loss": worst_loss,
        }

    s1 = _stats(v1_trades, "pnl")
    s2 = _stats(v2_trades, "pnl_per_dollar")
    s3 = _stats(v3_trades, "pnl_per_dollar")

    w = 14
    obs = "+0.1079"
    print(f"{'':28s}  {'v1 (first)':>{w}s}  {'v2 ($100)':>{w}s}  {'v3 ($1000)':>{w}s}  {'obs-level':>{w}s}")
    print("-" * (28 + 4 * (w + 2)))

    print(f"{'PnL per $1':<28s}  {s1['pnl']:>{w}.4f}  {s2['pnl']:>{w}.4f}  {s3['pnl']:>{w}.4f}  {obs:>{w}s}")
    print(f"{'Sharpe':<28s}  {s1['sharpe']:>{w}.3f}  {s2['sharpe']:>{w}.3f}  {s3['sharpe']:>{w}.3f}  {'--':>{w}s}")
    print(f"{'95% CI lo':<28s}  {s1['ci_lo']:>{w}.4f}  {s2['ci_lo']:>{w}.4f}  {s3['ci_lo']:>{w}.4f}  {'--':>{w}s}")

    def _yn(b: bool) -> str:
        return "YES" if b else "NO"
    print(f"{'Significant':<28s}  {_yn(s1['sig']):>{w}s}  {_yn(s2['sig']):>{w}s}  {_yn(s3['sig']):>{w}s}  {'YES':>{w}s}")
    print(f"{'Max DD per $1':<28s}  {s1['max_dd']:>{w}.4f}  {s2['max_dd']:>{w}.4f}  {s3['max_dd']:>{w}.4f}  {'--':>{w}s}")
    print(f"{'Mean invested/game':<28s}  {'$1':>{w}s}  ${s2['mean_inv']:>{w - 1}.2f}  ${s3['mean_inv']:>{w - 1}.2f}  {'--':>{w}s}")
    print(f"{'Worst single game loss':<28s}  ${abs(s1['worst_loss']):>{w - 1}.2f}  ${abs(s2['worst_loss']):>{w - 1}.2f}  ${abs(s3['worst_loss']):>{w - 1}.2f}  {'--':>{w}s}")
    print()


# ---------------------------------------------------------------------------
# Section 7c: v3 Trade Log Export
# ---------------------------------------------------------------------------


def section_7c(v3_trades: list[dict]) -> None:
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

    print(f"Saved {len(v3_trades)} v3 trades to {CSV_OUT_V3}")
    print()


# ---------------------------------------------------------------------------
# Section 16: Kelly Fraction Sweep
# ---------------------------------------------------------------------------


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


def section_16(games: list[dict]) -> dict[float, list[dict]]:
    """Kelly fraction sweep with fixed f*. Returns {fraction: trades}."""
    print("=" * 78)
    print("SECTION 16: Kelly Fraction Sweep (fixed f*)")
    print("=" * 78)
    print()
    print(f"Initial bankroll: ${BANKROLL:,}")
    print()

    results: dict[float, list[dict]] = {}

    header = (
        f"{'Fraction':>10s}  {'Eff %':>5s}  {'Final BR':>11s}  {'ROI':>7s}  "
        f"{'Sharpe':>6s}  {'Max DD%':>7s}  {'Max DD$':>9s}  {'Worst game':>10s}"
    )
    print(header)
    print("-" * len(header))

    for frac, label in KELLY_FRACTIONS:
        trades = run_strategy_kelly(
            games, initial_bankroll=BANKROLL, kelly_fraction=frac, quiet=True,
        )
        results[frac] = trades
        s = _kelly_stats(trades, BANKROLL)
        print(
            f"{label:>10s}  {frac * 53.5:>4.1f}%  ${s['final_br']:>10,.2f}  "
            f"{s['roi']:>+6.1f}%  {s['sharpe']:>6.3f}  {s['max_dd_pct']:>+6.1f}%  "
            f"${s['max_dd_dollar']:>+8,.2f}  ${s['worst_game']:>+9,.2f}"
        )

    print()
    return results


# ---------------------------------------------------------------------------
# Section 17: Kelly Bankroll Evolution
# ---------------------------------------------------------------------------


def section_17(kelly_trades: list[dict], frac_label: str) -> None:
    print("=" * 78)
    print(f"SECTION 17: Bankroll Evolution ({frac_label})")
    print("=" * 78)
    print()

    if not kelly_trades:
        print("No trades.")
        return

    header = (
        f"{'Trade':>5s}  {'Game PK':>10s}  {'BR before':>12s}  {'Cap':>8s}  "
        f"{'Invested':>8s}  {'Avg mid':>7s}  {'Won':>3s}  {'Game PnL':>10s}  {'BR after':>12s}"
    )
    print(header)
    print("-" * len(header))

    step = max(1, len(kelly_trades) // 20)
    indices = list(range(0, len(kelly_trades), step))
    if (len(kelly_trades) - 1) not in indices:
        indices.append(len(kelly_trades) - 1)

    for i in indices:
        t = kelly_trades[i]
        print(
            f"{i + 1:>5d}  {t['game_pk']:>10d}  ${t['bankroll_before']:>11,.2f}  "
            f"${t['game_cap']:>7,.0f}  ${t['total_invested']:>7,.0f}  "
            f"{t['avg_entry_mid']:>7.4f}  {t['home_won']:>3d}  "
            f"${t['game_pnl']:>+9,.2f}  ${t['bankroll_after']:>11,.2f}"
        )

    print()
    brs = [BANKROLL] + [t["bankroll_after"] for t in kelly_trades]
    peak_idx = int(np.argmax(brs))
    trough_idx = int(np.argmin(brs))
    print(f"Peak bankroll:    ${brs[peak_idx]:>,.2f} (after trade {peak_idx})")
    print(f"Trough bankroll:  ${brs[trough_idx]:>,.2f} (after trade {trough_idx})")
    print(f"Final bankroll:   ${brs[-1]:>,.2f}")
    print(f"Total ROI:        {(brs[-1] - BANKROLL) / BANKROLL * 100:+.1f}%")
    print()


# ---------------------------------------------------------------------------
# Section 18: Fixed vs Rolling f*
# ---------------------------------------------------------------------------


def section_18(games: list[dict], kelly_fraction: float, frac_label: str) -> None:
    print("=" * 78)
    print(f"SECTION 18: Fixed vs Rolling f* ({frac_label})")
    print("=" * 78)
    print()

    fixed_trades = run_strategy_kelly(
        games, kelly_fraction=kelly_fraction, use_rolling=False, quiet=True,
    )
    rolling_trades = run_strategy_kelly(
        games, kelly_fraction=kelly_fraction, use_rolling=True, quiet=True,
    )

    sf = _kelly_stats(fixed_trades, BANKROLL)
    sr = _kelly_stats(rolling_trades, BANKROLL)

    w = 14
    print(f"{'':28s}  {'Fixed f*':>{w}s}  {'Rolling f*':>{w}s}")
    print("-" * (28 + 2 * (w + 2)))
    print(f"{'Final bankroll':<28s}  ${sf['final_br']:>{w - 1},.2f}  ${sr['final_br']:>{w - 1},.2f}")
    print(f"{'ROI':<28s}  {sf['roi']:>{w - 1}.1f}%  {sr['roi']:>{w - 1}.1f}%")
    print(f"{'Max DD%':<28s}  {sf['max_dd_pct']:>{w - 1}.1f}%  {sr['max_dd_pct']:>{w - 1}.1f}%")
    print(f"{'Sharpe':<28s}  {sf['sharpe']:>{w}.3f}  {sr['sharpe']:>{w}.3f}")
    print()

    # Rolling f* evolution
    if rolling_trades:
        print("Rolling f* evolution:")
        checkpoints = [10, 25, 50, 75, len(rolling_trades)]
        for cp in checkpoints:
            if cp > len(rolling_trades):
                continue
            t = rolling_trades[cp - 1]
            print(f"  After {cp:>2d} games:  f* = {t['f_star']:.3f}")
    print()


# ---------------------------------------------------------------------------
# Section 19: Kelly vs Fixed Cap Comparison
# ---------------------------------------------------------------------------


def section_19(
    sweep: dict[float, list[dict]],
    kelly_sweep: dict[float, list[dict]],
) -> None:
    print("=" * 78)
    print("SECTION 19: Kelly vs Fixed Cap")
    print("=" * 78)
    print()

    # Pick representative fixed caps and Kelly fractions
    fixed_caps = [500, 1000, 2000]
    kelly_frac_picks = [0.125, 0.25]

    cols: list[tuple[str, dict]] = []

    for cap in fixed_caps:
        if cap in sweep and sweep[cap]:
            trades = sweep[cap]
            game_pnls = np.array([t["game_pnl"] for t in trades])
            pnls = np.array([t["pnl_per_dollar"] for t in trades])
            cum = np.cumsum(game_pnls)
            # Simulate bankroll evolution for fixed cap
            br = float(BANKROLL)
            worst = 0.0
            brs = [br]
            for gp in game_pnls:
                br += gp
                brs.append(br)
                worst = min(worst, gp)
            brs_arr = np.array(brs)
            peak = np.maximum.accumulate(brs_arr)
            dd_pct = float(((brs_arr - peak) / np.where(peak > 0, peak, 1.0)).min()) * 100
            ci_lo, _ = bootstrap_ci(pnls)
            cols.append((f"Fixed ${cap}", {
                "final_br": brs_arr[-1],
                "roi": (brs_arr[-1] - BANKROLL) / BANKROLL * 100,
                "max_dd_pct": dd_pct,
                "worst_game": worst,
                "sharpe": float(pnls.mean() / pnls.std()) if pnls.std() > 0 else 0.0,
                "sig": ci_lo > 0,
            }))

    for frac in kelly_frac_picks:
        if frac in kelly_sweep and kelly_sweep[frac]:
            trades = kelly_sweep[frac]
            s = _kelly_stats(trades, BANKROLL)
            pnls = np.array([t["pnl_per_dollar"] for t in trades])
            ci_lo, _ = bootstrap_ci(pnls)
            label = [l for f, l in KELLY_FRACTIONS if f == frac][0].strip()
            cols.append((label, {
                "final_br": s["final_br"],
                "roi": s["roi"],
                "max_dd_pct": s["max_dd_pct"],
                "worst_game": s["worst_game"],
                "sharpe": s["sharpe"],
                "sig": ci_lo > 0,
            }))

    if not cols:
        print("No data.")
        return

    w = 14
    header_labels = [c[0] for c in cols]
    print(f"{'':22s}  " + "  ".join(f"{h:>{w}s}" for h in header_labels))
    print("-" * (22 + len(cols) * (w + 2)))

    def _row(label: str, key: str, fmt: str) -> str:
        parts = [f"{label:<22s}"]
        for _, s in cols:
            val = s[key]
            parts.append(f"{fmt.format(val):>{w}s}")
        return "  ".join(parts)

    print(_row("Final bankroll", "final_br", "${:,.0f}"))
    print(_row("ROI", "roi", "{:+.1f}%"))
    print(_row("Max DD (% of BR)", "max_dd_pct", "{:.1f}%"))
    print(_row("Worst game ($)", "worst_game", "${:+,.0f}"))
    print(_row("Sharpe", "sharpe", "{:.3f}"))

    sig_parts = [f"{'Significant':<22s}"]
    for _, s in cols:
        sig_parts.append(f"{'YES' if s['sig'] else 'NO':>{w}s}")
    print("  ".join(sig_parts))
    print()


# ---------------------------------------------------------------------------
# Section 7d: Kelly Trade Log Export
# ---------------------------------------------------------------------------


def section_7d(kelly_trades: list[dict]) -> None:
    CSV_OUT_KELLY.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "game_pk", "game_date", "kelly_fraction", "f_star",
        "bankroll_before", "game_cap", "n_entries", "total_invested",
        "avg_entry_mid", "first_entry_mid", "home_won",
        "pnl_per_dollar", "game_pnl", "bankroll_after",
    ]

    with open(CSV_OUT_KELLY, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for t in kelly_trades:
            writer.writerow({k: t.get(k) for k in fieldnames})

    print(f"Saved {len(kelly_trades)} Kelly trades to {CSV_OUT_KELLY}")
    print()


# ---------------------------------------------------------------------------
# Section 20: Game Concurrency Profile
# ---------------------------------------------------------------------------


def section_20(games: list[dict]) -> None:
    print("=" * 78)
    print("SECTION 20: Game Concurrency Profile")
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
    # f* from full sample
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
# Section 21: Sequential vs Concurrent Kelly
# ---------------------------------------------------------------------------


def section_21(v4_trades: list[dict], v5_trades: list[dict], frac_label: str) -> None:
    print("=" * 78)
    print(f"SECTION 21: Sequential vs Concurrent Kelly ({frac_label})")
    print("=" * 78)
    print()

    if not v4_trades or not v5_trades:
        print("Insufficient trades.")
        return

    s4 = _kelly_stats(v4_trades, BANKROLL)
    s5 = _kelly_stats(v5_trades, BANKROLL)

    caps_4 = np.array([t["game_cap"] for t in v4_trades])
    caps_5 = np.array([t["game_cap"] for t in v5_trades])

    # Max total exposure: sum of all active game caps at peak
    # For sequential v4: only 1 game at a time, so max = max single cap
    max_exp_4 = float(caps_4.max()) / BANKROLL * 100

    # For concurrent v5: check n_concurrent_at_start * cap as proxy
    max_exp_5_est = 0.0
    for t in v5_trades:
        exp = t["game_cap"] * t.get("n_concurrent_at_start", 1)
        frac_br = exp / t["bankroll_before"] * 100 if t["bankroll_before"] > 0 else 0
        max_exp_5_est = max(max_exp_5_est, frac_br)

    w = 18
    print(f"{'':28s}  {'Sequential v4':>{w}s}  {'Concurrent v5':>{w}s}")
    print("-" * (28 + 2 * (w + 2)))
    print(f"{'Final bankroll':<28s}  ${s4['final_br']:>{w - 1},.2f}  ${s5['final_br']:>{w - 1},.2f}")
    print(f"{'ROI':<28s}  {s4['roi']:>{w - 1}.1f}%  {s5['roi']:>{w - 1}.1f}%")
    print(f"{'Sharpe':<28s}  {s4['sharpe']:>{w}.3f}  {s5['sharpe']:>{w}.3f}")
    print(f"{'Max DD%':<28s}  {s4['max_dd_pct']:>{w - 1}.1f}%  {s5['max_dd_pct']:>{w - 1}.1f}%")
    print(f"{'Max DD$':<28s}  ${s4['max_dd_dollar']:>{w - 1},.2f}  ${s5['max_dd_dollar']:>{w - 1},.2f}")
    print(f"{'Mean cap/game':<28s}  ${caps_4.mean():>{w - 1},.0f}  ${caps_5.mean():>{w - 1},.0f}")
    print(f"{'Max total exposure':<28s}  {max_exp_4:>{w - 1}.1f}%  {max_exp_5_est:>{w - 1}.1f}%")
    print(f"{'Worst game loss':<28s}  ${s4['worst_game']:>{w - 1},.2f}  ${s5['worst_game']:>{w - 1},.2f}")
    print()


# ---------------------------------------------------------------------------
# Section 22: Concurrent Kelly Fraction Sweep
# ---------------------------------------------------------------------------


def section_22(games: list[dict]) -> dict[float, list[dict]]:
    """Concurrent Kelly fraction sweep. Returns {fraction: trades}."""
    print("=" * 78)
    print("SECTION 22: Concurrent Kelly Fraction Sweep")
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
# Section 7e: Concurrent Kelly Trade Log Export
# ---------------------------------------------------------------------------


def section_7e(v5_trades: list[dict]) -> None:
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
    v1_trades: list[dict],
    v2_trades: list[dict],
    v3_trades: list[dict],
    v4_trades: list[dict],
    v5_trades: list[dict],
    kelly_conc_sweep: dict[float, list[dict]],
) -> None:
    print("=" * 78)
    print("VALIDATION RESULT")
    print("=" * 78)

    if not v1_trades:
        print("No trades -- cannot validate.")
        print("=" * 78)
        return

    v1_pnls = np.array([t["pnl"] for t in v1_trades])
    v1_ci = bootstrap_ci(v1_pnls)
    v1_sig = v1_ci[0] > 0
    print(f"v1 (first entry):    PnL {v1_pnls.mean():+.4f}, {'SIGNIFICANT' if v1_sig else 'NOT significant'}")

    v2_pnls = np.array([t["pnl_per_dollar"] for t in v2_trades])
    v2_ci = bootstrap_ci(v2_pnls)
    v2_sig = v2_ci[0] > 0
    print(f"v2 (DCA $100):       PnL {v2_pnls.mean():+.4f}, {'SIGNIFICANT' if v2_sig else 'NOT significant'}")

    v3_pnls = np.array([t["pnl_per_dollar"] for t in v3_trades])
    v3_ci = bootstrap_ci(v3_pnls)
    v3_sig = v3_ci[0] > 0
    print(f"v3 (DCA $1000):      PnL {v3_pnls.mean():+.4f}, {'SIGNIFICANT' if v3_sig else 'NOT significant'}")

    if v4_trades:
        v4_s = _kelly_stats(v4_trades, BANKROLL)
        print(f"v4 (Seq. Kelly):     Final BR ${v4_s['final_br']:,.2f}, ROI {v4_s['roi']:+.1f}%")

    if v5_trades:
        v5_s = _kelly_stats(v5_trades, BANKROLL)
        print(f"v5 (Conc. Kelly):    Final BR ${v5_s['final_br']:,.2f}, ROI {v5_s['roi']:+.1f}%, Max DD {v5_s['max_dd_pct']:.1f}%")
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

    validated = v1_pnls.mean() > 0 or v3_pnls.mean() > 0
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
        "--game-budget", type=float, default=DEFAULT_GAME_BUDGET,
        help="v2 DCA: max $ invested per game (default: 100)",
    )
    parser.add_argument(
        "--v3-cap", type=float, default=DEFAULT_V3_CAP,
        help="v3 DCA: max $ invested per game (default: 1000)",
    )
    parser.add_argument(
        "--kelly-fraction", type=float, default=0.125,
        help="v4/v5 Kelly fraction (default: 0.125 = 1/8 Kelly)",
    )
    args = parser.parse_args()

    cache_path = Path(args.cache)
    games = load_games(cache_path)
    total_games = len(games)

    # v1: single entry
    trades = run_strategy(games)

    section_1(trades, total_games)
    section_2(trades)
    section_3(trades)
    section_4(trades)
    section_5(trades)
    section_6(trades)
    section_7(trades)

    # v2: DCA ($100 cap)
    dca_trades = run_strategy_dca(games, game_budget=args.game_budget)

    section_8(trades, dca_trades)
    section_9(dca_trades)
    section_10(dca_trades)
    section_11(trades, dca_trades)
    section_7b(dca_trades)

    # v3: Variable-cap DCA
    v3_trades = run_strategy_dca_variable(games, game_cap=args.v3_cap)

    sweep = section_12(games)
    section_13(v3_trades, args.v3_cap)
    section_14(v3_trades, args.v3_cap)
    section_15(trades, dca_trades, v3_trades)
    section_7c(v3_trades)

    # v4: Sequential Kelly DCA
    frac_label = [l for f, l in KELLY_FRACTIONS if f == args.kelly_fraction]
    frac_label = frac_label[0].strip() if frac_label else f"{args.kelly_fraction:.3f}"

    v4_trades = run_strategy_kelly(games, kelly_fraction=args.kelly_fraction)

    kelly_sweep = section_16(games)
    section_17(v4_trades, frac_label)
    section_18(games, args.kelly_fraction, frac_label)
    section_19(sweep, kelly_sweep)
    section_7d(v4_trades)

    # v5: Concurrent Kelly DCA
    v5_trades = run_strategy_kelly_concurrent(
        games, kelly_fraction=args.kelly_fraction,
    )

    section_20(games)
    section_21(v4_trades, v5_trades, frac_label)
    conc_sweep = section_22(games)
    section_7e(v5_trades)

    print_verdict(trades, dca_trades, v3_trades, v4_trades, v5_trades, conc_sweep)


if __name__ == "__main__":
    main()
