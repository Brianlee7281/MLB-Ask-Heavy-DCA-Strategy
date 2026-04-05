"""OrderbookRecorder — persist orderbook snapshots and ask_heavy signals to SQLite.

Follows the same pattern as src/recorder.py but writes to a separate database
(data/orderbook_live.db) with its own schema.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

import structlog

from src.config import Config

log = structlog.get_logger()

_SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"


class OrderbookRecorder:
    """Persists orderbook data to SQLite for Phase 0 validation."""

    def __init__(self, config: Config | None = None) -> None:
        self.config = config or Config()
        db_path = self.config.ORDERBOOK_DB_PATH
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.row_factory = sqlite3.Row
        self._init_schema()
        self._write_buffer: list[tuple[str, tuple[Any, ...]]] = []

    def _init_schema(self) -> None:
        """Create tables from schema.sql if they don't exist."""
        sql = _SCHEMA_PATH.read_text(encoding="utf-8")
        self._conn.executescript(sql)
        self._conn.commit()

    # ── Game Sessions ────────────────────────────────────────────────

    def record_game_session(
        self,
        game_pk: int,
        date: str,
        home_team: str,
        away_team: str,
        kalshi_ticker: str | None = None,
    ) -> None:
        """Insert or update a game session (idempotent on game_pk)."""
        self._conn.execute(
            "INSERT OR IGNORE INTO game_sessions "
            "(game_pk, date, home_team, away_team, kalshi_ticker) "
            "VALUES (?, ?, ?, ?, ?)",
            (game_pk, date, home_team, away_team, kalshi_ticker),
        )
        if kalshi_ticker:
            self._conn.execute(
                "UPDATE game_sessions SET kalshi_ticker = ? WHERE game_pk = ? "
                "AND kalshi_ticker IS NULL",
                (kalshi_ticker, game_pk),
            )
        self._conn.commit()

    def update_game_start(self, game_pk: int) -> None:
        """Set game_start_ts to current time."""
        self._conn.execute(
            "UPDATE game_sessions SET game_start_ts = ? WHERE game_pk = ?",
            (time.time(), game_pk),
        )
        self._conn.commit()

    def update_game_end(
        self,
        game_pk: int,
        home_won: bool,
    ) -> None:
        """Set game_end_ts, home_won, and settlement_price."""
        now = time.time()
        settlement = 1.0 if home_won else 0.0
        self._conn.execute(
            "UPDATE game_sessions SET game_end_ts = ?, home_won = ?, "
            "settlement_price = ? WHERE game_pk = ?",
            (now, int(home_won), settlement, game_pk),
        )
        # Update theoretical_pnl for all ask_heavy signals in this game
        self._conn.execute(
            "UPDATE ask_heavy_signals SET home_won = ?, "
            "theoretical_pnl = ? - mid_at_onset "
            "WHERE game_pk = ?",
            (int(home_won), settlement, game_pk),
        )
        self._conn.commit()

    def increment_error(self, game_pk: int) -> None:
        """Increment monitoring_errors counter for a game session."""
        self._conn.execute(
            "UPDATE game_sessions SET monitoring_errors = monitoring_errors + 1 "
            "WHERE game_pk = ?",
            (game_pk,),
        )
        self._conn.commit()

    # ── Orderbook Snapshots ──────────────────────────────────────────

    def record_snapshot(
        self,
        game_pk: int,
        timestamp: float,
        best_bid: float,
        best_ask: float,
        bid_depth: int,
        ask_depth: int,
        bid_levels: list[list[float]] | None,
        ask_levels: list[list[float]] | None,
        inning: int | None,
        half_inning: str | None,
        outs: int | None,
        home_score: int | None,
        away_score: int | None,
        runners_on: str | None,
        current_pitcher_id: int | None,
    ) -> None:
        """Write one orderbook snapshot row."""
        mid = (best_bid + best_ask) / 2.0
        spread = best_ask - best_bid
        total_depth = bid_depth + ask_depth
        depth_ratio = bid_depth / total_depth if total_depth > 0 else 0.5
        ask_heavy = 1 if depth_ratio < self.config.ASK_HEAVY_THRESHOLD else 0
        home_favored = 1 if mid > 0.50 else 0

        total_bid = sum(int(lv[1]) for lv in bid_levels) if bid_levels else None
        total_ask = sum(int(lv[1]) for lv in ask_levels) if ask_levels else None

        self._conn.execute(
            "INSERT INTO orderbook_snapshots "
            "(game_pk, timestamp, best_bid, best_ask, mid, spread, "
            "bid_depth, ask_depth, depth_ratio, "
            "bid_levels, ask_levels, total_bid_depth, total_ask_depth, "
            "inning, half_inning, outs, home_score, away_score, "
            "runners_on, current_pitcher_id, ask_heavy, home_favored) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                game_pk,
                timestamp,
                best_bid,
                best_ask,
                mid,
                spread,
                bid_depth,
                ask_depth,
                depth_ratio,
                json.dumps(bid_levels) if bid_levels else None,
                json.dumps(ask_levels) if ask_levels else None,
                total_bid,
                total_ask,
                inning,
                half_inning,
                outs,
                home_score,
                away_score,
                runners_on,
                current_pitcher_id,
                ask_heavy,
                home_favored,
            ),
        )
        self._conn.commit()

        # Update snapshot count
        self._conn.execute(
            "UPDATE game_sessions SET total_snapshots = total_snapshots + 1 "
            "WHERE game_pk = ?",
            (game_pk,),
        )
        self._conn.commit()

    # ── Ask Heavy Signals ────────────────────────────────────────────

    def insert_ask_heavy_signal(
        self,
        game_pk: int,
        onset_ts: float,
        mid_at_onset: float,
        spread_at_onset: float,
        depth_ratio_at_onset: float,
        bid_depth_at_onset: int,
        ask_depth_at_onset: int,
        home_favored: bool,
        inning_at_onset: int | None,
        score_diff_at_onset: int | None,
    ) -> int:
        """Insert a new ask_heavy signal onset. Returns the signal row id."""
        cursor = self._conn.execute(
            "INSERT INTO ask_heavy_signals "
            "(game_pk, onset_ts, mid_at_onset, spread_at_onset, "
            "depth_ratio_at_onset, bid_depth_at_onset, ask_depth_at_onset, "
            "home_favored, inning_at_onset, score_diff_at_onset) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                game_pk,
                onset_ts,
                mid_at_onset,
                spread_at_onset,
                depth_ratio_at_onset,
                bid_depth_at_onset,
                ask_depth_at_onset,
                int(home_favored),
                inning_at_onset,
                score_diff_at_onset,
            ),
        )
        self._conn.commit()

        # Update signal count
        self._conn.execute(
            "UPDATE game_sessions SET total_ask_heavy = total_ask_heavy + 1 "
            "WHERE game_pk = ?",
            (game_pk,),
        )
        self._conn.commit()

        signal_id = cursor.lastrowid
        assert signal_id is not None
        return signal_id

    def update_ask_heavy_offset(
        self,
        signal_id: int,
        offset_ts: float,
    ) -> None:
        """Close out an ask_heavy signal with offset time and duration."""
        self._conn.execute(
            "UPDATE ask_heavy_signals SET offset_ts = ?, "
            "duration_seconds = ? - onset_ts "
            "WHERE id = ?",
            (offset_ts, offset_ts, signal_id),
        )
        self._conn.commit()

    def update_signal_mid_after(
        self,
        signal_id: int,
        column: str,
        value: float,
    ) -> None:
        """Update a mid_after_Xs column for price evolution tracking.

        Args:
            signal_id: The ask_heavy_signals row id.
            column: One of mid_after_30s, mid_after_60s, etc.
            value: The mid price at that offset.
        """
        allowed = {
            "mid_after_30s", "mid_after_60s", "mid_after_120s",
            "mid_after_300s", "mid_after_600s",
        }
        if column not in allowed:
            return
        self._conn.execute(
            f"UPDATE ask_heavy_signals SET {column} = ? WHERE id = ?",  # noqa: S608
            (value, signal_id),
        )
        self._conn.commit()

    def update_sim_fill(
        self,
        signal_id: int,
        column: str,
        fill_price: float | None = None,
        fill_time: float | None = None,
    ) -> None:
        """Update simulated fill columns.

        Args:
            signal_id: The ask_heavy_signals row id.
            column: One of sim_fill_30s, sim_fill_60s, sim_fill_300s.
            fill_price: Price of the simulated fill.
            fill_time: Seconds from onset to fill.
        """
        allowed = {"sim_fill_30s", "sim_fill_60s", "sim_fill_300s"}
        if column not in allowed:
            return
        self._conn.execute(
            f"UPDATE ask_heavy_signals SET {column} = 1 WHERE id = ?",  # noqa: S608
            (signal_id,),
        )
        if fill_price is not None:
            self._conn.execute(
                "UPDATE ask_heavy_signals SET sim_fill_price = ? "
                "WHERE id = ? AND sim_fill_price IS NULL",
                (fill_price, signal_id),
            )
        if fill_time is not None:
            self._conn.execute(
                "UPDATE ask_heavy_signals SET sim_fill_time = ? "
                "WHERE id = ? AND sim_fill_time IS NULL",
                (fill_time, signal_id),
            )
        self._conn.commit()

    # ── Trades ───────────────────────────────────────────────────────

    def record_trade(
        self,
        game_pk: int,
        timestamp: float,
        price: float,
        side: str,
        quantity: int,
        best_bid_at_trade: float | None = None,
        best_ask_at_trade: float | None = None,
        depth_ratio_at_trade: float | None = None,
        ask_heavy_at_trade: bool | None = None,
    ) -> None:
        """Write one observed trade row."""
        self._conn.execute(
            "INSERT INTO trades_observed "
            "(game_pk, timestamp, price, side, quantity, "
            "best_bid_at_trade, best_ask_at_trade, "
            "depth_ratio_at_trade, ask_heavy_at_trade) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                game_pk,
                timestamp,
                price,
                side,
                quantity,
                best_bid_at_trade,
                best_ask_at_trade,
                depth_ratio_at_trade,
                int(ask_heavy_at_trade) if ask_heavy_at_trade is not None else None,
            ),
        )
        self._conn.commit()

    # ── Paper Trading ─────────────────────────────────────────────────

    def create_paper_trade(
        self,
        game_pk: int,
        game_cap: float,
    ) -> int:
        """Create a paper_trades row. Returns the row id."""
        cursor = self._conn.execute(
            "INSERT INTO paper_trades (game_pk, game_cap, created_at) "
            "VALUES (?, ?, ?)",
            (game_pk, game_cap, time.time()),
        )
        self._conn.commit()
        row_id = cursor.lastrowid
        assert row_id is not None
        return row_id

    def record_paper_entry(
        self,
        trade_id: int,
        game_pk: int,
        entry: dict[str, Any],
    ) -> None:
        """Write one paper_entries row."""
        self._conn.execute(
            "INSERT INTO paper_entries "
            "(trade_id, game_pk, entry_ts, entry_mid, entry_spread, "
            "entry_depth_ratio, entry_amount, inning, home_score, away_score) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                trade_id,
                game_pk,
                entry["ts"],
                entry["mid"],
                entry.get("spread"),
                entry.get("depth_ratio"),
                entry["amount"],
                entry.get("inning"),
                entry.get("home_score"),
                entry.get("away_score"),
            ),
        )
        self._conn.commit()

    def update_paper_trade_summary(
        self,
        trade_id: int,
        n_entries: int,
        total_invested: float,
        avg_entry_mid: float,
        first_entry_mid: float,
        first_entry_ts: float,
        last_entry_mid: float,
        last_entry_ts: float,
        min_entry_mid: float,
        max_entry_mid: float,
        avg_depth_ratio: float | None,
        avg_spread: float | None,
        avg_inning: float | None,
    ) -> None:
        """Update the paper_trades summary columns."""
        self._conn.execute(
            "UPDATE paper_trades SET "
            "n_entries=?, total_invested=?, avg_entry_mid=?, "
            "first_entry_mid=?, first_entry_ts=?, "
            "last_entry_mid=?, last_entry_ts=?, "
            "min_entry_mid=?, max_entry_mid=?, "
            "avg_depth_ratio=?, avg_spread=?, avg_inning=? "
            "WHERE id=?",
            (
                n_entries, total_invested, avg_entry_mid,
                first_entry_mid, first_entry_ts,
                last_entry_mid, last_entry_ts,
                min_entry_mid, max_entry_mid,
                avg_depth_ratio, avg_spread, avg_inning,
                trade_id,
            ),
        )
        self._conn.commit()

    def update_paper_entry_fill(
        self,
        entry_id: int,
        fill_price: float,
        fill_ts: float,
        fill_seconds: float,
    ) -> None:
        """Mark a paper_entries row as sim-filled."""
        self._conn.execute(
            "UPDATE paper_entries SET sim_filled=1, sim_fill_price=?, "
            "sim_fill_ts=?, sim_fill_seconds=? WHERE id=?",
            (fill_price, fill_ts, fill_seconds, entry_id),
        )
        self._conn.commit()

    def settle_paper_trade(
        self,
        trade_id: int,
        home_won: bool,
        avg_entry_mid: float,
        pnl_per_dollar: float,
        game_pnl: float,
        n_sim_filled: int,
        fill_rate: float,
        avg_fill_time: float | None,
    ) -> None:
        """Settle a paper trade with final PnL."""
        self._conn.execute(
            "UPDATE paper_trades SET "
            "home_won=?, pnl_per_dollar=?, game_pnl=?, "
            "n_sim_filled=?, fill_rate=?, avg_fill_time=?, "
            "avg_entry_mid=?, status='settled', settled_at=? "
            "WHERE id=?",
            (
                int(home_won), pnl_per_dollar, game_pnl,
                n_sim_filled, fill_rate, avg_fill_time,
                avg_entry_mid, time.time(), trade_id,
            ),
        )
        self._conn.commit()

    def get_paper_trades_for_date(self, date: str) -> list[dict[str, Any]]:
        """Fetch all paper trades for games on a given date."""
        rows = self._conn.execute(
            "SELECT pt.* FROM paper_trades pt "
            "JOIN game_sessions gs ON pt.game_pk = gs.game_pk "
            "WHERE gs.date = ? ORDER BY pt.created_at",
            (date,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_paper_trades_cumulative(self) -> dict[str, Any]:
        """Get cumulative paper trading stats across all dates."""
        row = self._conn.execute(
            "SELECT COUNT(*) as total_games, "
            "SUM(CASE WHEN home_won=1 THEN 1 ELSE 0 END) as wins, "
            "SUM(CASE WHEN home_won=0 THEN 1 ELSE 0 END) as losses, "
            "SUM(total_invested) as total_invested, "
            "SUM(game_pnl) as total_pnl "
            "FROM paper_trades WHERE status='settled'",
        ).fetchone()
        return dict(row) if row else {}

    def get_paper_entry_ids_unfilled(
        self,
        trade_id: int,
    ) -> list[tuple[int, float, float]]:
        """Get unfilled paper entry ids with their mid and ts.

        Returns list of (entry_id, entry_mid, entry_ts).
        """
        rows = self._conn.execute(
            "SELECT id, entry_mid, entry_ts FROM paper_entries "
            "WHERE trade_id=? AND sim_filled=0",
            (trade_id,),
        ).fetchall()
        return [(r[0], r[1], r[2]) for r in rows]

    # ── Queries ──────────────────────────────────────────────────────

    def get_game_summary(self, game_pk: int) -> dict[str, Any] | None:
        """Fetch game session summary for post-game report."""
        row = self._conn.execute(
            "SELECT * FROM game_sessions WHERE game_pk = ?",
            (game_pk,),
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    def get_signal_count(self, game_pk: int) -> int:
        """Count ask_heavy signals for a game."""
        row = self._conn.execute(
            "SELECT COUNT(*) FROM ask_heavy_signals WHERE game_pk = ?",
            (game_pk,),
        ).fetchone()
        return int(row[0]) if row else 0

    def get_signals_for_game(self, game_pk: int) -> list[dict[str, Any]]:
        """Fetch all ask_heavy signals for a game."""
        rows = self._conn.execute(
            "SELECT * FROM ask_heavy_signals WHERE game_pk = ? ORDER BY onset_ts",
            (game_pk,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_snapshot_count(self, game_pk: int) -> int:
        """Count orderbook snapshots for a game."""
        row = self._conn.execute(
            "SELECT COUNT(*) FROM orderbook_snapshots WHERE game_pk = ?",
            (game_pk,),
        ).fetchone()
        return int(row[0]) if row else 0

    def get_total_snapshot_count(self) -> int:
        """Total snapshots across all games."""
        row = self._conn.execute(
            "SELECT COUNT(*) FROM orderbook_snapshots",
        ).fetchone()
        return int(row[0]) if row else 0

    def get_total_signal_count(self) -> int:
        """Total ask_heavy signals across all games."""
        row = self._conn.execute(
            "SELECT COUNT(*) FROM ask_heavy_signals",
        ).fetchone()
        return int(row[0]) if row else 0

    def get_active_game_count(self) -> int:
        """Count games with no game_end_ts."""
        row = self._conn.execute(
            "SELECT COUNT(*) FROM game_sessions WHERE game_end_ts IS NULL "
            "AND game_start_ts IS NOT NULL",
        ).fetchone()
        return int(row[0]) if row else 0

    def close(self) -> None:
        """Commit and close the database connection."""
        self._conn.commit()
        self._conn.close()
