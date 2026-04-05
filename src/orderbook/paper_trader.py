"""Paper trading — DCA ask_heavy strategy simulation.

Places virtual $1 YES limit orders at mid every second while ask_heavy
is active. Tracks simulated fills against observed trades and computes
PnL at settlement. No real orders are placed.
"""

from __future__ import annotations

import time
from typing import Any

import structlog

from src.config import Config
from src.orderbook.recorder import OrderbookRecorder
from src.orderbook.ws_client import OrderBook

log = structlog.get_logger()


class PaperTrader:
    """Manages DCA paper trading for a single game.

    Lifecycle:
    1. Created when game monitoring starts (if --paper-trade is active).
    2. ``on_orderbook_update`` called on every orderbook update — decides
       whether to add a DCA entry based on ask_heavy state + interval.
    3. ``on_trade_observed`` called on every trade — checks if pending
       entries would have filled.
    4. ``settle`` called when game ends — computes final PnL.
    """

    def __init__(
        self,
        game_pk: int,
        recorder: OrderbookRecorder,
        config: Config,
    ) -> None:
        self._game_pk = game_pk
        self._recorder = recorder
        self._config = config
        self._game_cap = config.PAPER_TRADE_GAME_CAP
        self._entry_size = config.PAPER_TRADE_ENTRY_SIZE
        self._interval = config.PAPER_TRADE_ENTRY_INTERVAL
        self._threshold = config.ASK_HEAVY_THRESHOLD
        self._fill_tolerance = config.SIM_FILL_TOLERANCE

        self._total_invested: float = 0.0
        self._entries: list[dict[str, Any]] = []
        self._trade_id: int | None = None  # paper_trades row id
        self._last_entry_ts: float = 0.0
        self._is_ask_heavy: bool = False

    @property
    def trade_id(self) -> int | None:
        return self._trade_id

    @property
    def n_entries(self) -> int:
        return len(self._entries)

    @property
    def total_invested(self) -> float:
        return self._total_invested

    @property
    def is_active(self) -> bool:
        return self._is_ask_heavy

    def on_orderbook_update(
        self,
        book: OrderBook,
        context: dict[str, Any],
    ) -> None:
        """Called on every orderbook update. Decides whether to add a DCA entry.

        Args:
            book: Current orderbook state.
            context: Game context dict with keys: inning, home_score, away_score.
        """
        if book.is_empty:
            return

        now_ts = time.time()
        depth_ratio = book.depth_ratio
        is_ask_heavy = depth_ratio < self._threshold

        # State transition logging
        if is_ask_heavy and not self._is_ask_heavy:
            log.info(
                "paper_dca_start",
                game_pk=self._game_pk,
                depth_ratio=round(depth_ratio, 3),
                mid=round(book.mid, 3),
            )
        elif not is_ask_heavy and self._is_ask_heavy:
            log.info(
                "paper_dca_pause",
                game_pk=self._game_pk,
                entries_so_far=len(self._entries),
            )

        self._is_ask_heavy = is_ask_heavy

        if not is_ask_heavy:
            return

        # Check cap
        if self._total_invested + self._entry_size > self._game_cap:
            return

        # Check interval
        if now_ts - self._last_entry_ts < self._interval:
            return

        # Record entry
        entry: dict[str, Any] = {
            "ts": now_ts,
            "mid": book.mid,
            "spread": book.spread,
            "depth_ratio": depth_ratio,
            "amount": self._entry_size,
            "inning": context.get("inning"),
            "home_score": context.get("home_score"),
            "away_score": context.get("away_score"),
            "sim_filled": False,
            "entry_row_id": None,
        }

        # Create paper_trades row on first entry
        if self._trade_id is None:
            self._trade_id = self._recorder.create_paper_trade(
                self._game_pk, self._game_cap,
            )

        # Write paper_entries row
        self._recorder.record_paper_entry(self._trade_id, self._game_pk, entry)

        # Get the row id of the entry we just inserted
        row = self._recorder._conn.execute(
            "SELECT last_insert_rowid()",
        ).fetchone()
        entry["entry_row_id"] = row[0] if row else None

        self._entries.append(entry)
        self._total_invested += self._entry_size
        self._last_entry_ts = now_ts

        # Update summary periodically (every 10 entries)
        if len(self._entries) % 10 == 0:
            self._update_trade_summary()

    def on_trade_observed(self, trade: dict[str, Any]) -> None:
        """Called when a trade is observed. Check if pending entries would fill.

        A fill occurs if the trade's YES price <= entry mid + fill_tolerance,
        and the trade happened after the entry.
        """
        yes_price = trade.get("yes_price", 0.0)
        trade_ts = float(trade.get("ts", 0))

        for entry in self._entries:
            if entry.get("sim_filled"):
                continue
            # Fill if trade price <= entry mid + tolerance
            if yes_price <= entry["mid"] + self._fill_tolerance:
                if trade_ts >= entry["ts"]:
                    entry["sim_filled"] = True
                    entry["sim_fill_price"] = yes_price
                    entry["sim_fill_ts"] = trade_ts
                    entry["sim_fill_seconds"] = trade_ts - entry["ts"]

                    # Update DB
                    entry_row_id = entry.get("entry_row_id")
                    if entry_row_id is not None:
                        self._recorder.update_paper_entry_fill(
                            entry_row_id,
                            fill_price=yes_price,
                            fill_ts=trade_ts,
                            fill_seconds=trade_ts - entry["ts"],
                        )

    def settle(self, home_won: bool | None) -> None:
        """Called when game ends. Compute final PnL.

        Args:
            home_won: True if home team won, False if away, None if unknown.
        """
        if not self._entries or self._trade_id is None:
            return

        # Final summary update
        self._update_trade_summary()

        if home_won is None:
            # Can't settle — mark as error
            self._recorder._conn.execute(
                "UPDATE paper_trades SET status='error' WHERE id=?",
                (self._trade_id,),
            )
            self._recorder._conn.commit()
            log.warning(
                "paper_trade_no_settlement",
                game_pk=self._game_pk,
                n_entries=len(self._entries),
            )
            return

        mids = [e["mid"] for e in self._entries]
        avg_mid = sum(mids) / len(mids)
        settlement = 1.0 if home_won else 0.0
        pnl_per_dollar = settlement - avg_mid
        game_pnl = pnl_per_dollar * self._total_invested

        n_filled = sum(1 for e in self._entries if e.get("sim_filled"))
        fill_times = [
            e["sim_fill_seconds"]
            for e in self._entries
            if e.get("sim_fill_seconds") is not None
        ]
        fill_rate = n_filled / len(self._entries) if self._entries else 0.0
        avg_fill_time = sum(fill_times) / len(fill_times) if fill_times else None

        self._recorder.settle_paper_trade(
            self._trade_id,
            home_won=home_won,
            avg_entry_mid=avg_mid,
            pnl_per_dollar=pnl_per_dollar,
            game_pnl=game_pnl,
            n_sim_filled=n_filled,
            fill_rate=fill_rate,
            avg_fill_time=avg_fill_time,
        )

        result = "WIN" if home_won else "LOSS"
        print(  # noqa: T201
            f"\n  PAPER TRADE settled: game_pk={self._game_pk}"
            f" | entries={len(self._entries)}"
            f" | invested=${self._total_invested:.0f}"
            f" | avg_mid={avg_mid:.4f}"
            f" | {result}"
            f" | PnL=${game_pnl:+.2f}"
            f" | fill_rate={n_filled}/{len(self._entries)}",
            flush=True,
        )

    def _update_trade_summary(self) -> None:
        """Update the paper_trades summary row with current stats."""
        if not self._entries or self._trade_id is None:
            return

        mids = [e["mid"] for e in self._entries]
        spreads = [e["spread"] for e in self._entries if e.get("spread") is not None]
        drs = [
            e["depth_ratio"] for e in self._entries
            if e.get("depth_ratio") is not None
        ]
        innings = [
            e["inning"] for e in self._entries
            if e.get("inning") is not None
        ]

        self._recorder.update_paper_trade_summary(
            trade_id=self._trade_id,
            n_entries=len(self._entries),
            total_invested=self._total_invested,
            avg_entry_mid=sum(mids) / len(mids),
            first_entry_mid=self._entries[0]["mid"],
            first_entry_ts=self._entries[0]["ts"],
            last_entry_mid=self._entries[-1]["mid"],
            last_entry_ts=self._entries[-1]["ts"],
            min_entry_mid=min(mids),
            max_entry_mid=max(mids),
            avg_depth_ratio=sum(drs) / len(drs) if drs else None,
            avg_spread=sum(spreads) / len(spreads) if spreads else None,
            avg_inning=sum(innings) / len(innings) if innings else None,
        )
