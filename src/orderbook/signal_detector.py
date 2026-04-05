"""Ask-heavy signal detection — onset/offset transitions and price tracking.

Tracks the state machine: non-ask_heavy ↔ ask_heavy, managing signal
lifecycle including simulated fill detection and price evolution updates.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

import structlog

from src.config import Config
from src.orderbook.recorder import OrderbookRecorder

log = structlog.get_logger()

# Price evolution checkpoints: (seconds_after_onset, column_name)
_PRICE_CHECKPOINTS = [
    (30, "mid_after_30s"),
    (60, "mid_after_60s"),
    (120, "mid_after_120s"),
    (300, "mid_after_300s"),
    (600, "mid_after_600s"),
]

# Simulated fill checkpoints: (seconds_after_onset, column_name)
_FILL_CHECKPOINTS = [
    (30, "sim_fill_30s"),
    (60, "sim_fill_60s"),
    (300, "sim_fill_300s"),
]


@dataclass
class ActiveSignal:
    """Tracks an active ask_heavy signal for price evolution and fill sim."""

    signal_id: int
    onset_ts: float
    mid_at_onset: float
    # Track which checkpoints have been filled
    price_checkpoints_done: set[str] = field(default_factory=set)
    fill_checkpoints_done: set[str] = field(default_factory=set)
    first_fill_recorded: bool = False


class SignalDetector:
    """Detects ask_heavy onset/offset transitions and tracks active signals.

    One instance per game. Call update() with each orderbook snapshot.
    """

    def __init__(
        self,
        game_pk: int,
        recorder: OrderbookRecorder,
        config: Config | None = None,
    ) -> None:
        self.game_pk = game_pk
        self._recorder = recorder
        self._config = config or Config()
        self._threshold = self._config.ASK_HEAVY_THRESHOLD
        self._fill_tolerance = self._config.SIM_FILL_TOLERANCE
        self._is_ask_heavy = False
        self._active_signals: list[ActiveSignal] = []

    @property
    def is_ask_heavy(self) -> bool:
        return self._is_ask_heavy

    @property
    def active_signal_count(self) -> int:
        return len(self._active_signals)

    def update(
        self,
        timestamp: float,
        mid: float,
        spread: float,
        depth_ratio: float,
        bid_depth: int,
        ask_depth: int,
        inning: int | None,
        home_score: int | None,
        away_score: int | None,
    ) -> None:
        """Process one orderbook snapshot for signal detection.

        Args:
            timestamp: Unix timestamp of the snapshot.
            mid: Midpoint price.
            spread: Bid-ask spread.
            depth_ratio: bid_depth / (bid_depth + ask_depth).
            bid_depth: Contracts at best bid.
            ask_depth: Contracts at best ask.
            inning: Current inning (from GUMBO context).
            home_score: Home team score.
            away_score: Away team score.
        """
        now_ask_heavy = depth_ratio < self._threshold

        # Transition: non-ask_heavy → ask_heavy (onset)
        if now_ask_heavy and not self._is_ask_heavy:
            home_favored = mid > 0.50
            score_diff = (
                (home_score - away_score)
                if home_score is not None and away_score is not None
                else None
            )
            signal_id = self._recorder.insert_ask_heavy_signal(
                game_pk=self.game_pk,
                onset_ts=timestamp,
                mid_at_onset=mid,
                spread_at_onset=spread,
                depth_ratio_at_onset=depth_ratio,
                bid_depth_at_onset=bid_depth,
                ask_depth_at_onset=ask_depth,
                home_favored=home_favored,
                inning_at_onset=inning,
                score_diff_at_onset=score_diff,
            )
            self._active_signals.append(
                ActiveSignal(
                    signal_id=signal_id,
                    onset_ts=timestamp,
                    mid_at_onset=mid,
                ),
            )
            log.info(
                "ask_heavy_onset",
                game_pk=self.game_pk,
                signal_id=signal_id,
                depth_ratio=round(depth_ratio, 3),
                mid=round(mid, 3),
                inning=inning,
            )

        # Transition: ask_heavy → non-ask_heavy (offset)
        if not now_ask_heavy and self._is_ask_heavy:
            for sig in self._active_signals:
                self._recorder.update_ask_heavy_offset(sig.signal_id, timestamp)
                duration = timestamp - sig.onset_ts
                log.info(
                    "ask_heavy_offset",
                    game_pk=self.game_pk,
                    signal_id=sig.signal_id,
                    duration_seconds=round(duration, 1),
                )
            # Keep signals for continued price/fill tracking (cleared on game end)

        self._is_ask_heavy = now_ask_heavy

        # Update price evolution and simulated fills for all active signals
        self._update_active_signals(timestamp, mid)

    def _update_active_signals(self, timestamp: float, mid: float) -> None:
        """Update price checkpoints and simulated fill detection."""
        for sig in self._active_signals:
            elapsed = timestamp - sig.onset_ts

            # Price evolution checkpoints
            for seconds, column in _PRICE_CHECKPOINTS:
                if column not in sig.price_checkpoints_done and elapsed >= seconds:
                    self._recorder.update_signal_mid_after(
                        sig.signal_id, column, mid,
                    )
                    sig.price_checkpoints_done.add(column)

            # Simulated fill detection:
            # "If I had placed a YES limit at mid_at_onset, would it have filled?"
            # A fill occurs if the current mid drops to or below mid_at_onset + tolerance
            # (meaning someone was willing to sell at our price)
            if mid <= sig.mid_at_onset + self._fill_tolerance:
                fill_time = elapsed

                for seconds, column in _FILL_CHECKPOINTS:
                    if (
                        column not in sig.fill_checkpoints_done
                        and elapsed <= seconds
                    ):
                        self._recorder.update_sim_fill(
                            sig.signal_id,
                            column,
                            fill_price=mid,
                            fill_time=fill_time,
                        )
                        sig.fill_checkpoints_done.add(column)

                        if not sig.first_fill_recorded:
                            sig.first_fill_recorded = True
                            log.info(
                                "sim_fill_detected",
                                game_pk=self.game_pk,
                                signal_id=sig.signal_id,
                                fill_price=round(mid, 4),
                                time_to_fill=round(fill_time, 1),
                            )

    def close_all_signals(self, timestamp: float) -> None:
        """Close any open signals at game end (offset_ts = game end)."""
        for sig in self._active_signals:
            # Only close if not already closed (offset_ts still NULL)
            self._recorder.update_ask_heavy_offset(sig.signal_id, timestamp)
        self._active_signals.clear()
        self._is_ask_heavy = False
