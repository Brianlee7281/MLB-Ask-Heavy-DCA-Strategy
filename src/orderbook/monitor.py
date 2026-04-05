"""GameMonitor — per-game monitor for orderbook + GUMBO data.

Supports two orderbook modes:
1. **WebSocket** (primary): receives sub-second updates via callbacks from
   a shared KalshiWebSocket instance. Snapshots are rate-limited to
   config.ORDERBOOK_SNAPSHOT_INTERVAL for DB writes.
2. **REST polling** (fallback): polls Kalshi REST API at
   config.ORDERBOOK_POLL_INTERVAL when WebSocket is unavailable.

GUMBO game context is always polled via REST at config.GUMBO_CONTEXT_INTERVAL.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any

import aiohttp
import structlog

from src.config import Config
from src.orderbook.paper_trader import PaperTrader
from src.orderbook.recorder import OrderbookRecorder
from src.orderbook.signal_detector import SignalDetector
from src.orderbook.ws_client import KalshiWebSocket, OrderBook

log = structlog.get_logger()

_TERMINAL_STATES = frozenset({"Final", "Game Over", "Completed Early"})
_PREGAME_STATES = frozenset({"Scheduled", "Pre-Game", "Warmup", "Delayed Start"})


@dataclass
class GumboContext:
    """Latest game context from GUMBO polling."""

    inning: int | None = None
    half_inning: str | None = None
    outs: int | None = None
    home_score: int | None = None
    away_score: int | None = None
    runners_on: str | None = None
    current_pitcher_id: int | None = None
    status: str = "Unknown"
    home_team: str = "UNK"
    away_team: str = "UNK"
    is_final: bool = False
    home_won: bool | None = None


def _parse_gumbo_context(data: dict[str, Any]) -> GumboContext:
    """Extract game context fields from a GUMBO response."""
    ctx = GumboContext()

    # Status
    try:
        ctx.status = str(data["gameData"]["status"]["detailedState"])
    except (KeyError, TypeError):
        pass

    ctx.is_final = ctx.status in _TERMINAL_STATES

    # Teams
    try:
        teams = data["gameData"]["teams"]
        ctx.home_team = str(teams["home"]["abbreviation"])
        ctx.away_team = str(teams["away"]["abbreviation"])
    except (KeyError, TypeError):
        pass

    # Linescore (inning, score, outs)
    try:
        linescore = data["liveData"]["linescore"]
        ctx.inning = int(linescore["currentInning"])
        ctx.half_inning = str(linescore["inningHalf"]).lower()
        ctx.outs = int(linescore["outs"])
        ctx.home_score = int(linescore["teams"]["home"]["runs"])
        ctx.away_score = int(linescore["teams"]["away"]["runs"])
    except (KeyError, TypeError, ValueError):
        pass

    # Runners
    try:
        offense = linescore["offense"]
        bases = []
        if offense.get("first"):
            bases.append("1B")
        if offense.get("second"):
            bases.append("2B")
        if offense.get("third"):
            bases.append("3B")
        ctx.runners_on = ",".join(bases) if bases else "empty"
    except (KeyError, TypeError, UnboundLocalError):
        ctx.runners_on = None

    # Current pitcher
    try:
        defense = data["liveData"]["linescore"]["defense"]
        ctx.current_pitcher_id = int(defense["pitcher"]["id"])
    except (KeyError, TypeError, ValueError):
        pass

    # Home won (only meaningful if final)
    if ctx.is_final and ctx.home_score is not None and ctx.away_score is not None:
        ctx.home_won = ctx.home_score > ctx.away_score

    return ctx


def _parse_orderbook(
    data: dict[str, Any],
) -> tuple[float, float, int, int, list[list[float]], list[list[float]]]:
    """Parse Kalshi REST orderbook response into structured data.

    Handles both the legacy cents-integer format and the current
    ``orderbook_fp`` dollar-string format returned by the Kalshi v2 API.

    Returns:
        (best_bid, best_ask, bid_depth, ask_depth, bid_levels, ask_levels)
        Prices are in 0.00-1.00 scale (dollars).
    """
    # Prefer the new orderbook_fp format (dollar strings), fall back to legacy
    orderbook = data.get("orderbook_fp") or data.get("orderbook", data)

    bids_raw = orderbook.get("yes_dollars", orderbook.get("yes", orderbook.get("bids", [])))
    asks_raw = orderbook.get("no_dollars", orderbook.get("no", orderbook.get("asks", [])))

    # YES bids: highest price = best bid (Kalshi returns ascending order)
    bid_levels: list[list[float]] = []
    for lv in bids_raw:
        price = float(lv[0])
        qty = float(lv[1])
        bid_levels.append([price, qty])
    bid_levels.sort(key=lambda x: x[0], reverse=True)  # best (highest) first

    # NO bids -> YES asks: YES ask = 1.0 - NO price
    # Highest NO bid -> lowest YES ask = best ask
    ask_levels: list[list[float]] = []
    for lv in asks_raw:
        no_price = float(lv[0])
        qty = float(lv[1])
        ask_levels.append([round(1.0 - no_price, 4), qty])
    ask_levels.sort(key=lambda x: x[0])  # best (lowest) first

    best_bid = bid_levels[0][0] if bid_levels else 0.0
    best_ask = ask_levels[0][0] if ask_levels else 1.0
    bid_depth = int(bid_levels[0][1]) if bid_levels else 0
    ask_depth = int(ask_levels[0][1]) if ask_levels else 0

    return best_bid, best_ask, bid_depth, ask_depth, bid_levels, ask_levels


class GameMonitor:
    """Monitors a single game's orderbook and game state.

    Two modes of operation:
    - **WebSocket mode** (ws_client provided): receives orderbook updates via
      callback from a shared KalshiWebSocket. GUMBO polling runs independently.
    - **REST mode** (no ws_client): falls back to polling Kalshi REST API.

    The monitor can switch from WS to REST mid-session if the WebSocket
    disconnects, and back to WS when it reconnects.
    """

    def __init__(
        self,
        game_pk: int,
        kalshi_ticker: str,
        recorder: OrderbookRecorder,
        config: Config | None = None,
        ws_client: KalshiWebSocket | None = None,
        paper_trader: PaperTrader | None = None,
    ) -> None:
        self.game_pk = game_pk
        self.ticker = kalshi_ticker
        self._recorder = recorder
        self._config = config or Config()
        self._detector = SignalDetector(game_pk, recorder, config)
        self._gumbo_ctx = GumboContext()
        self._ws_client = ws_client
        self._paper_trader = paper_trader
        self._running = False
        self._snapshot_count = 0
        self._trade_count = 0
        self._error_count = 0
        self._last_mid: float | None = None
        self._last_depth_ratio: float | None = None
        self._last_snapshot_write: float = 0.0  # rate limit DB writes
        self._game_ended = asyncio.Event()
        self._using_ws = False
        self._rest_fallback_active = False

    @property
    def context(self) -> GumboContext:
        return self._gumbo_ctx

    @property
    def snapshot_count(self) -> int:
        return self._snapshot_count

    @property
    def trade_count(self) -> int:
        return self._trade_count

    @property
    def is_ask_heavy(self) -> bool:
        return self._detector.is_ask_heavy

    @property
    def last_mid(self) -> float | None:
        return self._last_mid

    @property
    def using_websocket(self) -> bool:
        return self._using_ws and not self._rest_fallback_active

    # ── WebSocket callbacks ──────────────────────────────────────────

    def handle_orderbook_update(self, ticker: str, book: OrderBook) -> None:
        """Callback from KalshiWebSocket on orderbook snapshot/delta.

        This is called synchronously from the WS dispatch loop. Keep it fast.
        """
        if ticker != self.ticker or not self._running:
            return
        if book.is_empty:
            return

        now = time.time()
        mid = book.mid
        spread = book.spread
        depth_ratio = book.depth_ratio
        bid_depth = book.bid_depth
        ask_depth = book.ask_depth

        self._last_mid = mid
        self._last_depth_ratio = depth_ratio

        # Always run signal detection (no rate limit)
        ctx = self._gumbo_ctx
        self._detector.update(
            timestamp=now,
            mid=mid,
            spread=spread,
            depth_ratio=depth_ratio,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
            inning=ctx.inning,
            home_score=ctx.home_score,
            away_score=ctx.away_score,
        )

        # Rate-limit DB snapshot writes
        elapsed = now - self._last_snapshot_write
        snap_interval = self._config.ORDERBOOK_SNAPSHOT_INTERVAL
        was_ask_heavy = self._detector.is_ask_heavy

        # Write if: interval elapsed, OR ask_heavy state just changed
        if elapsed >= snap_interval or was_ask_heavy != self._detector.is_ask_heavy:
            self._write_snapshot(
                now, book.best_bid, book.best_ask, bid_depth, ask_depth,
                book.bid_levels, book.ask_levels,
            )

        # Paper trading: DCA logic
        if self._paper_trader is not None:
            self._paper_trader.on_orderbook_update(book, {
                "inning": ctx.inning,
                "home_score": ctx.home_score,
                "away_score": ctx.away_score,
            })

    def handle_trade(self, trade: dict[str, Any]) -> None:
        """Callback from KalshiWebSocket on trade event."""
        ticker = trade.get("market_ticker", "")
        if ticker != self.ticker or not self._running:
            return

        # Determine YES price for recording
        yes_price = trade.get("yes_price", 0.0)
        taker_side = trade.get("taker_side", "")
        count = trade.get("count", 0)
        ts = float(trade.get("ts", time.time()))

        book = self._ws_client.get_book(self.ticker) if self._ws_client else None

        self._recorder.record_trade(
            game_pk=self.game_pk,
            timestamp=ts,
            price=yes_price,
            side=taker_side,
            quantity=count,
            best_bid_at_trade=book.best_bid if book else None,
            best_ask_at_trade=book.best_ask if book else None,
            depth_ratio_at_trade=book.depth_ratio if book else None,
            ask_heavy_at_trade=self._detector.is_ask_heavy,
        )
        self._trade_count += 1

        # Paper trading: check fills
        if self._paper_trader is not None:
            self._paper_trader.on_trade_observed(trade)

    # ── Main run loop ────────────────────────────────────────────────

    async def run(self) -> None:
        """Main entry point — run GUMBO polling + orderbook monitoring.

        If ws_client is provided and connected, uses WS mode. Otherwise
        falls back to REST polling. Can switch between modes dynamically.
        """
        self._running = True

        # Determine initial mode
        if self._ws_client and self._ws_client.is_connected:
            self._using_ws = True
            method = "websocket"
        else:
            self._using_ws = False
            method = "rest"

        log.info(
            "monitoring_started",
            game_pk=self.game_pk,
            ticker=self.ticker,
            method=method,
        )

        async with aiohttp.ClientSession() as session:
            gumbo_task = asyncio.create_task(self._gumbo_loop(session))

            if self._using_ws:
                # WS mode: orderbook updates arrive via callbacks.
                # We just need a fallback watcher + game end waiter.
                fallback_task = asyncio.create_task(
                    self._ws_fallback_watcher(session),
                )
                await self._game_ended.wait()
                fallback_task.cancel()
                try:
                    await fallback_task
                except asyncio.CancelledError:
                    pass
            else:
                # Pure REST mode
                orderbook_task = asyncio.create_task(self._orderbook_loop(session))
                done, pending = await asyncio.wait(
                    [gumbo_task, orderbook_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                for task in done:
                    exc = task.exception()
                    if exc:
                        log.error(
                            "monitor_task_error",
                            game_pk=self.game_pk,
                            error=str(exc),
                        )

            gumbo_task.cancel()
            try:
                await gumbo_task
            except asyncio.CancelledError:
                pass

        self._running = False

    async def _ws_fallback_watcher(self, session: aiohttp.ClientSession) -> None:
        """Watch WebSocket health and fall back to REST if disconnected.

        In WS mode, this loop monitors the connection and activates REST
        polling when the WS is down. When WS recovers, REST stops.
        """
        poll_interval = self._config.ORDERBOOK_POLL_INTERVAL
        rest_task: asyncio.Task[None] | None = None

        while self._running and not self._game_ended.is_set():
            ws_ok = self._ws_client is not None and self._ws_client.is_connected

            if not ws_ok and not self._rest_fallback_active:
                # WS down — start REST fallback
                log.warning(
                    "ws_fallback_activated",
                    game_pk=self.game_pk,
                    ticker=self.ticker,
                )
                self._rest_fallback_active = True
                rest_task = asyncio.create_task(self._orderbook_loop(session))

            elif ws_ok and self._rest_fallback_active:
                # WS recovered — stop REST fallback
                log.info(
                    "ws_fallback_deactivated",
                    game_pk=self.game_pk,
                    ticker=self.ticker,
                )
                self._rest_fallback_active = False
                if rest_task and not rest_task.done():
                    rest_task.cancel()
                    try:
                        await rest_task
                    except asyncio.CancelledError:
                        pass
                    rest_task = None

            await asyncio.sleep(poll_interval)

        # Cleanup
        if rest_task and not rest_task.done():
            rest_task.cancel()
            try:
                await rest_task
            except asyncio.CancelledError:
                pass

    # ── GUMBO polling ────────────────────────────────────────────────

    async def _gumbo_loop(self, session: aiohttp.ClientSession) -> None:
        """Poll GUMBO for game context at GUMBO_CONTEXT_INTERVAL."""
        base_url = self._config.MLB_API_BASE_URL
        url = f"{base_url}/game/{self.game_pk}/feed/live"
        interval = self._config.GUMBO_CONTEXT_INTERVAL
        backoff = self._config.ORDERBOOK_INITIAL_BACKOFF
        game_started = False

        while self._running:
            try:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    data: dict[str, Any] = await resp.json()

                self._gumbo_ctx = _parse_gumbo_context(data)
                backoff = self._config.ORDERBOOK_INITIAL_BACKOFF

                # Mark game start on first non-pregame status
                if not game_started and self._gumbo_ctx.status not in _PREGAME_STATES:
                    game_started = True
                    self._recorder.update_game_start(self.game_pk)

                # Check for game end
                if self._gumbo_ctx.is_final:
                    log.info(
                        "game_ended_gumbo",
                        game_pk=self.game_pk,
                        status=self._gumbo_ctx.status,
                        home_score=self._gumbo_ctx.home_score,
                        away_score=self._gumbo_ctx.away_score,
                    )
                    self._handle_game_end()
                    return

                # Slower polling for pregame
                wait = 60.0 if self._gumbo_ctx.status in _PREGAME_STATES else interval

            except aiohttp.ClientError as e:
                log.warning(
                    "gumbo_poll_error",
                    game_pk=self.game_pk,
                    error=str(e),
                )
                self._error_count += 1
                self._recorder.increment_error(self.game_pk)
                wait = min(backoff, self._config.ORDERBOOK_MAX_BACKOFF)
                backoff = min(backoff * 2, self._config.ORDERBOOK_MAX_BACKOFF)

            await asyncio.sleep(wait)

    # ── REST orderbook polling (fallback) ────────────────────────────

    async def _orderbook_loop(self, session: aiohttp.ClientSession) -> None:
        """Poll Kalshi orderbook at ORDERBOOK_POLL_INTERVAL (REST fallback)."""
        base_url = self._config.KALSHI_API_URL
        path = f"/markets/{self.ticker}/orderbook"
        url = f"{base_url}{path}"
        interval = self._config.ORDERBOOK_POLL_INTERVAL
        backoff = self._config.ORDERBOOK_INITIAL_BACKOFF

        from src.trading.kalshi_client import KalshiClient

        client = KalshiClient(self._config)

        while self._running and not self._game_ended.is_set():
            # If WS recovered and this is a fallback task, exit
            if self._using_ws and not self._rest_fallback_active:
                return

            try:
                headers = client._sign_request("GET", path)
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 429:
                        retry_after = float(
                            resp.headers.get("Retry-After", interval * 2),
                        )
                        log.warning(
                            "rate_limited",
                            endpoint="orderbook",
                            retry_after=retry_after,
                        )
                        await asyncio.sleep(retry_after)
                        continue

                    if resp.status in (401, 403):
                        body = await resp.text()
                        log.warning(
                            "kalshi_auth_failed",
                            status=resp.status,
                            body=body[:200],
                        )
                        self._error_count += 1
                        self._recorder.increment_error(self.game_pk)
                        await asyncio.sleep(self._config.ORDERBOOK_MAX_BACKOFF)
                        continue

                    resp.raise_for_status()
                    data = await resp.json()

                backoff = self._config.ORDERBOOK_INITIAL_BACKOFF
                now = time.time()

                best_bid, best_ask, bid_depth, ask_depth, bid_levels, ask_levels = (
                    _parse_orderbook(data)
                )

                # Skip empty orderbooks
                if best_bid == 0.0 and best_ask == 1.0:
                    await asyncio.sleep(interval)
                    continue

                mid = (best_bid + best_ask) / 2.0
                spread = best_ask - best_bid
                total_depth = bid_depth + ask_depth
                depth_ratio = bid_depth / total_depth if total_depth > 0 else 0.5

                self._last_mid = mid
                self._last_depth_ratio = depth_ratio

                ctx = self._gumbo_ctx

                # Write snapshot (no rate limiting in REST mode — already slow)
                self._write_snapshot(
                    now, best_bid, best_ask, bid_depth, ask_depth,
                    bid_levels, ask_levels,
                )

                # Update signal detector
                self._detector.update(
                    timestamp=now,
                    mid=mid,
                    spread=spread,
                    depth_ratio=depth_ratio,
                    bid_depth=bid_depth,
                    ask_depth=ask_depth,
                    inning=ctx.inning,
                    home_score=ctx.home_score,
                    away_score=ctx.away_score,
                )

                # Paper trading: DCA logic (REST mode — build a temp OrderBook)
                if self._paper_trader is not None:
                    from src.orderbook.ws_client import OrderBook as _OB

                    rest_book = _OB(ticker=self.ticker)
                    rest_book.yes_bids = bid_levels
                    rest_book.no_bids = [
                        [round(1.0 - lv[0], 4), lv[1]] for lv in ask_levels
                    ]
                    rest_book.no_bids.sort(key=lambda x: x[0], reverse=True)
                    self._paper_trader.on_orderbook_update(rest_book, {
                        "inning": ctx.inning,
                        "home_score": ctx.home_score,
                        "away_score": ctx.away_score,
                    })

            except asyncio.CancelledError:
                raise
            except aiohttp.ClientError as e:
                log.warning(
                    "orderbook_poll_error",
                    game_pk=self.game_pk,
                    ticker=self.ticker,
                    error=str(e),
                )
                self._error_count += 1
                self._recorder.increment_error(self.game_pk)
                await asyncio.sleep(
                    min(backoff, self._config.ORDERBOOK_MAX_BACKOFF),
                )
                backoff = min(backoff * 2, self._config.ORDERBOOK_MAX_BACKOFF)
                continue

            await asyncio.sleep(interval)

    # ── Shared helpers ───────────────────────────────────────────────

    def _write_snapshot(
        self,
        now: float,
        best_bid: float,
        best_ask: float,
        bid_depth: int,
        ask_depth: int,
        bid_levels: list[list[float]],
        ask_levels: list[list[float]],
    ) -> None:
        """Write orderbook snapshot to DB and update counters."""
        ctx = self._gumbo_ctx
        self._recorder.record_snapshot(
            game_pk=self.game_pk,
            timestamp=now,
            best_bid=best_bid,
            best_ask=best_ask,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
            bid_levels=bid_levels,
            ask_levels=ask_levels,
            inning=ctx.inning,
            half_inning=ctx.half_inning,
            outs=ctx.outs,
            home_score=ctx.home_score,
            away_score=ctx.away_score,
            runners_on=ctx.runners_on,
            current_pitcher_id=ctx.current_pitcher_id,
        )
        self._snapshot_count += 1
        self._last_snapshot_write = now

    def _handle_game_end(self) -> None:
        """Process game end: close signals, update session."""
        now = time.time()
        self._detector.close_all_signals(now)
        self._running = False
        self._game_ended.set()

        if self._gumbo_ctx.home_won is not None:
            self._recorder.update_game_end(
                self.game_pk, self._gumbo_ctx.home_won,
            )
            log.info(
                "game_settled",
                game_pk=self.game_pk,
                home_won=self._gumbo_ctx.home_won,
                total_snapshots=self._snapshot_count,
                total_trades=self._trade_count,
                total_signals=self._detector.active_signal_count,
            )
        else:
            log.warning(
                "game_ended_no_winner",
                game_pk=self.game_pk,
                status=self._gumbo_ctx.status,
            )

        # Paper trading: settle
        if self._paper_trader is not None:
            self._paper_trader.settle(self._gumbo_ctx.home_won)

    def get_status_line(self) -> str:
        """Build a one-line status string for console summary."""
        ctx = self._gumbo_ctx
        teams = f"{ctx.away_team} @ {ctx.home_team}"

        if ctx.inning is not None and ctx.home_score is not None:
            half = "T" if ctx.half_inning == "top" else "B"
            game_info = f"({half}{ctx.inning}, {ctx.away_score}-{ctx.home_score})"
        elif ctx.status in _PREGAME_STATES:
            game_info = "(pregame)"
        else:
            game_info = f"({ctx.status})"

        mid_str = f"mid={self._last_mid:.2f}" if self._last_mid else "mid=--"
        dr_str = (
            f"depth_ratio={self._last_depth_ratio:.2f}"
            if self._last_depth_ratio is not None
            else "depth_ratio=--"
        )
        mode = "WS" if self.using_websocket else "REST"
        ah_flag = "  ASK_HEAVY" if self._detector.is_ask_heavy else ""

        dca_str = ""
        if self._paper_trader is not None and self._paper_trader.n_entries > 0:
            pt = self._paper_trader
            dca_str = f"  DCA: {pt.n_entries} entries, ${pt.total_invested:.0f}"

        return f"  {teams} {game_info}  {mid_str}  {dr_str}  [{mode}]{ah_flag}{dca_str}"
