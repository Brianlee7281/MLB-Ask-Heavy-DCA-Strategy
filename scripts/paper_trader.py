"""Paper Trading Bot — DCA ask_heavy strategy on Kalshi MLB markets.

Monitors real-time orderbooks (WebSocket primary, REST fallback), detects
ask_heavy conditions (depth_ratio < 0.4), and simulates DCA entries at 1
contract per tick. Tracks results in a lightweight SQLite table.

Usage:
    python scripts/paper_trader.py --date 2026-04-06
    python scripts/paper_trader.py --date 2026-04-06 --game-cap 1000
    python scripts/paper_trader.py --game-pk 831547
"""

from __future__ import annotations

import argparse
import asyncio
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
import structlog

from src.config import Config
from src.data_ingestion.schedule import fetch_todays_games
from src.orderbook.ws_client import KalshiWebSocket, OrderBook
from src.trading.kalshi_client import KalshiClient

log = structlog.get_logger()

ET = ZoneInfo("America/New_York")

# MLB team abbreviation aliases: Kalshi ticker -> GUMBO abbreviation
_TEAM_ALIASES: dict[str, str] = {
    "ARI": "AZ", "AZ": "AZ",
    "ATL": "ATL",
    "BAL": "BAL",
    "BOS": "BOS",
    "CHC": "CHC", "CUB": "CHC",
    "CWS": "CWS", "CHW": "CWS",
    "CIN": "CIN",
    "CLE": "CLE",
    "COL": "COL",
    "DET": "DET",
    "HOU": "HOU",
    "KC": "KC", "KCR": "KC",
    "LAA": "LAA",
    "LAD": "LAD",
    "MIA": "MIA",
    "MIL": "MIL",
    "MIN": "MIN",
    "NYM": "NYM",
    "NYY": "NYY",
    "OAK": "OAK", "ATH": "OAK",
    "PHI": "PHI",
    "PIT": "PIT",
    "SD": "SD", "SDP": "SD",
    "SF": "SF", "SFG": "SF",
    "SEA": "SEA",
    "STL": "STL",
    "TB": "TB", "TBR": "TB",
    "TEX": "TEX",
    "TOR": "TOR",
    "WSH": "WSH", "WAS": "WSH",
}


def _normalize_abbr(abbr: str) -> str:
    """Normalize a team abbreviation for matching."""
    return _TEAM_ALIASES.get(abbr.upper(), abbr.upper())


# ── Lightweight SQLite store ────────────────────────────────────────


class PaperTradeStore:
    """Minimal SQLite store for paper trade results."""

    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS paper_trades (
                game_pk        INTEGER PRIMARY KEY,
                date           TEXT NOT NULL,
                home_team      TEXT NOT NULL,
                away_team      TEXT NOT NULL,
                ticker         TEXT NOT NULL,
                n_entries      INTEGER NOT NULL DEFAULT 0,
                total_invested INTEGER NOT NULL DEFAULT 0,
                avg_mid        REAL,
                home_won       INTEGER,
                pnl_per_dollar REAL,
                game_pnl       REAL,
                settled_at     REAL
            );
        """)
        self._conn.commit()

    def upsert_game(
        self,
        game_pk: int,
        date: str,
        home_team: str,
        away_team: str,
        ticker: str,
    ) -> None:
        """Register a game (idempotent)."""
        self._conn.execute(
            "INSERT OR IGNORE INTO paper_trades "
            "(game_pk, date, home_team, away_team, ticker) "
            "VALUES (?, ?, ?, ?, ?)",
            (game_pk, date, home_team, away_team, ticker),
        )
        self._conn.commit()

    def update_entries(
        self,
        game_pk: int,
        n_entries: int,
        total_invested: int,
        avg_mid: float,
    ) -> None:
        """Update running DCA entry totals."""
        self._conn.execute(
            "UPDATE paper_trades SET n_entries=?, total_invested=?, avg_mid=? "
            "WHERE game_pk=?",
            (n_entries, total_invested, avg_mid, game_pk),
        )
        self._conn.commit()

    def settle(
        self,
        game_pk: int,
        home_won: bool,
        pnl_per_dollar: float,
        game_pnl: float,
    ) -> None:
        """Write settlement outcome."""
        self._conn.execute(
            "UPDATE paper_trades SET home_won=?, pnl_per_dollar=?, "
            "game_pnl=?, settled_at=? WHERE game_pk=?",
            (int(home_won), pnl_per_dollar, game_pnl, time.time(), game_pk),
        )
        self._conn.commit()

    def get_trades_for_date(self, date: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM paper_trades WHERE date=? ORDER BY game_pk",
            (date,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_cumulative(self) -> dict[str, Any]:
        row = self._conn.execute(
            "SELECT COUNT(*) as total_games, "
            "SUM(CASE WHEN home_won=1 THEN 1 ELSE 0 END) as wins, "
            "SUM(CASE WHEN home_won=0 THEN 1 ELSE 0 END) as losses, "
            "SUM(total_invested) as total_contracts, "
            "SUM(game_pnl) as total_pnl "
            "FROM paper_trades WHERE settled_at IS NOT NULL",
        ).fetchone()
        return dict(row) if row else {}

    def close(self) -> None:
        self._conn.commit()
        self._conn.close()


# ── Game context from GUMBO ─────────────────────────────────────────

_TERMINAL_STATES = frozenset({"Final", "Game Over", "Completed Early"})
_PREGAME_STATES = frozenset({"Scheduled", "Pre-Game", "Warmup", "Delayed Start"})


class _GameContext:
    """Lightweight game state from GUMBO polling."""

    __slots__ = (
        "inning", "half_inning", "home_score", "away_score",
        "status", "home_team", "away_team", "is_final", "home_won",
    )

    def __init__(self) -> None:
        self.inning: int | None = None
        self.half_inning: str | None = None
        self.home_score: int | None = None
        self.away_score: int | None = None
        self.status: str = "Unknown"
        self.home_team: str = "UNK"
        self.away_team: str = "UNK"
        self.is_final: bool = False
        self.home_won: bool | None = None


def _parse_gumbo(data: dict[str, Any]) -> _GameContext:
    """Extract game context from a GUMBO live feed response."""
    ctx = _GameContext()
    try:
        ctx.status = str(data["gameData"]["status"]["detailedState"])
    except (KeyError, TypeError):
        pass
    ctx.is_final = ctx.status in _TERMINAL_STATES
    try:
        teams = data["gameData"]["teams"]
        ctx.home_team = str(teams["home"]["abbreviation"])
        ctx.away_team = str(teams["away"]["abbreviation"])
    except (KeyError, TypeError):
        pass
    try:
        ls = data["liveData"]["linescore"]
        ctx.inning = int(ls["currentInning"])
        ctx.half_inning = str(ls["inningHalf"]).lower()
        ctx.home_score = int(ls["teams"]["home"]["runs"])
        ctx.away_score = int(ls["teams"]["away"]["runs"])
    except (KeyError, TypeError, ValueError):
        pass
    if ctx.is_final and ctx.home_score is not None and ctx.away_score is not None:
        ctx.home_won = ctx.home_score > ctx.away_score
    return ctx


# ── REST orderbook parser ───────────────────────────────────────────


def _parse_rest_orderbook(data: dict[str, Any], ticker: str) -> OrderBook | None:
    """Parse Kalshi REST orderbook response into an OrderBook object."""
    orderbook = data.get("orderbook_fp") or data.get("orderbook", data)
    bids_raw = orderbook.get(
        "yes_dollars", orderbook.get("yes", orderbook.get("bids", [])),
    )
    asks_raw = orderbook.get(
        "no_dollars", orderbook.get("no", orderbook.get("asks", [])),
    )
    if not bids_raw and not asks_raw:
        return None

    book = OrderBook(ticker=ticker)
    book.yes_bids = sorted(
        [[float(lv[0]), float(lv[1])] for lv in bids_raw],
        key=lambda x: x[0],
        reverse=True,
    )
    book.no_bids = sorted(
        [[float(lv[0]), float(lv[1])] for lv in asks_raw],
        key=lambda x: x[0],
        reverse=True,
    )
    book.last_update = time.time()
    return book


# ── Paper Game Monitor ──────────────────────────────────────────────


class PaperGameMonitor:
    """Monitors one game: reads orderbook, detects ask_heavy, simulates DCA.

    One instance per game. Receives orderbook updates via WebSocket callback
    or REST polling fallback, computes depth_ratio each tick, and simulates
    DCA entries (1 contract at mid) whenever ask_heavy is active.
    """

    def __init__(
        self,
        game_pk: int,
        ticker: str,
        date: str,
        home_team: str,
        away_team: str,
        store: PaperTradeStore,
        config: Config,
        ws_client: KalshiWebSocket | None = None,
        game_cap: int = 500,
    ) -> None:
        self.game_pk = game_pk
        self.ticker = ticker
        self._date = date
        self._home = home_team
        self._away = away_team
        self._store = store
        self._config = config
        self._ws = ws_client
        self._game_cap = game_cap
        self._threshold = config.ASK_HEAVY_THRESHOLD

        # DCA state
        self._entries: list[float] = []  # mid price at each entry
        self._last_entry_ts: float = 0.0
        self._is_ask_heavy: bool = False

        # Display state
        self._ctx = _GameContext()
        self._last_mid: float | None = None
        self._last_depth_ratio: float | None = None

        # Runtime
        self._running = False
        self._game_ended = asyncio.Event()
        self._using_ws = False
        self._rest_fallback_active = False
        self._error_count = 0

        store.upsert_game(game_pk, date, home_team, away_team, ticker)

    @property
    def n_entries(self) -> int:
        return len(self._entries)

    @property
    def is_ask_heavy(self) -> bool:
        return self._is_ask_heavy

    # ── WebSocket callbacks ──────────────────────────────────────

    def handle_orderbook_update(self, ticker: str, book: OrderBook) -> None:
        """Callback from KalshiWebSocket on orderbook snapshot/delta."""
        if ticker != self.ticker or not self._running or book.is_empty:
            return
        self._process_book(book)

    def handle_trade(self, trade: dict[str, Any]) -> None:
        """Trade callback — unused in paper trader."""

    # ── Core DCA logic ───────────────────────────────────────────

    def _process_book(self, book: OrderBook) -> None:
        """Compute depth_ratio and simulate DCA entry if ask_heavy."""
        if self._ctx.status in _PREGAME_STATES or self._ctx.is_final:
            return

        mid = book.mid
        depth_ratio = book.depth_ratio
        self._last_mid = mid
        self._last_depth_ratio = depth_ratio
        self._is_ask_heavy = depth_ratio < self._threshold

        if not self._is_ask_heavy:
            return

        now = time.time()
        if now - self._last_entry_ts < self._config.PAPER_TRADE_ENTRY_INTERVAL:
            return
        if len(self._entries) >= self._game_cap:
            return

        # DCA entry: 1 contract at mid
        self._entries.append(mid)
        self._last_entry_ts = now

        # Persist every 10 entries
        if len(self._entries) % 10 == 0:
            self._flush_entries()

    def _flush_entries(self) -> None:
        """Write current entry stats to SQLite."""
        if not self._entries:
            return
        avg_mid = sum(self._entries) / len(self._entries)
        self._store.update_entries(
            self.game_pk,
            n_entries=len(self._entries),
            total_invested=len(self._entries),
            avg_mid=avg_mid,
        )

    # ── Main run loop ────────────────────────────────────────────

    async def run(self) -> None:
        """Run GUMBO polling + orderbook monitoring until game ends."""
        self._running = True
        if self._ws and self._ws.is_connected:
            self._using_ws = True

        log.info(
            "monitoring_started",
            game_pk=self.game_pk,
            ticker=self.ticker,
            method="websocket" if self._using_ws else "rest",
        )

        async with aiohttp.ClientSession() as session:
            gumbo_task = asyncio.create_task(self._gumbo_loop(session))

            if self._using_ws:
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
                ob_task = asyncio.create_task(self._orderbook_loop(session))
                await self._game_ended.wait()
                ob_task.cancel()
                try:
                    await ob_task
                except asyncio.CancelledError:
                    pass

            gumbo_task.cancel()
            try:
                await gumbo_task
            except asyncio.CancelledError:
                pass

        self._running = False

    async def _gumbo_loop(self, session: aiohttp.ClientSession) -> None:
        """Poll GUMBO for game state. Detects game end for settlement."""
        url = f"{self._config.MLB_API_BASE_URL}/game/{self.game_pk}/feed/live"
        interval = self._config.GUMBO_CONTEXT_INTERVAL
        backoff = self._config.ORDERBOOK_INITIAL_BACKOFF

        while self._running:
            try:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    data: dict[str, Any] = await resp.json()

                self._ctx = _parse_gumbo(data)
                backoff = self._config.ORDERBOOK_INITIAL_BACKOFF

                if self._ctx.is_final:
                    log.info(
                        "game_ended",
                        game_pk=self.game_pk,
                        status=self._ctx.status,
                        home_score=self._ctx.home_score,
                        away_score=self._ctx.away_score,
                    )
                    self._settle()
                    return

                wait = (
                    60.0 if self._ctx.status in _PREGAME_STATES else interval
                )

            except aiohttp.ClientError as e:
                log.warning(
                    "gumbo_error", game_pk=self.game_pk, error=str(e),
                )
                self._error_count += 1
                wait = min(backoff, self._config.ORDERBOOK_MAX_BACKOFF)
                backoff = min(backoff * 2, self._config.ORDERBOOK_MAX_BACKOFF)

            await asyncio.sleep(wait)

    async def _orderbook_loop(self, session: aiohttp.ClientSession) -> None:
        """REST orderbook polling (fallback when WS unavailable)."""
        base_url = self._config.KALSHI_API_URL
        path = f"/markets/{self.ticker}/orderbook"
        url = f"{base_url}{path}"
        interval = self._config.ORDERBOOK_POLL_INTERVAL
        backoff = self._config.ORDERBOOK_INITIAL_BACKOFF

        client = KalshiClient(self._config)

        while self._running and not self._game_ended.is_set():
            if self._using_ws and not self._rest_fallback_active:
                return

            try:
                headers = client._sign_request("GET", path)
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 429:
                        retry_after = float(
                            resp.headers.get("Retry-After", interval * 2),
                        )
                        await asyncio.sleep(retry_after)
                        continue
                    resp.raise_for_status()
                    data = await resp.json()

                backoff = self._config.ORDERBOOK_INITIAL_BACKOFF
                book = _parse_rest_orderbook(data, self.ticker)
                if book and not book.is_empty:
                    self._process_book(book)

            except asyncio.CancelledError:
                raise
            except aiohttp.ClientError as e:
                log.warning(
                    "orderbook_error",
                    game_pk=self.game_pk,
                    error=str(e),
                )
                self._error_count += 1
                await asyncio.sleep(
                    min(backoff, self._config.ORDERBOOK_MAX_BACKOFF),
                )
                backoff = min(backoff * 2, self._config.ORDERBOOK_MAX_BACKOFF)
                continue

            await asyncio.sleep(interval)

    async def _ws_fallback_watcher(
        self, session: aiohttp.ClientSession,
    ) -> None:
        """Watch WS health and activate REST fallback when disconnected."""
        poll_interval = self._config.ORDERBOOK_POLL_INTERVAL
        rest_task: asyncio.Task[None] | None = None

        while self._running and not self._game_ended.is_set():
            ws_ok = self._ws is not None and self._ws.is_connected

            if not ws_ok and not self._rest_fallback_active:
                log.warning("ws_fallback", game_pk=self.game_pk)
                self._rest_fallback_active = True
                rest_task = asyncio.create_task(
                    self._orderbook_loop(session),
                )
            elif ws_ok and self._rest_fallback_active:
                log.info("ws_restored", game_pk=self.game_pk)
                self._rest_fallback_active = False
                if rest_task and not rest_task.done():
                    rest_task.cancel()
                    try:
                        await rest_task
                    except asyncio.CancelledError:
                        pass
                    rest_task = None

            await asyncio.sleep(poll_interval)

        if rest_task and not rest_task.done():
            rest_task.cancel()
            try:
                await rest_task
            except asyncio.CancelledError:
                pass

    # ── Settlement ───────────────────────────────────────────────

    def _settle(self) -> None:
        """Compute PnL at game end and persist to SQLite."""
        self._running = False
        self._game_ended.set()
        self._flush_entries()

        if not self._entries:
            return

        home_won = self._ctx.home_won
        if home_won is None:
            log.warning("no_settlement", game_pk=self.game_pk)
            return

        n = len(self._entries)
        total_cost = sum(self._entries)
        avg_mid = total_cost / n
        settlement = 1.0 if home_won else 0.0
        game_pnl = n * settlement - total_cost
        pnl_per_dollar = game_pnl / total_cost if total_cost > 0 else 0.0

        self._store.settle(self.game_pk, home_won, pnl_per_dollar, game_pnl)

        result = "WIN" if home_won else "LOSS"
        print(  # noqa: T201
            f"\n  SETTLED: {self._away}@{self._home} | {result}"
            f" | {n} contracts | avg_mid={avg_mid:.4f}"
            f" | PnL=${game_pnl:+.2f} ({pnl_per_dollar:+.4f}/dollar)",
            flush=True,
        )

    # ── Status line ──────────────────────────────────────────────

    def get_status_line(self) -> str:
        """One-line console status for this game."""
        ctx = self._ctx
        teams = f"{ctx.away_team}@{ctx.home_team}"

        if ctx.inning is not None and ctx.home_score is not None:
            half = "T" if ctx.half_inning == "top" else "B"
            game_info = (
                f"{half}{ctx.inning} {ctx.away_score}-{ctx.home_score}"
            )
        elif ctx.status in _PREGAME_STATES:
            game_info = "pregame"
        else:
            game_info = ctx.status

        mid_str = (
            f"{self._last_mid:.3f}" if self._last_mid is not None else "--"
        )
        dr_str = (
            f"{self._last_depth_ratio:.3f}"
            if self._last_depth_ratio is not None
            else "--"
        )
        ah = "ASK_HEAVY" if self._is_ask_heavy else ""
        mode = (
            "WS"
            if (self._using_ws and not self._rest_fallback_active)
            else "REST"
        )
        entries = f"{self.n_entries}/{self._game_cap}"

        return (
            f"  {teams:<12s} ({game_info:<14s})"
            f"  mid={mid_str:<7s}  dr={dr_str:<7s}"
            f"  {ah:9s}  entries={entries}  [{mode}]"
        )


# ── Game discovery ──────────────────────────────────────────────────


async def _discover_kalshi_tickers(config: Config) -> dict[str, str]:
    """Fetch open Kalshi MLB markets. Returns {normalized_abbr: ticker}."""
    tickers: dict[str, str] = {}
    try:
        async with KalshiClient(config) as client:
            markets = await client.list_mlb_markets(status="open")
            for market in markets:
                ticker = market.get("ticker", "")
                parts = ticker.split("-")
                if len(parts) >= 3:
                    norm = _normalize_abbr(parts[-1])
                    tickers[norm] = ticker
    except Exception as e:
        log.warning("kalshi_discovery_failed", error=str(e))
    return tickers


async def _fetch_game_info(
    game_pk: int, config: Config,
) -> dict[str, str] | None:
    """Fetch teams from GUMBO for a single game_pk."""
    url = f"{config.MLB_API_BASE_URL}/game/{game_pk}/feed/live"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                data: dict[str, Any] = await resp.json()
        teams = data["gameData"]["teams"]
        return {
            "home_team": str(teams["home"]["abbreviation"]),
            "away_team": str(teams["away"]["abbreviation"]),
        }
    except Exception as e:
        log.warning("game_info_failed", game_pk=game_pk, error=str(e))
        return None


async def _match_games_to_tickers(
    game_pks: list[int],
    kalshi_tickers: dict[str, str],
    config: Config,
) -> list[tuple[int, str, str, str]]:
    """Match game_pks to Kalshi tickers.

    Returns list of (game_pk, ticker, home_team, away_team).
    """
    matched: list[tuple[int, str, str, str]] = []
    for game_pk in game_pks:
        info = await _fetch_game_info(game_pk, config)
        if info is None:
            continue
        home = info["home_team"]
        away = info["away_team"]
        ticker = (
            kalshi_tickers.get(_normalize_abbr(home))
            or kalshi_tickers.get(_normalize_abbr(away))
        )
        if ticker:
            matched.append((game_pk, ticker, home, away))
            log.info(
                "game_matched",
                game_pk=game_pk,
                ticker=ticker,
                matchup=f"{away}@{home}",
            )
        else:
            log.info(
                "no_market",
                game_pk=game_pk,
                matchup=f"{away}@{home}",
            )
    return matched


# ── WebSocket helpers ───────────────────────────────────────────────


async def _connect_ws(config: Config) -> KalshiWebSocket | None:
    """Attempt WebSocket connection. Returns None on failure."""
    if not config.KALSHI_WS_URL:
        return None
    ws = KalshiWebSocket(config)
    try:
        await ws.connect()
        return ws
    except Exception as e:
        log.warning("ws_connect_failed", error=str(e))
        return None


async def _ws_run_with_reconnect(
    ws: KalshiWebSocket, config: Config,
) -> None:
    """Run WS receive loop with auto-reconnect on disconnect."""
    interval = config.ORDERBOOK_WS_RECONNECT_INTERVAL
    while True:
        try:
            await ws.run()
        except Exception as e:
            log.warning("ws_disconnected", error=str(e))
        await asyncio.sleep(interval)
        try:
            await ws.connect()
            if ws._subscribed_tickers:
                tickers = list(ws._subscribed_tickers)
                ws._subscribed_tickers.clear()
                await ws.subscribe(tickers)
            log.info("ws_reconnected")
        except Exception as e:
            log.warning("ws_reconnect_failed", error=str(e))


# ── Console output ──────────────────────────────────────────────────


def _print_status(
    monitors: dict[int, PaperGameMonitor],
    ws_connected: bool,
) -> None:
    """Print a live status summary to console."""
    now = datetime.now(ET)
    total_entries = sum(m.n_entries for m in monitors.values())
    active_ah = sum(1 for m in monitors.values() if m.is_ask_heavy)
    ws_str = "WS" if ws_connected else "REST"

    header = (
        f"[{now.strftime('%H:%M:%S')}] Games: {len(monitors)}"
        f" | Ask-heavy: {active_ah}"
        f" | Total entries: {total_entries}"
        f" | Mode: {ws_str}"
    )
    lines = [header]
    for m in monitors.values():
        lines.append(m.get_status_line())
    print("\n".join(lines), flush=True)  # noqa: T201


async def _status_loop(
    monitors: dict[int, PaperGameMonitor],
    config: Config,
    ws: KalshiWebSocket | None = None,
) -> None:
    """Periodic console status updates."""
    interval = config.ORDERBOOK_SUMMARY_INTERVAL
    while True:
        await asyncio.sleep(interval)
        _print_status(monitors, ws.is_connected if ws else False)


def _print_daily_paper_summary(date: str, store: PaperTradeStore) -> None:
    """Print end-of-day paper trading summary."""
    trades = store.get_trades_for_date(date)
    if not trades:
        return

    settled = [t for t in trades if t["settled_at"] is not None]
    if not settled:
        print(  # noqa: T201
            f"\nPAPER TRADING — {date}: {len(trades)} games, none settled yet.",
            flush=True,
        )
        return

    wins = sum(1 for t in settled if t["home_won"] == 1)
    losses = sum(1 for t in settled if t["home_won"] == 0)
    total_contracts = sum(t["total_invested"] for t in settled)
    total_pnl = sum(t["game_pnl"] or 0 for t in settled)
    avg_pnl = total_pnl / total_contracts if total_contracts > 0 else 0
    mean_entries = sum(t["n_entries"] for t in settled) / len(settled)

    cum = store.get_cumulative()
    cum_games = cum.get("total_games", 0) or 0
    cum_pnl = cum.get("total_pnl", 0) or 0
    cum_contracts = cum.get("total_contracts", 0) or 0
    cum_avg = cum_pnl / cum_contracts if cum_contracts else 0

    win_pct = wins / len(settled) * 100 if settled else 0
    sep = "=" * 70

    print(  # noqa: T201
        f"\n{sep}\n"
        f"PAPER TRADING DAILY SUMMARY — {date}\n"
        f"{sep}\n\n"
        f"Games monitored:        {len(trades)}\n"
        f"Games settled:          {len(settled)}\n\n"
        f"Results:\n"
        f"  Total contracts:      {total_contracts:,}\n"
        f"  Won:                  {wins} ({win_pct:.1f}%)\n"
        f"  Lost:                 {losses} ({100 - win_pct:.1f}%)\n"
        f"  Total PnL:            ${total_pnl:+,.2f}\n"
        f"  PnL/dollar invested:  {avg_pnl:+.4f}\n\n"
        f"  Mean entries/game:    {mean_entries:.0f}\n\n"
        f"Cumulative (all days):\n"
        f"  Total games:          {cum_games}\n"
        f"  Total PnL:            ${cum_pnl:+,.2f}\n"
        f"  Running PnL/dollar:   {cum_avg:+.4f}\n"
        f"{sep}",
        flush=True,
    )


# ── Run modes ───────────────────────────────────────────────────────


async def run_single_game(
    game_pk: int, config: Config, game_cap: int,
) -> None:
    """Monitor and paper-trade a single game by game_pk."""
    db_path = config.ORDERBOOK_DB_PATH.parent / "paper_trades.db"
    store = PaperTradeStore(db_path)

    info = await _fetch_game_info(game_pk, config)
    if info is None:
        log.error("cannot_fetch_game_info", game_pk=game_pk)
        store.close()
        return

    home, away = info["home_team"], info["away_team"]
    tickers = await _discover_kalshi_tickers(config)
    ticker = (
        tickers.get(_normalize_abbr(home))
        or tickers.get(_normalize_abbr(away))
    )

    if not ticker:
        log.error(
            "no_kalshi_market",
            game_pk=game_pk,
            home=home,
            away=away,
            available=list(tickers.values()),
        )
        store.close()
        return

    date_str = datetime.now(ET).strftime("%Y-%m-%d")

    ws = await _connect_ws(config)
    if ws:
        await ws.subscribe([ticker])

    monitor = PaperGameMonitor(
        game_pk, ticker, date_str, home, away,
        store, config, ws_client=ws, game_cap=game_cap,
    )

    if ws:
        ws.on_orderbook(monitor.handle_orderbook_update)
        ws.on_trade(monitor.handle_trade)

    monitors = {game_pk: monitor}
    tasks: list[asyncio.Task[Any]] = [
        asyncio.create_task(monitor.run()),
        asyncio.create_task(_status_loop(monitors, config, ws)),
    ]
    if ws:
        tasks.append(asyncio.create_task(_ws_run_with_reconnect(ws, config)))

    await tasks[0]
    for t in tasks[1:]:
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass

    if ws:
        await ws.close()
    store.close()


async def run_all_games(
    date: str, config: Config, game_cap: int,
) -> None:
    """Monitor all MLB games for a date with auto-discovery."""
    db_path = config.ORDERBOOK_DB_PATH.parent / "paper_trades.db"
    store = PaperTradeStore(db_path)
    monitors: dict[int, PaperGameMonitor] = {}
    monitor_tasks: dict[int, asyncio.Task[None]] = {}

    ws = await _connect_ws(config)

    async def discovery_loop() -> None:
        """Periodically discover new games and spawn monitors."""
        checked: set[int] = set()

        while True:
            try:
                game_pks = await fetch_todays_games(date, config)
                kalshi_tickers = await _discover_kalshi_tickers(config)
                new_pks = [pk for pk in game_pks if pk not in checked]

                if new_pks:
                    matched = await _match_games_to_tickers(
                        new_pks, kalshi_tickers, config,
                    )
                    for game_pk, ticker, home, away in matched:
                        if game_pk in monitors:
                            continue

                        if ws and ws.is_connected:
                            await ws.subscribe([ticker])

                        monitor = PaperGameMonitor(
                            game_pk, ticker, date, home, away,
                            store, config, ws_client=ws, game_cap=game_cap,
                        )

                        if ws:
                            ws.on_orderbook(monitor.handle_orderbook_update)
                            ws.on_trade(monitor.handle_trade)

                        monitors[game_pk] = monitor

                        async def _run_and_cleanup(
                            gpk: int, mon: PaperGameMonitor,
                        ) -> None:
                            try:
                                await mon.run()
                            finally:
                                monitors.pop(gpk, None)
                                monitor_tasks.pop(gpk, None)

                        if len(monitors) <= config.ORDERBOOK_MAX_CONCURRENT_GAMES:
                            task = asyncio.create_task(
                                _run_and_cleanup(game_pk, monitor),
                            )
                            monitor_tasks[game_pk] = task

                checked.update(new_pks)

            except Exception as e:
                log.warning("discovery_error", error=str(e))

            await asyncio.sleep(config.ORDERBOOK_SCHEDULE_CHECK_INTERVAL)

    disc_task = asyncio.create_task(discovery_loop())
    summ_task = asyncio.create_task(_status_loop(monitors, config, ws))
    ws_task: asyncio.Task[None] | None = None
    if ws:
        ws_task = asyncio.create_task(_ws_run_with_reconnect(ws, config))

    try:
        while True:
            await asyncio.sleep(60)
            now_et = datetime.now(ET)
            if not monitors and monitor_tasks:
                break
            # Stop after 1:30 AM ET if no active games
            if 1 <= now_et.hour < 4 and not monitors:
                break
    except asyncio.CancelledError:
        pass
    finally:
        disc_task.cancel()
        summ_task.cancel()
        if ws_task:
            ws_task.cancel()
        for task in monitor_tasks.values():
            task.cancel()

        all_tasks = [disc_task, summ_task]
        if ws_task:
            all_tasks.append(ws_task)
        all_tasks.extend(monitor_tasks.values())

        for task in all_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass

        _print_daily_paper_summary(date, store)

        if ws:
            await ws.close()
        store.close()

    print(f"\nSession complete. DB: {db_path}", flush=True)  # noqa: T201


# ── CLI ─────────────────────────────────────────────────────────────


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Paper Trading Bot — DCA ask_heavy strategy "
            "on Kalshi MLB markets"
        ),
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Date to monitor (YYYY-MM-DD). Default: today ET.",
    )
    parser.add_argument(
        "--game-pk",
        type=int,
        help="Monitor a specific MLB game_pk.",
    )
    parser.add_argument(
        "--game-cap",
        type=int,
        default=2000,
        help="Max contracts per game (default: 2000).",
    )
    args = parser.parse_args()
    config = Config()

    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )

    if args.game_pk:
        print(  # noqa: T201
            f"Paper trader starting for game_pk={args.game_pk}\n"
            f"Game cap: {args.game_cap} contracts\n",
            flush=True,
        )
        await run_single_game(args.game_pk, config, args.game_cap)
    else:
        date = args.date or datetime.now(ET).strftime("%Y-%m-%d")
        print(  # noqa: T201
            f"Paper trader starting for {date}\n"
            f"Game cap: {args.game_cap} contracts\n"
            f"Ask-heavy threshold: {config.ASK_HEAVY_THRESHOLD}\n"
            f"Entry interval: {config.PAPER_TRADE_ENTRY_INTERVAL}s\n"
            f"WebSocket: {'enabled' if config.KALSHI_WS_URL else 'disabled'}\n",
            flush=True,
        )
        await run_all_games(date, config, args.game_cap)


if __name__ == "__main__":
    asyncio.run(main())
