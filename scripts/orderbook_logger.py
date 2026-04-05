"""Phase 0 Orderbook Logging Bot — monitors Kalshi MLB orderbooks without trading.

Primary mode: Kalshi WebSocket for sub-second orderbook and trade data.
Fallback: REST polling if WebSocket disconnects.
Optional: --paper-trade mode simulates DCA ask_heavy strategy.

Usage:
    python scripts/orderbook_logger.py --date 2026-04-06
    python scripts/orderbook_logger.py --game-pk 831547
    python scripts/orderbook_logger.py --date 2026-04-06 --paper-trade
    python scripts/orderbook_logger.py --date 2026-04-06 --paper-trade --game-cap 500 --entry-size 1
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import structlog

from src.config import Config
from src.data_ingestion.schedule import fetch_todays_games
from src.orderbook.monitor import GameMonitor
from src.orderbook.paper_trader import PaperTrader
from src.orderbook.recorder import OrderbookRecorder
from src.orderbook.ws_client import KalshiWebSocket
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


async def _discover_kalshi_tickers(
    config: Config,
) -> dict[str, str]:
    """Fetch open Kalshi MLB game markets and return {normalized_team_abbr: ticker}.

    The ticker format is like KXMLBGAME-26APR042105NYMSF-NYM where the last
    segment is the team code. Each game has two tickers (one per team).
    """
    tickers: dict[str, str] = {}
    try:
        async with KalshiClient(config) as client:
            markets = await client.list_mlb_markets(status="open")
            for market in markets:
                ticker = market.get("ticker", "")
                # Extract team code from end of ticker: KXMLBGAME-26APR04...-NYM -> NYM
                parts = ticker.split("-")
                if len(parts) >= 3:
                    team_code = parts[-1]
                    norm = _normalize_abbr(team_code)
                    tickers[norm] = ticker
    except Exception as e:
        log.warning("kalshi_discovery_failed", error=str(e))
    return tickers


async def _fetch_game_info(
    game_pk: int,
    config: Config,
) -> dict[str, str] | None:
    """Fetch basic game info (teams) from GUMBO for a single game."""
    import aiohttp

    base = config.MLB_API_BASE_URL
    url = f"{base}/game/{game_pk}/feed/live"
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
        log.warning("game_info_fetch_failed", game_pk=game_pk, error=str(e))
        return None


async def _match_games_to_tickers(
    game_pks: list[int],
    kalshi_tickers: dict[str, str],
    config: Config,
) -> list[tuple[int, str, str, str]]:
    """Match MLB game_pks to Kalshi tickers.

    Returns:
        List of (game_pk, ticker, home_team, away_team) tuples.
    """
    matched: list[tuple[int, str, str, str]] = []

    for game_pk in game_pks:
        info = await _fetch_game_info(game_pk, config)
        if info is None:
            continue

        home = info["home_team"]
        away = info["away_team"]
        home_norm = _normalize_abbr(home)
        away_norm = _normalize_abbr(away)

        # Try matching home team first (Kalshi tickers usually use home team)
        ticker = kalshi_tickers.get(home_norm) or kalshi_tickers.get(away_norm)
        if ticker:
            matched.append((game_pk, ticker, home, away))
            log.info(
                "game_discovered",
                game_pk=game_pk,
                ticker=ticker,
                home=home,
                away=away,
            )
        else:
            log.info(
                "game_no_kalshi_market",
                game_pk=game_pk,
                home=home,
                away=away,
            )

    return matched


def _print_summary(
    monitors: dict[int, GameMonitor],
    recorder: OrderbookRecorder,
    error_count: int,
    ws_connected: bool,
    paper_traders: dict[int, PaperTrader] | None = None,
) -> None:
    """Print a live status summary to console."""
    now = datetime.now(ET)
    total_snapshots = recorder.get_total_snapshot_count()
    total_signals = recorder.get_total_signal_count()
    ws_str = "WS" if ws_connected else "REST"

    header = (
        f"[{now.strftime('%H:%M:%S')}] Active: {len(monitors)} games "
        f"| Snapshots: {total_snapshots:,} "
        f"| Signals: {total_signals} "
        f"| Errors: {error_count} "
        f"| Mode: {ws_str}"
    )
    if paper_traders:
        active_pt = sum(1 for pt in paper_traders.values() if pt.n_entries > 0)
        total_inv = sum(pt.total_invested for pt in paper_traders.values())
        header += f" | Paper: {active_pt} active, ${total_inv:.0f} invested"

    lines = [header]

    for monitor in monitors.values():
        lines.append(monitor.get_status_line())

    print("\n".join(lines), flush=True)  # noqa: T201


def _print_game_summary(
    game_pk: int,
    recorder: OrderbookRecorder,
) -> None:
    """Print post-game validation summary."""
    session = recorder.get_game_summary(game_pk)
    if session is None:
        return

    signals = recorder.get_signals_for_game(game_pk)
    snapshot_count = recorder.get_snapshot_count(game_pk)

    duration_s = 0.0
    if session["game_start_ts"] and session["game_end_ts"]:
        duration_s = session["game_end_ts"] - session["game_start_ts"]
    hours = int(duration_s // 3600)
    minutes = int((duration_s % 3600) // 60)

    # Fill rate stats
    fills_60 = sum(1 for s in signals if s.get("sim_fill_60s"))
    total_sigs = len(signals)
    fill_rate = (fills_60 / total_sigs * 100) if total_sigs > 0 else 0.0

    # Avg depth ratio during ask_heavy
    avg_dr = 0.0
    if signals:
        drs = [s["depth_ratio_at_onset"] for s in signals]
        avg_dr = sum(drs) / len(drs)

    # Theoretical PnL
    pnl_sum = sum(
        s["theoretical_pnl"] for s in signals if s.get("theoretical_pnl") is not None
    )

    home_won = session.get("home_won")
    home_won_str = "YES" if home_won == 1 else "NO" if home_won == 0 else "UNKNOWN"

    print(  # noqa: T201
        f"\nGame {game_pk} Summary:\n"
        f"  Duration: {hours}h {minutes}m\n"
        f"  Snapshots recorded: {snapshot_count:,}\n"
        f"  Ask-heavy signals: {total_sigs}\n"
        f"  Avg depth_ratio during ask_heavy: {avg_dr:.3f}\n"
        f"  Simulated fills (60s): {fills_60}/{total_sigs}"
        f" ({fill_rate:.1f}%)\n"
        f"  Theoretical PnL (if traded all): {pnl_sum:+.4f}\n"
        f"  Home won: {home_won_str}\n"
        f"  Errors: {session.get('monitoring_errors', 0)}",
        flush=True,
    )


async def _connect_websocket(config: Config) -> KalshiWebSocket | None:
    """Attempt to connect to Kalshi WebSocket. Returns None on failure."""
    ws = KalshiWebSocket(config)
    try:
        await ws.connect()
        return ws
    except Exception as e:
        log.warning("ws_connect_failed", error=str(e))
        return None


async def _ws_run_with_reconnect(
    ws: KalshiWebSocket,
    config: Config,
) -> None:
    """Run WebSocket receive loop with auto-reconnect.

    On disconnect, waits and retries. The monitors detect disconnection
    via ws.is_connected and fall back to REST automatically.
    """
    reconnect_interval = config.ORDERBOOK_WS_RECONNECT_INTERVAL

    while True:
        try:
            await ws.run()
        except Exception as e:
            log.warning("ws_disconnected", error=str(e))

        # WS disconnected — try to reconnect
        await asyncio.sleep(reconnect_interval)
        try:
            await ws.connect()
            # Re-subscribe to all tickers
            if ws._subscribed_tickers:
                tickers = list(ws._subscribed_tickers)
                ws._subscribed_tickers.clear()
                await ws.subscribe(tickers)
            log.info("ws_reconnected")
        except Exception as e:
            log.warning("ws_reconnect_failed", error=str(e))


async def run_single_game(
    game_pk: int,
    config: Config,
) -> None:
    """Monitor a single game by game_pk (discovers ticker automatically)."""
    recorder = OrderbookRecorder(config)

    # Get game info
    info = await _fetch_game_info(game_pk, config)
    if info is None:
        log.error("cannot_fetch_game_info", game_pk=game_pk)
        recorder.close()
        return

    home = info["home_team"]
    away = info["away_team"]

    # Find Kalshi ticker
    tickers = await _discover_kalshi_tickers(config)
    home_norm = _normalize_abbr(home)
    away_norm = _normalize_abbr(away)
    ticker = tickers.get(home_norm) or tickers.get(away_norm)

    if not ticker:
        log.error(
            "no_kalshi_market",
            game_pk=game_pk,
            home=home,
            away=away,
            available_tickers=list(tickers.values()),
        )
        recorder.close()
        return

    date_str = datetime.now(ET).strftime("%Y-%m-%d")
    recorder.record_game_session(game_pk, date_str, home, away, ticker)

    # Connect WebSocket
    ws = await _connect_websocket(config)
    if ws:
        await ws.subscribe([ticker])

    # Paper trading
    paper_trader: PaperTrader | None = None
    if config.PAPER_TRADE_ENABLED:
        paper_trader = PaperTrader(game_pk, recorder, config)

    monitor = GameMonitor(
        game_pk, ticker, recorder, config,
        ws_client=ws, paper_trader=paper_trader,
    )

    # Register WS callbacks
    if ws:
        ws.on_orderbook(monitor.handle_orderbook_update)
        ws.on_trade(monitor.handle_trade)

    monitors = {game_pk: monitor}
    paper_traders = {game_pk: paper_trader} if paper_trader else None

    # Run monitor, WS receive loop, and summary loop concurrently
    tasks: list[asyncio.Task[Any]] = [
        asyncio.create_task(monitor.run()),
        asyncio.create_task(
            _summary_loop(monitors, recorder, config, ws, paper_traders),
        ),
    ]
    if ws:
        tasks.append(asyncio.create_task(_ws_run_with_reconnect(ws, config)))

    # Wait for monitor to finish (game end)
    await tasks[0]

    # Cancel remaining tasks
    for task in tasks[1:]:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    _print_game_summary(game_pk, recorder)

    if ws:
        await ws.close()
    recorder.close()


async def _summary_loop(
    monitors: dict[int, GameMonitor],
    recorder: OrderbookRecorder,
    config: Config,
    ws: KalshiWebSocket | None = None,
    paper_traders: dict[int, PaperTrader] | None = None,
) -> None:
    """Print periodic console summaries."""
    interval = config.ORDERBOOK_SUMMARY_INTERVAL
    error_count = 0
    while True:
        await asyncio.sleep(interval)
        ws_connected = ws.is_connected if ws else False
        _print_summary(monitors, recorder, error_count, ws_connected, paper_traders)


async def run_all_games(
    date: str,
    config: Config,
) -> None:
    """Monitor all MLB games for a given date with auto-discovery."""
    recorder = OrderbookRecorder(config)
    monitors: dict[int, GameMonitor] = {}
    monitor_tasks: dict[int, asyncio.Task[None]] = {}
    paper_traders: dict[int, PaperTrader] = {}
    total_errors = 0

    # Connect WebSocket (shared across all games)
    ws = await _connect_websocket(config)

    async def discovery_loop() -> None:
        """Periodically discover new games and spawn monitors."""
        nonlocal total_errors
        checked_games: set[int] = set()

        while True:
            try:
                game_pks = await fetch_todays_games(date, config)
                kalshi_tickers = await _discover_kalshi_tickers(config)
                new_pks = [pk for pk in game_pks if pk not in checked_games]

                if new_pks:
                    matched = await _match_games_to_tickers(
                        new_pks, kalshi_tickers, config,
                    )

                    for game_pk, ticker, home, away in matched:
                        if game_pk in monitors:
                            continue

                        recorder.record_game_session(
                            game_pk, date, home, away, ticker,
                        )

                        # Subscribe this ticker on the shared WS
                        if ws and ws.is_connected:
                            await ws.subscribe([ticker])

                        # Paper trading
                        paper_trader: PaperTrader | None = None
                        if config.PAPER_TRADE_ENABLED:
                            paper_trader = PaperTrader(game_pk, recorder, config)
                            paper_traders[game_pk] = paper_trader

                        monitor = GameMonitor(
                            game_pk, ticker, recorder, config,
                            ws_client=ws, paper_trader=paper_trader,
                        )

                        # Register WS callbacks
                        if ws:
                            ws.on_orderbook(monitor.handle_orderbook_update)
                            ws.on_trade(monitor.handle_trade)

                        monitors[game_pk] = monitor

                        async def _run_and_cleanup(
                            gpk: int, mon: GameMonitor,
                        ) -> None:
                            try:
                                await mon.run()
                            finally:
                                _print_game_summary(gpk, recorder)
                                monitors.pop(gpk, None)
                                monitor_tasks.pop(gpk, None)

                        if len(monitors) <= config.ORDERBOOK_MAX_CONCURRENT_GAMES:
                            task = asyncio.create_task(
                                _run_and_cleanup(game_pk, monitor),
                            )
                            monitor_tasks[game_pk] = task

                checked_games.update(new_pks)

            except Exception as e:
                log.warning("discovery_error", error=str(e))
                total_errors += 1

            await asyncio.sleep(config.ORDERBOOK_SCHEDULE_CHECK_INTERVAL)

    # Start discovery, WS receive, and summary loops
    disc_task = asyncio.create_task(discovery_loop())
    summ_task = asyncio.create_task(
        _summary_loop(monitors, recorder, config, ws, paper_traders or None),
    )

    ws_task: asyncio.Task[None] | None = None
    if ws:
        ws_task = asyncio.create_task(_ws_run_with_reconnect(ws, config))

    try:
        while True:
            await asyncio.sleep(60)
            now_et = datetime.now(ET)
            # Check if all games are done
            if not monitors and monitor_tasks:
                break
            # Time-based stop: 1:30 AM next day
            if now_et.hour >= 1 and now_et.hour < 4 and not monitors:
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

        # Daily paper trading summary
        if config.PAPER_TRADE_ENABLED:
            _print_daily_paper_summary(date, recorder)

        if ws:
            await ws.close()
        recorder.close()

    print(  # noqa: T201
        f"\nSession complete. DB: {config.ORDERBOOK_DB_PATH}",
        flush=True,
    )


def _print_daily_paper_summary(date: str, recorder: OrderbookRecorder) -> None:
    """Print end-of-day paper trading summary."""
    trades = recorder.get_paper_trades_for_date(date)
    if not trades:
        return

    settled = [t for t in trades if t["status"] == "settled"]
    if not settled:
        print(  # noqa: T201
            f"\nPAPER TRADING — {date}: {len(trades)} games, none settled yet.",
            flush=True,
        )
        return

    wins = sum(1 for t in settled if t["home_won"] == 1)
    losses = sum(1 for t in settled if t["home_won"] == 0)
    total_invested = sum(t["total_invested"] for t in settled)
    total_pnl = sum(t["game_pnl"] or 0 for t in settled)
    pnl_per_dollar = total_pnl / total_invested if total_invested > 0 else 0
    mean_entries = sum(t["n_entries"] for t in settled) / len(settled)
    mean_invested = total_invested / len(settled)
    cap_hits = sum(
        1 for t in settled
        if t["total_invested"] >= t["game_cap"] * 0.95
    )

    fill_rates = [t["fill_rate"] for t in settled if t["fill_rate"] is not None]
    fill_times = [t["avg_fill_time"] for t in settled if t["avg_fill_time"] is not None]
    mean_fill_rate = sum(fill_rates) / len(fill_rates) if fill_rates else 0
    mean_fill_time = sum(fill_times) / len(fill_times) if fill_times else 0

    # Cumulative stats
    cum = recorder.get_paper_trades_cumulative()
    cum_games = cum.get("total_games", 0) or 0
    cum_pnl = cum.get("total_pnl", 0) or 0
    cum_invested = cum.get("total_invested", 0) or 0
    cum_pnl_per = cum_pnl / cum_invested if cum_invested > 0 else 0

    win_pct = wins / len(settled) * 100 if settled else 0
    sep = "=" * 78

    print(  # noqa: T201
        f"\n{sep}\n"
        f"PAPER TRADING DAILY SUMMARY — {date}\n"
        f"{sep}\n\n"
        f"Games monitored:          {len(trades)}\n"
        f"Games with paper trades:  {len(settled)}\n\n"
        f"Paper trade results:\n"
        f"  Total invested:         ${total_invested:,.0f}\n"
        f"  Won:                    {wins} ({win_pct:.1f}%)\n"
        f"  Lost:                   {losses} ({100 - win_pct:.1f}%)\n"
        f"  Total PnL:              ${total_pnl:+,.2f}\n"
        f"  PnL per $1 invested:    {pnl_per_dollar:+.4f}\n\n"
        f"  Mean entries/game:       {mean_entries:.0f}\n"
        f"  Mean invested/game:      ${mean_invested:.2f}\n"
        f"  Games hitting cap:       {cap_hits} ({cap_hits / len(settled) * 100:.1f}%)\n\n"
        f"Fill simulation:\n"
        f"  Mean fill rate:          {mean_fill_rate * 100:.1f}%\n"
        f"  Mean fill time:          {mean_fill_time:.1f}s\n\n"
        f"Cumulative (all days):\n"
        f"  Total games:             {cum_games}\n"
        f"  Total PnL:               ${cum_pnl:+,.2f}\n"
        f"  Running PnL/$1:          {cum_pnl_per:+.4f}\n"
        f"{sep}",
        flush=True,
    )


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 0 Orderbook Logger — monitor Kalshi MLB orderbooks",
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
        "--db-path",
        type=str,
        help="SQLite database path (default: data/orderbook_live.db).",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        help="REST orderbook poll interval in seconds (default: 5).",
    )
    parser.add_argument(
        "--no-ws",
        action="store_true",
        help="Disable WebSocket, use REST polling only.",
    )
    parser.add_argument(
        "--paper-trade",
        action="store_true",
        help="Enable paper trading (DCA ask_heavy strategy).",
    )
    parser.add_argument(
        "--game-cap",
        type=float,
        help="Max dollars to invest per game (default: 500).",
    )
    parser.add_argument(
        "--entry-size",
        type=float,
        help="Dollars per DCA entry (default: 1).",
    )
    args = parser.parse_args()

    # Build config with overrides
    overrides: dict[str, Any] = {}
    if args.db_path:
        overrides["ORDERBOOK_DB_PATH"] = Path(args.db_path)
    if args.poll_interval:
        overrides["ORDERBOOK_POLL_INTERVAL"] = args.poll_interval
    if args.no_ws:
        overrides["KALSHI_WS_URL"] = ""
    if args.paper_trade:
        overrides["PAPER_TRADE_ENABLED"] = True
    if args.game_cap is not None:
        overrides["PAPER_TRADE_GAME_CAP"] = args.game_cap
    if args.entry_size is not None:
        overrides["PAPER_TRADE_ENTRY_SIZE"] = args.entry_size

    config = Config(**overrides) if overrides else Config()

    # Configure structlog for console output
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
        await run_single_game(args.game_pk, config)
    else:
        date = args.date or datetime.now(ET).strftime("%Y-%m-%d")
        paper_str = "disabled"
        if config.PAPER_TRADE_ENABLED:
            paper_str = (
                f"enabled (cap=${config.PAPER_TRADE_GAME_CAP:.0f}, "
                f"entry=${config.PAPER_TRADE_ENTRY_SIZE:.0f})"
            )
        print(  # noqa: T201
            f"Orderbook Logger starting for {date}\n"
            f"DB: {config.ORDERBOOK_DB_PATH}\n"
            f"Poll interval: {config.ORDERBOOK_POLL_INTERVAL}s\n"
            f"GUMBO interval: {config.GUMBO_CONTEXT_INTERVAL}s\n"
            f"WebSocket: {'enabled' if config.KALSHI_WS_URL else 'disabled'}\n"
            f"Paper trading: {paper_str}\n",
            flush=True,
        )
        await run_all_games(date, config)


if __name__ == "__main__":
    asyncio.run(main())
