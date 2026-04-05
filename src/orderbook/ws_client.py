"""Kalshi WebSocket client for real-time orderbook and trade data.

Maintains local orderbook state from snapshots + deltas, dispatches
updates to registered callbacks. Falls back to REST polling on disconnect.

Auth: reuses RSA-PSS signing from kalshi_client.py.
Protocol: docs/data_sources.md SS5.4.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import base64

import structlog
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from websockets.asyncio.client import ClientConnection

from src.config import Config

log = structlog.get_logger()


@dataclass
class OrderBook:
    """Local orderbook state maintained from WebSocket snapshots and deltas.

    YES bids sorted descending (best first), NO bids sorted descending.
    YES ask = 1.0 - best NO bid.
    """

    ticker: str
    yes_bids: list[list[float]] = field(default_factory=list)  # [[price, qty], ...] desc
    no_bids: list[list[float]] = field(default_factory=list)   # [[price, qty], ...] desc
    last_update: float = 0.0

    @property
    def best_bid(self) -> float:
        return self.yes_bids[0][0] if self.yes_bids else 0.0

    @property
    def best_ask(self) -> float:
        return round(1.0 - self.no_bids[0][0], 4) if self.no_bids else 1.0

    @property
    def mid(self) -> float:
        return (self.best_bid + self.best_ask) / 2.0

    @property
    def spread(self) -> float:
        return self.best_ask - self.best_bid

    @property
    def bid_depth(self) -> int:
        return int(self.yes_bids[0][1]) if self.yes_bids else 0

    @property
    def ask_depth(self) -> int:
        return int(self.no_bids[0][1]) if self.no_bids else 0

    @property
    def depth_ratio(self) -> float:
        total = self.bid_depth + self.ask_depth
        return self.bid_depth / total if total > 0 else 0.5

    @property
    def bid_levels(self) -> list[list[float]]:
        return self.yes_bids

    @property
    def ask_levels(self) -> list[list[float]]:
        """YES ask levels: derived from NO bids (price = 1 - no_price)."""
        return [
            [round(1.0 - lv[0], 4), lv[1]] for lv in self.no_bids
        ]

    @property
    def is_empty(self) -> bool:
        return not self.yes_bids and not self.no_bids

    def apply_snapshot(self, yes: list[list[str]], no: list[list[str]]) -> None:
        """Replace book with full snapshot.

        Args:
            yes: YES bid levels as [["price_str", "qty_str"], ...].
            no: NO bid levels as [["price_str", "qty_str"], ...].
        """
        self.yes_bids = [[float(lv[0]), float(lv[1])] for lv in yes]
        self.no_bids = [[float(lv[0]), float(lv[1])] for lv in no]
        # Sort descending by price (best first)
        self.yes_bids.sort(key=lambda x: x[0], reverse=True)
        self.no_bids.sort(key=lambda x: x[0], reverse=True)
        self.last_update = time.time()

    def apply_delta(self, price: float, delta: float, side: str) -> None:
        """Apply incremental change. If resulting qty <= 0, remove the level.

        Args:
            price: Price level (dollar float).
            delta: Change in contracts (positive = add, negative = remove).
            side: "yes" or "no".
        """
        levels = self.yes_bids if side == "yes" else self.no_bids

        # Find existing level
        for i, lv in enumerate(levels):
            if abs(lv[0] - price) < 1e-6:
                lv[1] += delta
                if lv[1] <= 0:
                    levels.pop(i)
                return

        # New level (only add if positive)
        if delta > 0:
            levels.append([price, delta])
            levels.sort(key=lambda x: x[0], reverse=True)

        self.last_update = time.time()


# Type alias for callbacks
OrderbookCallback = Callable[[str, OrderBook], None]
TradeCallback = Callable[[dict[str, Any]], None]


class KalshiWebSocket:
    """Manages Kalshi WebSocket connection with auto-reconnect.

    One instance serves all games. Monitors subscribe/unsubscribe tickers
    dynamically as games start and end.
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self._ws: ClientConnection | None = None
        self._books: dict[str, OrderBook] = {}
        self._subscribed_tickers: set[str] = set()
        self._orderbook_callbacks: list[OrderbookCallback] = []
        self._trade_callbacks: list[TradeCallback] = []
        self._msg_id = 0
        self._last_seq: dict[int, int] = {}  # sid -> last seq
        self._connected = False
        self._running = False
        self._private_key: rsa.RSAPrivateKey | None = None
        self._key_id = config.KALSHI_KEY_ID
        self._load_key()

    def _load_key(self) -> None:
        """Load RSA private key for WebSocket auth."""
        from pathlib import Path

        key_path = Path(self._config.KALSHI_KEY_PATH)
        if not key_path.exists():
            log.warning("kalshi_key_not_found", path=str(key_path))
            return
        key_bytes = key_path.read_bytes()
        loaded = serialization.load_pem_private_key(key_bytes, password=None)
        if isinstance(loaded, rsa.RSAPrivateKey):
            self._private_key = loaded
            log.info("kalshi_key_loaded", path=str(key_path))

    def _sign_ws_headers(self) -> dict[str, str]:
        """Build auth headers for WebSocket handshake.

        Signs: timestamp_ms + "GET" + "/trade-api/ws/v2"
        (WS path is NOT prefixed with /trade-api/v2 like REST.)
        """
        if self._private_key is None:
            return {}

        timestamp_ms = str(int(time.time() * 1000))
        message = timestamp_ms + "GET" + "/trade-api/ws/v2"
        signature = self._private_key.sign(
            message.encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self._key_id,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        }

    @property
    def is_connected(self) -> bool:
        return self._connected

    def on_orderbook(self, callback: OrderbookCallback) -> None:
        """Register callback for orderbook updates. Called with (ticker, book)."""
        self._orderbook_callbacks.append(callback)

    def on_trade(self, callback: TradeCallback) -> None:
        """Register callback for trade events. Called with trade dict."""
        self._trade_callbacks.append(callback)

    def get_book(self, ticker: str) -> OrderBook | None:
        """Get current orderbook state for a ticker."""
        return self._books.get(ticker)

    async def connect(self) -> None:
        """Connect to Kalshi WebSocket with RSA auth headers."""
        ws_url = self._config.KALSHI_WS_URL
        headers = self._sign_ws_headers()

        self._ws = await websockets.connect(
            ws_url,
            additional_headers=headers,
            ping_interval=self._config.ORDERBOOK_WS_PING_INTERVAL,
            ping_timeout=self._config.ORDERBOOK_WS_PING_TIMEOUT,
        )
        self._connected = True
        log.info("ws_connected", url=ws_url)

    async def subscribe(self, tickers: list[str]) -> None:
        """Subscribe to orderbook_delta and trade channels for given tickers."""
        if not self._ws or not self._connected:
            return

        new_tickers = [t for t in tickers if t not in self._subscribed_tickers]
        if not new_tickers:
            return

        self._msg_id += 1
        msg = {
            "id": self._msg_id,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta", "trade"],
                "market_tickers": new_tickers,
            },
        }
        await self._ws.send(json.dumps(msg))
        self._subscribed_tickers.update(new_tickers)

        # Initialize empty books for new tickers
        for ticker in new_tickers:
            if ticker not in self._books:
                self._books[ticker] = OrderBook(ticker=ticker)

        log.info("ws_subscribed", tickers=new_tickers)

    async def unsubscribe(self, tickers: list[str]) -> None:
        """Unsubscribe from channels for given tickers."""
        if not self._ws or not self._connected:
            return

        active = [t for t in tickers if t in self._subscribed_tickers]
        if not active:
            return

        self._msg_id += 1
        msg = {
            "id": self._msg_id,
            "cmd": "unsubscribe",
            "params": {
                "channels": ["orderbook_delta", "trade"],
                "market_tickers": active,
            },
        }
        await self._ws.send(json.dumps(msg))
        self._subscribed_tickers -= set(active)
        log.info("ws_unsubscribed", tickers=active)

    async def run(self) -> None:
        """Main receive loop. Dispatches messages to handlers.

        Raises ConnectionError or websockets exceptions on disconnect.
        """
        if not self._ws:
            msg = "WebSocket not connected. Call connect() first."
            raise RuntimeError(msg)

        self._running = True
        try:
            async for raw in self._ws:
                if not self._running:
                    break
                try:
                    data = json.loads(raw)
                    self._dispatch(data)
                except (json.JSONDecodeError, KeyError, TypeError) as e:
                    log.warning("ws_parse_error", error=str(e), raw=str(raw)[:200])
        finally:
            self._connected = False
            self._running = False

    async def close(self) -> None:
        """Gracefully close the WebSocket connection."""
        self._running = False
        if self._ws:
            await self._ws.close()
            self._ws = None
        self._connected = False

    def _dispatch(self, data: dict[str, Any]) -> None:
        """Route incoming message to appropriate handler."""
        msg_type = data.get("type")
        if msg_type == "orderbook_snapshot":
            self._handle_orderbook_snapshot(data)
        elif msg_type == "orderbook_delta":
            self._handle_orderbook_delta(data)
        elif msg_type == "trade":
            self._handle_trade(data)
        # Silently ignore subscription confirmations, errors, etc.

    def _handle_orderbook_snapshot(self, data: dict[str, Any]) -> None:
        """Process full orderbook snapshot — replaces local book state."""
        msg = data.get("msg", {})
        ticker = msg.get("market_ticker", "")
        if not ticker:
            return

        yes = msg.get("yes_dollars_fp", [])
        no = msg.get("no_dollars_fp", [])

        if ticker not in self._books:
            self._books[ticker] = OrderBook(ticker=ticker)

        book = self._books[ticker]
        book.apply_snapshot(yes, no)

        # Track sequence for gap detection
        sid = data.get("sid")
        seq = data.get("seq")
        if sid is not None and seq is not None:
            self._last_seq[sid] = seq

        self._fire_orderbook_callbacks(ticker, book)

    def _handle_orderbook_delta(self, data: dict[str, Any]) -> None:
        """Process incremental orderbook update."""
        # Gap detection
        sid = data.get("sid")
        seq = data.get("seq")
        if sid is not None and seq is not None:
            expected = self._last_seq.get(sid)
            if expected is not None and seq != expected + 1:
                log.warning(
                    "ws_seq_gap",
                    sid=sid,
                    expected=expected + 1,
                    got=seq,
                )
                # Gap detected — need to resubscribe for fresh snapshot
                # The next snapshot will reset state
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._resubscribe_for_snapshot(sid))
                except RuntimeError:
                    pass  # no running loop (e.g. in tests)
            self._last_seq[sid] = seq

        msg = data.get("msg", {})
        ticker = msg.get("market_ticker", "")
        if not ticker or ticker not in self._books:
            return

        price = float(msg.get("price_dollars", "0"))
        delta = float(msg.get("delta_fp", "0"))
        side = msg.get("side", "")

        if side not in ("yes", "no") or price == 0:
            return

        book = self._books[ticker]
        book.apply_delta(price, delta, side)

        self._fire_orderbook_callbacks(ticker, book)

    def _handle_trade(self, data: dict[str, Any]) -> None:
        """Process trade (fill) message."""
        msg = data.get("msg", {})
        ticker = msg.get("market_ticker", "")
        if not ticker:
            return

        trade = {
            "trade_id": msg.get("trade_id", ""),
            "market_ticker": ticker,
            "yes_price": float(msg.get("yes_price_dollars", "0")),
            "no_price": float(msg.get("no_price_dollars", "0")),
            "count": int(float(msg.get("count_fp", "0"))),
            "taker_side": msg.get("taker_side", ""),
            "ts": msg.get("ts", time.time()),
        }

        for cb in self._trade_callbacks:
            try:
                cb(trade)
            except Exception as e:
                log.warning("ws_trade_callback_error", error=str(e))

    def _fire_orderbook_callbacks(self, ticker: str, book: OrderBook) -> None:
        """Invoke all registered orderbook callbacks."""
        for cb in self._orderbook_callbacks:
            try:
                cb(ticker, book)
            except Exception as e:
                log.warning("ws_orderbook_callback_error", error=str(e))

    async def _resubscribe_for_snapshot(self, sid: int) -> None:
        """Resubscribe to get a fresh snapshot after sequence gap."""
        # Find tickers associated with this sid (we don't track sid→ticker,
        # so resubscribe all current tickers)
        if not self._subscribed_tickers:
            return
        tickers = list(self._subscribed_tickers)
        self._subscribed_tickers.clear()
        await self.subscribe(tickers)
        log.info("ws_resubscribed_after_gap", sid=sid, ticker_count=len(tickers))
