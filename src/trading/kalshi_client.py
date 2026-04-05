"""Kalshi REST API client.

Auth: RSA JWT signing per data_sources.md §5.2.
"""

from __future__ import annotations

import base64
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp
import structlog
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from pydantic import BaseModel, Field

from src.config import Config

log = structlog.get_logger()


class MarketSnapshot(BaseModel):
    """Current market state from Kalshi."""

    ticker: str
    best_bid: float = Field(ge=0.0, le=1.0)
    best_ask: float = Field(ge=0.0, le=1.0)
    mid_price: float = Field(ge=0.0, le=1.0)
    bid_size: int = Field(ge=0, default=0)
    ask_size: int = Field(ge=0, default=0)
    timestamp: datetime

    @property
    def spread(self) -> float:
        return self.best_ask - self.best_bid


class KalshiClient:
    """Async Kalshi API client with read support."""

    def __init__(self, config: Config | None = None) -> None:
        self.config = config or Config()
        self._base_url = self.config.KALSHI_API_URL
        self._session: aiohttp.ClientSession | None = None
        self._private_key: rsa.RSAPrivateKey | None = None
        self._key_id = self.config.KALSHI_KEY_ID
        self._load_key()

    def _load_key(self) -> None:
        """Load RSA private key from config path, if it exists."""
        key_path = Path(self.config.KALSHI_KEY_PATH)
        if not key_path.exists():
            log.warning(
                "kalshi_key_not_found",
                path=str(key_path),
                msg="Auth will fail — generate key and set KALSHI_KEY_PATH",
            )
            return

        key_bytes = key_path.read_bytes()
        loaded = serialization.load_pem_private_key(key_bytes, password=None)
        if not isinstance(loaded, rsa.RSAPrivateKey):
            msg = "KALSHI_KEY_PATH must point to an RSA private key"
            raise TypeError(msg)
        self._private_key = loaded
        log.info("kalshi_key_loaded", path=str(key_path))

    def _sign_request(self, method: str, path: str) -> dict[str, str]:
        """Build auth headers with RSA-signed timestamp."""
        if self._private_key is None:
            return {}

        timestamp_ms = str(int(time.time() * 1000))
        full_path = "/trade-api/v2" + path.split("?")[0]
        message = timestamp_ms + method.upper() + full_path
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
            "Content-Type": "application/json",
        }

    async def __aenter__(self) -> KalshiClient:
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def _get(self, path: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        """Authenticated GET request."""
        if self._session is None:
            msg = "Use 'async with KalshiClient() as client:' to manage session"
            raise RuntimeError(msg)

        url = f"{self._base_url}{path}"
        headers = self._sign_request("GET", path)

        try:
            async with self._session.get(url, headers=headers, params=params) as resp:
                if resp.status in (401, 403):
                    body = await resp.text()
                    log.warning(
                        "kalshi_auth_failed",
                        status=resp.status,
                        body=body[:200],
                    )
                resp.raise_for_status()
                result: dict[str, Any] = await resp.json()
                return result
        except aiohttp.ClientError as e:
            log.warning("kalshi_request_failed", path=path, error=str(e))
            raise

    # ── Market Data ───────────────────────────────────────────────────

    async def list_mlb_markets(
        self,
        status: str | None = None,
        series_ticker: str = "KXMLBGAME",
    ) -> list[dict[str, Any]]:
        """List MLB game winner markets."""
        params: dict[str, str] = {"series_ticker": series_ticker}
        if status:
            params["status"] = status

        data = await self._get("/markets", params=params)
        markets: list[dict[str, Any]] = data.get("markets", [])
        log.info("kalshi_markets_fetched", count=len(markets))
        return markets

    async def get_orderbook(self, ticker: str) -> MarketSnapshot:
        """Fetch orderbook for a specific market ticker."""
        data = await self._get(f"/markets/{ticker}/orderbook")

        orderbook = data.get("orderbook_fp") or data.get("orderbook", data)

        bids = orderbook.get("yes_dollars", orderbook.get("yes", orderbook.get("bids", [])))
        asks = orderbook.get("no_dollars", orderbook.get("no", orderbook.get("asks", [])))

        best_bid = max((float(lv[0]) for lv in bids), default=0.0)
        best_no = max((float(lv[0]) for lv in asks), default=0.0)
        best_ask = round(1.0 - best_no, 4) if best_no > 0 else 1.0

        bid_size = int(float(bids[-1][1])) if bids else 0
        ask_size = int(float(asks[-1][1])) if asks else 0

        mid_price = (best_bid + best_ask) / 2.0 if (bids or asks) else 0.5

        return MarketSnapshot(
            ticker=ticker,
            best_bid=best_bid,
            best_ask=best_ask,
            mid_price=mid_price,
            bid_size=bid_size,
            ask_size=ask_size,
            timestamp=datetime.now(UTC),
        )
