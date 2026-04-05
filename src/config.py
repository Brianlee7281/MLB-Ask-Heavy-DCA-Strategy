"""Configuration module. Single source of all tunable parameters.

Import as: from src.config import Config
"""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    """System configuration. Loads from .env, falls back to defaults."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── MLB Stats API ───────────────────────────────────────────────────

    MLB_API_BASE_URL: str = "https://statsapi.mlb.com/api/v1.1"

    # ── Kalshi ──────────────────────────────────────────────────────────

    KALSHI_API_URL: str = "https://api.elections.kalshi.com/trade-api/v2"
    KALSHI_WS_URL: str = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    KALSHI_KEY_PATH: Path = Path("./kalshi_key.pem")
    KALSHI_KEY_ID: str = ""
    KALSHI_TAKER_FEE_RATE: float = 0.07
    KALSHI_TAKER_FEE_MAX: float = 0.0175
    KALSHI_MAKER_FEE: float = 0.00
    KALSHI_GAME_SERIES_TICKER: str = "KXMLBGAME"

    # ── Orderbook Logger ────────────────────────────────────────────────

    ORDERBOOK_DB_PATH: Path = Path("./data/orderbook_live.db")
    ORDERBOOK_POLL_INTERVAL: float = 5.0       # seconds between REST orderbook polls (fallback)
    ORDERBOOK_SNAPSHOT_INTERVAL: float = 1.0   # min seconds between DB writes per game (WS mode)
    GUMBO_CONTEXT_INTERVAL: float = 15.0       # seconds between GUMBO polls for game context
    ASK_HEAVY_THRESHOLD: float = 0.4           # depth_ratio < this → ask_heavy
    SIM_FILL_TOLERANCE: float = 0.005          # half-penny tolerance for simulated fills
    ORDERBOOK_SCHEDULE_CHECK_INTERVAL: float = 600.0  # 10 min between schedule checks
    ORDERBOOK_SUMMARY_INTERVAL: float = 60.0   # seconds between console summary prints
    ORDERBOOK_MAX_CONCURRENT_GAMES: int = 15
    ORDERBOOK_INITIAL_BACKOFF: float = 1.0     # exponential backoff start
    ORDERBOOK_MAX_BACKOFF: float = 60.0        # exponential backoff cap
    ORDERBOOK_WS_RECONNECT_INTERVAL: float = 30.0  # seconds between WS reconnect attempts
    ORDERBOOK_WS_PING_INTERVAL: float = 20.0   # WebSocket ping interval
    ORDERBOOK_WS_PING_TIMEOUT: float = 10.0    # WebSocket ping timeout

    # ── Paper Trading (DCA ask_heavy) ────────────────────────────────

    PAPER_TRADE_ENABLED: bool = False
    PAPER_TRADE_GAME_CAP: float = 500.0        # max dollars to invest per game
    PAPER_TRADE_ENTRY_SIZE: float = 1.0         # dollars per DCA entry
    PAPER_TRADE_ENTRY_INTERVAL: float = 1.0     # seconds between entries
