"""Daily schedule fetch — discover active game_pk values.

Endpoint: MLB Stats API /schedule?sportId=1&date={YYYY-MM-DD}
See docs/data_sources.md §1.1 for verification status.
"""

from __future__ import annotations

from typing import Any

import aiohttp
import structlog

from src.config import Config

log = structlog.get_logger()


async def fetch_todays_games(
    date: str,
    config: Config | None = None,
) -> list[int]:
    """Fetch game_pk values for all MLB games on the given date.

    Args:
        date: Date string in YYYY-MM-DD format.
        config: System config. Uses defaults if None.

    Returns:
        List of game_pk integers. Empty list if no games scheduled.
    """
    if config is None:
        config = Config()

    # Schedule endpoint uses v1 (not v1.1 which is for live feed only)
    base = config.MLB_API_BASE_URL.replace("/v1.1", "/v1")
    url = f"{base}/schedule"
    # Use startDate/endDate + season + gameType for historical compatibility.
    # The bare "date" param doesn't return results for past seasons.
    year = date[:4]
    params = {
        "sportId": "1",
        "startDate": date,
        "endDate": date,
        "season": year,
        "gameType": "R",
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                data: dict[str, Any] = await resp.json()
        except aiohttp.ClientError as e:
            log.warning("schedule_fetch_failed", date=date, error=str(e))
            return []

    dates = data.get("dates", [])
    if not dates:
        log.info("no_games_scheduled", date=date)
        return []

    game_pks: list[int] = []
    for game in dates[0].get("games", []):
        pk = game.get("gamePk")
        if pk is not None:
            game_pks.append(int(pk))

    log.info("schedule_fetched", date=date, game_count=len(game_pks))
    return game_pks
