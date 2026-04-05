# Data Sources — Verified API Endpoints & Schemas

> All verification data from game 831547 (TB @ PHI), 2026-03-23. 271 pitches, 119 polls, 30 minutes of sustained 15-second polling.

---

## 1. MLB Stats API (GUMBO) — Live Game Feed

### 1.1 Endpoints

| endpoint | url_pattern | auth | rate_limit | verified |
|---|---|---|---|---|
| Schedule | `https://statsapi.mlb.com/api/v1.1/schedule?sportId=1&date={YYYY-MM-DD}` | None | <!-- UNVERIFIED --> | ✅ |
| Live Feed | `https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live` | None | 15s polling sustained 119 polls, no throttling | ✅ |

- **Cost:** Free, no API key required
- **Latency:** ~12 seconds estimated vs live broadcast (<!-- UNVERIFIED --> — broadcast comparison not yet done)

### 1.2 Schedule → game_pk Discovery Flow

```
GET /api/v1.1/schedule?sportId=1&date=2026-03-26
  → response.dates[0].games[i].gamePk  (integer, e.g., 831547)
  → use gamePk in Live Feed endpoint
```

- **Verified:** ✅ Tested on game 831547

### 1.3 GUMBO Response Structure

Three top-level nodes:

```
{
  "metaData": { ... },      // polling hints, timestamps
  "gameData": { ... },      // teams, players, venue, weather, probable pitchers
  "liveData": { ... }       // all plays, pitch-level Statcast, linescore
}
```

#### metaData

| field | path | type | purpose | verified |
|---|---|---|---|---|
| wait | `metaData.wait` | int (seconds) | recommended polling interval | ☐ <!-- UNVERIFIED --> |
| timeStamp | `metaData.timeStamp` | string | last update timestamp | <!-- UNVERIFIED --> |

#### gameData (pre-game / static)

| field | path | type | purpose | verified |
|---|---|---|---|---|
| teams | `gameData.teams.home/away` | object | team name, id, abbreviation | ✅ |
| players | `gameData.players` | object | roster with player IDs, names, positions | ✅ (implicit) |
| venue | `gameData.venue` | object | park name, id | <!-- UNVERIFIED --> |
| weather | `gameData.weather` | object | temp, wind, condition | <!-- UNVERIFIED --> |
| probablePitchers | `gameData.probablePitchers` | object | starter IDs | <!-- UNVERIFIED --> |
| status | `gameData.status.detailedState` | string | "In Progress", "Final", etc. | ✅ (game end detection) |

#### liveData (real-time)

| field | path | type | purpose | verified |
|---|---|---|---|---|
| allPlays | `liveData.plays.allPlays` | array | all completed plate appearances | ✅ |
| currentPlay | `liveData.plays.currentPlay` | object | in-progress PA | ✅ |
| linescore | `liveData.linescore` | object | inning, half, outs, runners, score | ✅ |

### 1.4 Pitch-Level Field Availability

**Test metadata:** Game 831547, TB @ PHI, 271 pitches, 119 polls, 2026-03-23

Path to a single pitch event:
```
liveData.plays.allPlays[i].playEvents[j]
```

#### Tier 1: Always Available (Confirmed 100%)

| field | json_path | type | availability | notes |
|---|---|---|---|---|
| pitch velocity (start) | `pitchData.startSpeed` | float (mph) | ✅ 100.0% (271/271) | core fatigue signal |
| pitch velocity (end) | `pitchData.endSpeed` | float (mph) | ✅ 100.0% (271/271) | |
| plate coordinates | `pitchData.coordinates.x`, `.y` | float | ✅ 100.0% | pitch location at plate |
| break angle | `pitchData.breaks.breakAngle` | float | ✅ 100.0% | movement tracking |
| zone | `pitchData.zone` | int (1-14) | ✅ 100.0% | strike zone grid |
| plate time | `pitchData.plateTime` | float (sec) | ✅ 100.0% | pitch travel time |
| pitch type code | `details.type.code` | string | ✅ 100.0% | FF, SL, CH, CU, SI, FC, etc. |
| pitcher ID | `matchup.pitcher.id` | int | ✅ | pitcher changes detected (4 changes in test) |
| batter ID | `matchup.batter.id` | int | ✅ | |
| at-bat index | `atBatIndex` | int | ✅ | increments each PA — primary diff key |
| call description | `details.call.description` | string | ✅ | "Called Strike", "Ball", etc. |

#### Tier 2: Available with Minor Gaps

| field | json_path | type | availability | notes |
|---|---|---|---|---|
| spin rate | `pitchData.breaks.spinRate` | int (rpm) | ✅ 98.9% (268/271) | **leading fatigue indicator — confirmed live** |
| spin direction | `pitchData.breaks.spinDirection` | int (degrees) | ✅ 98.9% (268/271) | |
| extension | `pitchData.extension` | float (ft) | ⚠️ 92.6% | sometimes delayed |

#### Tier 3: Unverified or Unavailable

| field | json_path | type | availability | notes |
|---|---|---|---|---|
| release point X | `pitchData.coordinates.x0` | float | ⚠️ UNVERIFIED | plate coords (x,y) confirmed, release coords (x0,z0) NOT explicitly tested |
| release point Z | `pitchData.coordinates.z0` | float | ⚠️ UNVERIFIED | same — **needs one more targeted verification** |
| exit velocity | `hitData.launchSpeed` | float (mph) | ☐ UNTESTED | insufficient batted ball events in 30-min test window |
| launch angle | `hitData.launchAngle` | float (deg) | ☐ UNTESTED | same |
| hit distance | `hitData.totalDistance` | float (ft) | ☐ UNTESTED | same |
| hit coordinates | `hitData.coordinates.coordX/Y` | float | ☐ UNTESTED | same |
| break length | `pitchData.breaks.breakLength` | float | ❌ NOT LIVE 0.0% | post-game backfill only |
| break Y | `pitchData.breaks.breakY` | float | ❌ NOT LIVE 0.0% | post-game backfill only |

#### Fatigue Model Tier Status

| tier | required_fields | status | implication |
|---|---|---|---|
| 1 | velocity + pitch_count + pitch_mix_shift | ✅ CONFIRMED | baseline fatigue model viable |
| 2 | + spin_rate | ✅ CONFIRMED (98.9%) | leading indicator available |
| 3 | + release_point (x0, z0) | ⚠️ PENDING | needs x0/z0 verification; plate x/y can serve as proxy |

### 1.5 Game State Detection (Event Diffing)

Detect key events by comparing consecutive GUMBO responses:

| event | detection_method | verified |
|---|---|---|
| new plate appearance | `atBatIndex` increments | ✅ |
| pitching change | `matchup.pitcher.id` changes between PAs | ✅ (4 changes detected: Alvarado → Gómez → Keller → Bigge) |
| inning change | `about.inning` and `about.halfInning` change | ✅ (implicit from test) |
| game end | `gameData.status.detailedState` == "Final" | ✅ |
| mound visit | `playEvents` with type containing mound visit | <!-- UNVERIFIED --> |
| lineup change / pinch hitter | `matchup.batter.id` changes unexpectedly | ☐ (no PH observed in test window) |
| new pitch (mid-PA) | `playEvents` array length increases | ✅ |

### 1.6 Linescore State Extraction

```
liveData.linescore.currentInning        → int (1-9+)
liveData.linescore.isTopInning          → bool
liveData.linescore.outs                 → int (0-2)
liveData.linescore.offense.first        → player object or absent (runner on 1B)
liveData.linescore.offense.second       → player object or absent (runner on 2B)
liveData.linescore.offense.third        → player object or absent (runner on 3B)
liveData.linescore.teams.home.runs      → int
liveData.linescore.teams.away.runs      → int
```

Base encoding:
```python
def encode_bases(first, second, third) -> int:
    """Returns 0-7 binary encoding: bit2=1B, bit1=2B, bit0=3B"""
    return (int(first is not None) << 2
          | int(second is not None) << 1
          | int(third is not None))
```

### 1.7 Polling Protocol

| parameter | value | source |
|---|---|---|
| recommended interval | use `metaData.wait` if present, else 10-15 seconds | architecture.md (<!-- UNVERIFIED --> for metaData.wait) |
| tested interval | 15 seconds | checklist (119 polls, no throttling) |
| diff strategy | track `atBatIndex` for PA-level, `playEvents` length for pitch-level | tested |
| sustained duration | 30+ minutes confirmed | checklist |

### 1.8 Edge Cases

| scenario | handling | verified |
|---|---|---|
| rain delay | monitor `gameData.status.detailedState` for delay status; keep polling | ☐ <!-- UNVERIFIED --> |
| suspended game | check status field; may resume with different game_pk | ☐ <!-- UNVERIFIED --> |
| extra innings (Manfred runner) | runner on 2B should appear in `linescore.offense.second` at inning start | ☐ <!-- UNVERIFIED --> |
| doubleheader | separate game_pk values; 7-inning games no longer apply (2026) | ☐ <!-- UNVERIFIED --> |
| lineup change / pinch hitter | new batter_id appears in `matchup.batter.id` | ☐ (no PH in test window) |
| challenge / review | play may be overturned; watch for `reviewType` in play events | ☐ <!-- UNVERIFIED --> |
| ejection | player removed from lineup; roster changes may appear in gameData | ☐ <!-- UNVERIFIED --> |

### 1.9 Umpire Strike Zone Data (for Zone Estimation Model)

| field | json_path | type | purpose | verified |
|---|---|---|---|---|
| plate coordinate X | `pitchData.coordinates.x` | float (feet from center) | horizontal plate location | ✅ 100% |
| plate coordinate Y | `pitchData.coordinates.y` | float (feet) | vertical plate location | ✅ 100% |
| call | `details.call.description` | string | "Called Strike" / "Ball" | ✅ |

- Used for umpire zone estimation model (see baseball_model.md §Appendix B)
- Require ~30+ called pitches for zone factor estimation

---

## 2. Statcast Historical Data (pybaseball)

### 2.1 Installation

```bash
pip install pybaseball
```

### 2.2 Key Functions

| function | purpose | parameters | row_limit | date_chunking | verified |
|---|---|---|---|---|---|
| `statcast(start_dt, end_dt)` | all pitches in date range | `'YYYY-MM-DD'` strings | ~40K per call | chunk by week | ☐ |
| `statcast_pitcher(start_dt, end_dt, player_id)` | single pitcher's pitches | add `player_id: int` | — | — | ☐ |
| `pitching_stats(season, qual=0)` | season aggregates from FanGraphs | `season: int` | — | — | ☐ |
| `batting_stats(season, qual=0)` | season batting aggregates | `season: int` | — | — | ☐ |

```python
from pybaseball import statcast, statcast_pitcher, pitching_stats, batting_stats

# Example: pull one week of Statcast data
df = statcast('2025-06-01', '2025-06-07')

# Example: pull a specific pitcher's season
df = statcast_pitcher('2025-04-01', '2025-09-30', player_id=543037)

# Example: season aggregates
pitchers = pitching_stats(2025, qual=0)
```

### 2.3 Key Columns

Columns from Statcast CSVs used by this system. Cross-referenced to GUMBO live fields.

| statcast_column | type | description | gumbo_equivalent | gumbo_path |
|---|---|---|---|---|
| `release_speed` | float | pitch velocity (mph) | startSpeed | `pitchData.startSpeed` |
| `release_spin_rate` | int | spin rate (rpm) | spinRate | `pitchData.breaks.spinRate` |
| `release_pos_x` | float | release point X (feet) | x0 | `pitchData.coordinates.x0` (⚠️) |
| `release_pos_z` | float | release point Z (feet) | z0 | `pitchData.coordinates.z0` (⚠️) |
| `plate_x` | float | horizontal plate location (feet) | x | `pitchData.coordinates.x` |
| `plate_z` | float | vertical plate location (feet) | y | `pitchData.coordinates.y` |
| `pitch_type` | string | pitch type code (FF, SL, etc.) | type.code | `details.type.code` |
| `launch_speed` | float | exit velocity (mph) | launchSpeed | `hitData.launchSpeed` (☐) |
| `launch_angle` | float | launch angle (degrees) | launchAngle | `hitData.launchAngle` (☐) |
| `events` | string | PA outcome (single, home_run, etc.) | — | derived from play result |
| `description` | string | pitch result (called_strike, ball, etc.) | call.description | `details.call.description` |
| `pitcher` | int | pitcher MLB ID | pitcher.id | `matchup.pitcher.id` |
| `batter` | int | batter MLB ID | batter.id | `matchup.batter.id` |
| `game_pk` | int | game identifier | gamePk | from schedule |
| `game_date` | date | game date | — | from schedule |
| `p_throws` | string | pitcher handedness (L/R) | — | from `gameData.players` |
| `stand` | string | batter stance (L/R) | — | from `gameData.players` |
| `zone` | int | strike zone grid (1-14) | zone | `pitchData.zone` |
| `pfx_x` | float | horizontal movement (inches) | — | not directly in GUMBO |
| `pfx_z` | float | vertical movement (inches) | — | not directly in GUMBO |
| `effective_speed` | float | perceived velocity | — | not in GUMBO |
| `bat_speed` | float | bat speed (mph, 2024+) | — | not in GUMBO |
| `swing_length` | float | swing length (ft, 2024+) | — | not in GUMBO |

### 2.4 Data Volume

| season_range | estimated_pitches | estimated_size | source |
|---|---|---|---|
| single season | ~700K pitches | ~800 MB CSV | checklist |
| 2015-2025 (11 seasons) | ~7M pitches | ~8 GB total | checklist |

### 2.5 Date Chunking Strategy

```python
import pandas as pd
from pybaseball import statcast

def pull_season(year: int) -> pd.DataFrame:
    """Pull full season in weekly chunks to avoid row limits."""
    start = f'{year}-03-20'  # spring training / opening day
    end = f'{year}-11-05'    # end of World Series
    chunks = []
    for week_start in pd.date_range(start, end, freq='7D'):
        week_end = min(week_start + pd.Timedelta(days=6), pd.Timestamp(end))
        chunk = statcast(
            start_dt=week_start.strftime('%Y-%m-%d'),
            end_dt=week_end.strftime('%Y-%m-%d'),
        )
        chunks.append(chunk)
    return pd.concat(chunks, ignore_index=True)
```

---

## 3. Retrosheet (Historical Play-by-Play)

### 3.1 Data Source

| item | value |
|---|---|
| URL | https://www.retrosheet.org/game.htm |
| format | `.EVA` (AL) and `.EVN` (NL) event files per season |
| parser | Chadwick Bureau `cwevent` tool |
| parser repo | https://github.com/chadwickbureau/chadwick |
| verified | ☐ (download and parsing not yet tested) |

### 3.2 Installation

```bash
# Install Chadwick tools
# macOS: brew install chadwick
# Linux: build from source (https://github.com/chadwickbureau/chadwick)
# Windows: download pre-built binaries or use WSL

# Convert event files to CSV:
cwevent -y 2024 -f 0-96 2024*.EV* > events_2024.csv
```

### 3.3 Event File Schema (cwevent CSV Output)

Key fields for this system:

| column_index | field_name | type | description | maps_to |
|---|---|---|---|---|
| 0 | GAME_ID | string | unique game identifier | game linkage |
| 1 | VISITING_TEAM | string | away team code | — |
| 2 | INNING | int | current inning (1-9+) | MarkovState.inning |
| 3 | BATTING_TEAM | int | 0=visitors, 1=home | MarkovState.batting_team |
| 4 | OUTS | int | outs before event (0-2) | MarkovState.outs |
| 5 | BALLS | int | ball count | — |
| 6 | STRIKES | int | strike count | — |
| 7-9 | RUNNER_1B/2B/3B | string | runner ID or empty | MarkovState.bases |
| 10 | EVENT_TEXT | string | Retrosheet event code | PA outcome mapping |
| 11 | EVENT_TYPE | int | event type code (see below) | PA outcome mapping |
| 26 | BAT_ID | string | batter player ID | — |
| 27 | RES_PIT_ID | string | responsible pitcher ID | — |

### 3.4 EVENT_TYPE → PA Outcome Mapping

Map Retrosheet EVENT_TYPE codes to the 8-outcome set from baseball_model.md §2:

| event_type_code | retrosheet_name | pa_outcome | notes |
|---|---|---|---|
| 2 | Generic out | other_out | fielded out, no DP |
| 3 | Strikeout | strikeout | includes K+WP, K+PB |
| 4 | Stolen base | — | not a PA outcome |
| 5 | Defensive indifference | — | not a PA outcome |
| 6 | Caught stealing | — | not a PA outcome |
| 7 | Pickoff error | — | not a PA outcome |
| 8 | Pickoff | — | not a PA outcome |
| 9 | Wild pitch | — | not a PA outcome |
| 10 | Passed ball | — | not a PA outcome |
| 11 | Balk | — | not a PA outcome |
| 12 | Other advance (defensive) | — | not a PA outcome |
| 13 | Foul error | — | rare, treat as other_out |
| 14 | Walk | walk_hbp | includes IBB |
| 15 | Intentional walk | walk_hbp | |
| 16 | Hit by pitch | walk_hbp | |
| 17 | Interference | walk_hbp | rare |
| 18 | Error | other_out | reached on error; treat as out for transition matrix (batter did not earn the base) <!-- DECISION NEEDED: count as out or single? --> |
| 19 | Fielder's choice | other_out | |
| 20 | Single | single | |
| 21 | Double | double | |
| 22 | Triple | triple | |
| 23 | Home run | home_run | |
| 24 | Missing play | — | skip |

**Double play detection:** EVENT_TYPE=2 with DP flag in EVENT_TEXT → `double_play`. Check `DP_FL` field (column index varies; typically field 37).

### 3.5 Runner Advancement Data

Retrosheet EVENT_TEXT encodes explicit runner advancement:
```
Example: "S7/L7.3-H;1-3"
  S7    = single to left field
  3-H   = runner from 3B scored (Home)
  1-3   = runner from 1B advanced to 3B
```

This is the source for calibrating runner advancement probabilities in baseball_model.md §2.1 (e.g., `P_SCORE_FROM_2B_ON_SINGLE`).

### 3.6 Data Volume

| season_range | estimated_events_per_season | total | source |
|---|---|---|---|
| single season | ~191K | — | checklist |
| 2015-2024 (10 seasons) | — | ~1.9M rows | checklist |

---

## 4. FanGraphs Projections

### 4.1 Access Methods

| method | tool | verified | notes |
|---|---|---|---|
| pybaseball | `pitching_stats(2026, qual=0)` | ☐ | may include projection columns |
| Steamer REST | <!-- UNVERIFIED --> | ☐ | semi-public endpoint, check availability |
| manual CSV | https://www.fangraphs.com/projections | ☐ | fallback download |

### 4.2 Projection Systems

| system | provider | update_frequency | notes |
|---|---|---|---|
| ZiPS | Dan Szymborski / FanGraphs | pre-season + in-season updates | <!-- DECISION NEEDED: which system to use? --> |
| Steamer | Jared Cross / FanGraphs | pre-season + in-season updates | |
| Depth Charts | FanGraphs community | combines ZiPS + Steamer + playing time | recommended as default |

### 4.3 Key Projection Columns

| column | type | description | used_for |
|---|---|---|---|
| `Name` | string | player name | identification |
| `playerid` | int | FanGraphs player ID | cross-reference |
| `MLBAMID` | int | MLB Stats API player ID | join to GUMBO |
| `W` | int | projected wins | — |
| `ERA` | float | projected ERA | pitcher quality |
| `FIP` | float | projected FIP | pitcher quality (defense-independent) |
| `xFIP` | float | projected xFIP | pitcher quality (HR-regression) |
| `K/9` | float | projected K rate | transition matrix input |
| `BB/9` | float | projected BB rate | transition matrix input |
| `HR/9` | float | projected HR rate | transition matrix input |
| `wOBA` | float | projected wOBA (batters) | shrinkage target |
| `PA` | int | projected plate appearances | playing time |
| `IP` | float | projected innings pitched | playing time |

---

## 5. Kalshi Exchange API

### 5.1 Environments

| environment | base_url | purpose | verified |
|---|---|---|---|
| production | `https://trading-api.kalshi.com/trade-api/v2` | live trading | ☐ (account exists, key exposed — must regenerate) |
| sandbox | `https://demo-api.kalshi.co` | paper trading | ☐ <!-- UNVERIFIED --> |

### 5.2 Authentication

**Method:** RSA key-based JWT signing

```bash
# Step 1: Generate 4096-bit RSA key pair
openssl genrsa -out kalshi_key.pem 4096
openssl rsa -in kalshi_key.pem -pubout -out kalshi_key_pub.pem

# Step 2: Upload public key to Kalshi dashboard
# Step 3: Sign requests with private key → JWT in Authorization header
```

| step | status | notes |
|---|---|---|
| key generation | ☐ | old key compromised — **MUST regenerate** |
| public key upload | ☐ | via Kalshi dashboard |
| auth flow test | ☐ | JWT signing with private key |
| sandbox auth | ☐ | confirm sandbox accepts same auth flow |

### 5.3 REST Endpoints

| method | endpoint | purpose | verified |
|---|---|---|---|
| GET | `/markets?series_ticker=KXMLB` | list MLB game winner markets | ☐ |
| GET | `/markets/{ticker}` | single market details | ☐ |
| GET | `/markets/{ticker}/orderbook` | orderbook (bids/asks with depth) | ✅ (format documented) |
| GET | `/markets/trades` | public trade history (paginated) | ✅ (from docs) |
| POST | `/portfolio/orders` | place order (market or limit) | ☐ |
| GET | `/portfolio/orders` | list open orders | ☐ |
| DELETE | `/portfolio/orders/{order_id}` | cancel order | ☐ |
| GET | `/portfolio/positions` | open positions | ☐ |
| GET | `/portfolio/settlements` | settlement history | ☐ |

#### Rate Limits (from official docs, 2026-04-04)

| tier | reads/sec | writes/sec | qualification |
|---|---|---|---|
| Basic | 20 | 10 | signup completion |
| Advanced | 30 | 30 | form submission (free, generally responsive) |
| Premier | 100 | 100 | 3.75% monthly exchange volume + technical competency |
| Prime | 400 | 400 | 7.5% monthly exchange volume + competency |

- **Write operations:** CreateOrder, CancelOrder, AmendOrder, DecreaseOrder, BatchCreateOrders, BatchCancelOrders (each batch item = 1 txn, except batch cancel = 0.2 txn each)
- **Read operations:** everything else
- **Exceeded:** HTTP 429 response
- **WebSocket:** no separate rate limits documented

For Phase 0 orderbook logging at Basic tier (20 reads/sec):
- 15 games × 1 poll/5s = 3 req/sec → **well within Basic tier limits**
- Safe to increase to 1 poll/3s (5 req/sec) if needed

#### GET /markets?series_ticker=KXMLB

- **Purpose:** discover all MLB game winner markets for a given day
- **Request params:** `series_ticker=KXMLB`, optional `status=open`
- **Response:** <!-- NEEDS REAL RESPONSE — make test call after key regeneration -->

#### GET /markets/{ticker}/orderbook

- **Purpose:** current orderbook depth (bids/asks)
- **Request params:** `ticker` (string), `depth` (int, 0-100, default 0 = all levels)
- **Auth:** required (RSA-PSS headers)
- **Response format (fixed-point):**
```json
{
  "orderbook_fp": {
    "yes_dollars": [["0.5500", "100.00"], ["0.5400", "200.00"]],
    "no_dollars": [["0.4400", "150.00"], ["0.4300", "80.00"]]
  }
}
```

- Prices are **dollar strings** (e.g., `"0.5500"` = $0.55 = 55 cents)
- Quantities are **contract count strings** (e.g., `"100.00"` = 100 contracts)
- Only bids are returned per side. A YES bid at $X = NO ask at $(1.00 - X)
- YES ask price = 1.00 - highest NO bid price

⚠️ **Parsing note:** `kalshi_client.py:get_orderbook()` currently expects `[[price_cents_int, qty_int], ...]` format under keys `yes`/`bids`/`no`/`asks`. The actual API returns `orderbook_fp.yes_dollars`/`no_dollars` with string values. This parsing needs to be updated before live use.

#### GET /markets/trades

- **Purpose:** public trade history for any market (no auth required)
- **Request params:**
  - `ticker` (string, optional) — filter by market ticker
  - `limit` (int, 1-1000, default 100)
  - `cursor` (string) — pagination cursor for next page
  - `min_ts` (int) — unix timestamp lower bound
  - `max_ts` (int) — unix timestamp upper bound
- **Response:**
```json
{
  "trades": [
    {
      "trade_id": "TRD123",
      "ticker": "KXMLB-26APR06-NYY",
      "count_fp": "10.00",
      "yes_price_dollars": "0.560000",
      "no_price_dollars": "0.440000",
      "taker_side": "yes",
      "created_time": "2024-01-15T10:30:00Z"
    }
  ],
  "cursor": "next_page_cursor_string"
}
```

- **Auth:** not required (public endpoint)
- **Use case:** post-game trade history fetch for `trades_observed` table in orderbook logger
- **Pagination:** pass returned `cursor` as param to fetch next page

#### POST /portfolio/orders

- **Purpose:** place buy/sell order
- **Request body (expected):**
```json
{
  "ticker": "KXMLB-26MAR26-NYY",
  "action": "buy",
  "side": "yes",
  "type": "limit",
  "count": 10,
  "yes_price": 55,
  "expiration_ts": 1711500000
}
```
<!-- UNVERIFIED — field names and types need confirmation from API docs or test call -->
- **Response:** <!-- NEEDS REAL RESPONSE -->

#### DELETE /portfolio/orders/{order_id}

- **Purpose:** cancel an open order
- **Response:** <!-- NEEDS REAL RESPONSE -->

### 5.4 WebSocket

**Status: AVAILABLE** (documented 2026-04-04, from official Kalshi API docs)

| item | value | verified |
|---|---|---|
| endpoint (production) | `wss://api.elections.kalshi.com/trade-api/ws/v2` | ✅ (in `src/config.py:KALSHI_WS_URL`, used by `trade_monitor.py`) |
| endpoint (demo) | `wss://demo-api.kalshi.co/trade-api/ws/v2` | ☐ |
| authentication | RSA-PSS headers at handshake (same as REST) | ✅ (implemented in `trade_monitor.py:_build_auth_headers`) |
| signing message | `{timestamp_ms}GET/trade-api/ws/v2` | ✅ |
| heartbeat / keepalive | Standard WebSocket ping/pong (no Kalshi-specific protocol) | ✅ (`ping_interval=20, ping_timeout=10` in `trade_monitor.py`) |
| max subscriptions | not documented | ☐ |
| max connections | not documented | ☐ |
| reconnection | client-side exponential backoff (no server-side auto-reconnect) | ✅ |

#### Authentication Headers (WebSocket Handshake)

Same RSA-PSS as REST, but signing path is `/trade-api/ws/v2`:

```
KALSHI-ACCESS-KEY: {key_id}
KALSHI-ACCESS-SIGNATURE: base64(RSA-PSS-SHA256(timestamp_ms + "GET" + "/trade-api/ws/v2"))
KALSHI-ACCESS-TIMESTAMP: {timestamp_ms}
```

#### Available Channels

| channel | type | description | implemented |
|---|---|---|---|
| `orderbook_delta` | private | Full orderbook snapshots + incremental deltas | ☐ |
| `trade` | public | Every fill (trade_id, ticker, price, count, taker_side) | ✅ (`trade_monitor.py`) |
| `ticker` | public | BBO updates (best bid/ask, sizes, volume, OI) | ☐ |
| `fill` | private | Your order fills in real-time | ☐ |
| `market_lifecycle_v2` | public | Market open/close/settlement events | ☐ |
| `market_positions` | private | Your position updates | ☐ |

#### Subscription Message Format

```json
{
  "id": 1,
  "cmd": "subscribe",
  "params": {
    "channels": ["orderbook_delta"],
    "market_tickers": ["KXMLB-26APR06-NYY"]
  }
}
```

- Use `market_tickers` (array) for multi-ticker subscriptions
- Can also use `market_ticker` (single string)
- Use `update_subscription` with `add_markets`/`delete_markets` to modify live

#### Orderbook Snapshot Message (sent first on subscribe)

```json
{
  "type": "orderbook_snapshot",
  "sid": 2,
  "seq": 2,
  "msg": {
    "market_ticker": "KXMLB-26APR06-NYY",
    "market_id": "9b0f6b43-...",
    "yes_dollars_fp": [["0.0800", "300.00"], ["0.2200", "333.00"]],
    "no_dollars_fp": [["0.5400", "20.00"], ["0.5600", "146.00"]]
  }
}
```

- `yes_dollars_fp` / `no_dollars_fp`: arrays of `[price_dollars_str, quantity_str]`
- Prices are **dollar strings** (e.g., `"0.0800"` = 8 cents)
- Quantities are **contract count strings** (e.g., `"300.00"` = 300 contracts)
- `seq`: sequential message number for gap detection
- `sid`: subscription ID (constant for your subscription)

#### Orderbook Delta Message (incremental updates)

```json
{
  "type": "orderbook_delta",
  "sid": 2,
  "seq": 3,
  "msg": {
    "market_ticker": "KXMLB-26APR06-NYY",
    "market_id": "9b0f6b43-...",
    "price_dollars": "0.960",
    "delta_fp": "-54.00",
    "side": "yes",
    "ts": "2022-11-22T20:44:01Z"
  }
}
```

- `delta_fp`: fixed-point change in contracts (negative = removed from book)
- If quantity at a price level reaches zero, remove the level
- Must maintain local orderbook state by applying deltas to last snapshot
- **Gap detection:** if `seq` is not previous+1, re-subscribe to get fresh snapshot

#### Trade Stream Message

```json
{
  "type": "trade",
  "sid": 11,
  "msg": {
    "trade_id": "d91bc706-...",
    "market_ticker": "KXMLB-26APR06-NYY",
    "yes_price_dollars": "0.360",
    "no_price_dollars": "0.640",
    "count_fp": "136.00",
    "taker_side": "no",
    "ts": 1669149841
  }
}
```

- `count_fp`: fixed-point contract count string (⚠️ `trade_monitor.py` parses as `count` int — needs fix)
- `taker_side`: "yes" or "no"
- `ts`: unix timestamp (integer seconds)

#### Ticker Channel Message (lightweight BBO alternative)

```json
{
  "type": "ticker",
  "sid": 11,
  "msg": {
    "market_ticker": "KXMLB-26APR06-NYY",
    "price_dollars": "0.480",
    "yes_bid_dollars": "0.450",
    "yes_ask_dollars": "0.530",
    "yes_bid_size_fp": "300.00",
    "yes_ask_size_fp": "150.00",
    "last_trade_size_fp": "25.00",
    "volume_fp": "33896.00",
    "open_interest_fp": "20422.00",
    "ts": 1669149841
  }
}
```

Sent whenever any field changes. Suitable for BBO-only monitoring without maintaining full orderbook state.

#### Depth Units — VERIFIED from API docs

| unit | format | example | meaning |
|---|---|---|---|
| prices | dollar strings (fixed-point) | `"0.5500"` | $0.55 (55 cents) |
| quantities | contract count strings (fixed-point) | `"300.00"` | 300 contracts |

⚠️ **Breaking finding:** `kalshi_client.py:get_orderbook()` parses prices as `cents / 100` (integer division). The actual API returns dollar strings in `orderbook_fp.yes_dollars` / `no_dollars`. REST and WS formats differ from what the client expects. See §5.3 for correct REST format.

### 5.5 Market Structure

| attribute | value | source |
|---|---|---|
| contract type | game winner binary (YES/NO) | ✅ confirmed on live ST game |
| other contract types | **none** — no F5, run line, totals, props, inning-level | ✅ confirmed |
| price range | $0.01 to $0.99 | ✅ |
| settlement | $1.00 (win) or $0.00 (lose) | ✅ |
| ticker format example | `KXMLB-26MAR26-NYY` | ☐ <!-- UNVERIFIED — confirm regex pattern via API --> |
| observed spread (ST) | 4-6 cents | ✅ (Spring Training) |
| expected spread (regular) | 1-3 cents | estimate based on higher regular season liquidity |
| observed volume (ST) | $186,986 for single ST game | ✅ |

### 5.6 Fee Structure

| fee_type | formula | max | verified |
|---|---|---|---|
| taker fee | `0.07 * price * (1 - price)` | $0.0175 at P=0.50 | ✅ (from docs) |
| maker fee | $0 | — | ✅ (from docs) |
| deposit (bank transfer) | 0% | — | checklist |
| deposit (debit card) | 2% | — | checklist |

**Fee table:**

| price | taker_fee |
|---|---|
| $0.10 | $0.0063 |
| $0.20 | $0.0112 |
| $0.25 | $0.0131 |
| $0.30 | $0.0147 |
| $0.40 | $0.0168 |
| $0.50 | $0.0175 (max) |
| $0.60 | $0.0168 |
| $0.70 | $0.0147 |
| $0.75 | $0.0131 |
| $0.80 | $0.0112 |
| $0.90 | $0.0063 |

### 5.7 API Protocol Stack

| protocol | latency | purpose | verified |
|---|---|---|---|
| REST v2 | 50-200ms | order placement, market queries | ☐ (latency not measured) |
| WebSocket | sub-second | orderbook streaming, trade feed, ticker BBO | ✅ (documented, `trade` channel implemented in `trade_monitor.py`) |
| FIX 4.4 | ~5-10ms | institutional (not needed initially) | ☐ |

---

## 6. Cross-Reference Table

Maps external data fields to internal model fields. Internal model field names reference `baseball_model.md` and will be finalized in `architecture.md` §2 type contracts.

| source | external_path | internal_field | transform | verified |
|---|---|---|---|---|
| GUMBO | `pitchData.startSpeed` | `current_velocity` | direct (float mph) | ✅ |
| GUMBO | `pitchData.endSpeed` | `end_velocity` | direct (float mph) | ✅ |
| GUMBO | `pitchData.breaks.spinRate` | `current_spin_rate` | direct (int rpm) | ✅ |
| GUMBO | `pitchData.breaks.spinDirection` | `spin_direction` | direct (int degrees) | ✅ |
| GUMBO | `pitchData.coordinates.x` | `plate_x` | direct (float ft) | ✅ |
| GUMBO | `pitchData.coordinates.y` | `plate_z` | direct (float ft) | ✅ |
| GUMBO | `pitchData.coordinates.x0` | `release_x` | direct (float ft) | ⚠️ |
| GUMBO | `pitchData.coordinates.z0` | `release_z` | direct (float ft) | ⚠️ |
| GUMBO | `pitchData.zone` | `zone` | direct (int) | ✅ |
| GUMBO | `pitchData.extension` | `extension` | direct (float ft) | ⚠️ 92.6% |
| GUMBO | `details.type.code` | `pitch_type` | direct (string) | ✅ |
| GUMBO | `details.call.description` | `call` | map to "called_strike" / "ball" | ✅ |
| GUMBO | `matchup.pitcher.id` | `pitcher_id` | direct (int) | ✅ |
| GUMBO | `matchup.batter.id` | `batter_id` | direct (int) | ✅ |
| GUMBO | `atBatIndex` | `pa_index` | direct (int) | ✅ |
| GUMBO | `hitData.launchSpeed` | `exit_velocity` | direct (float mph) | ☐ |
| GUMBO | `hitData.launchAngle` | `launch_angle` | direct (float deg) | ☐ |
| GUMBO | `liveData.linescore.currentInning` | `inning` | direct (int) | ✅ |
| GUMBO | `liveData.linescore.isTopInning` | `is_top` | direct (bool) | ✅ |
| GUMBO | `liveData.linescore.outs` | `outs` | direct (int 0-2) | ✅ |
| GUMBO | `liveData.linescore.offense.first/second/third` | `runners` | `encode_bases()` → int 0-7 | ✅ |
| GUMBO | `liveData.linescore.teams.home.runs` | `score_home` | direct (int) | ✅ |
| GUMBO | `liveData.linescore.teams.away.runs` | `score_away` | direct (int) | ✅ |
| Statcast | `release_speed` | `velocity_reading` | same as GUMBO startSpeed | ☐ |
| Statcast | `release_spin_rate` | `spin_reading` | same as GUMBO spinRate | ☐ |
| Statcast | `release_pos_x` | `release_x` | same as GUMBO x0 | ☐ |
| Statcast | `release_pos_z` | `release_z` | same as GUMBO z0 | ☐ |
| Retrosheet | `EVENT_TYPE` | PA outcome | map via §3.4 table | ☐ |
| Kalshi | `orderbook_fp.yes_dollars[0][0]` | `best_bid` | dollar string → float (e.g., `"0.55"` → `0.55`) | ✅ (from docs) |
| Kalshi | `orderbook_fp.no_dollars[0][0]` | `best_ask` | `1.0 - float(no_price)` (NO bid → YES ask) | ✅ (from docs) |
| Kalshi | `orderbook_fp.*.[][1]` | `bid_size` / `ask_size` | contract count string → int (e.g., `"100.00"` → `100`) | ✅ (from docs) |

---

## 7. Data Storage Estimates

| data_type | storage | estimated_size | source |
|---|---|---|---|
| Retrosheet 2015-2024 | SQLite or PostgreSQL | ~500 MB | checklist |
| Statcast 2015-2025 | SQLite or PostgreSQL | ~8 GB | checklist |
| real-time game state | in-memory (dict or Redis) | <100 MB per active game | checklist |
| pre-computed WP tables | NumPy array (pickle or .npy) | ~2.6 MB per game | architecture.md |
| trade log | SQLite | <10 MB per season | checklist |
| in-game Kalshi price log | SQLite | <!-- NEEDS ESTIMATE --> | **must log from Opening Day** |

---

## 8. Remaining Verification Items

Ordered by priority:

| item | what_to_do | priority | blocking |
|---|---|---|---|
| Kalshi API key | revoke exposed key, regenerate RSA pair | 🚨 **CRITICAL** | all Kalshi API testing |
| release point (x0, z0) | run targeted GUMBO test checking `pitchData.coordinates.x0/z0` explicitly | high | fatigue Tier 3 |
| exit velocity / launch angle | monitor game with more batted balls, check `hitData` fields | high | run environment model |
| metaData.wait | check if field exists in GUMBO response | medium | polling interval |
| Kalshi sandbox URL | confirm `https://demo-api.kalshi.co` is active | medium | paper trading |
| Kalshi order schema | make test order in sandbox, capture real response | medium | executor implementation |
| Kalshi WebSocket | ✅ documented (2026-04-04); test live connection after key regen | low | real-time market data |
| Kalshi orderbook parsing | `kalshi_client.py` expects cents int, API returns dollar strings — **must fix before live use** | 🚨 **HIGH** | all orderbook reads |
| Kalshi trade monitor field | `trade_monitor.py` reads `count` int, API sends `count_fp` string — **must fix** | high | trade volatility detection |
| broadcast latency | compare API timestamps vs live TV to measure actual delay | low | latency characterization |
| post-game backfill comparison | compare `statcast_live_831547.csv` with post-game Statcast values | low | data consistency validation |
| pybaseball functions | test `statcast()`, `statcast_pitcher()`, `pitching_stats()` calls | low | historical data pipeline |
| Retrosheet download + parse | download .EVA/.EVN files, run `cwevent`, verify output | low | training data |

---

## 9. Resources

| resource | url |
|---|---|
| Kalshi API docs | https://trading-api.kalshi.com/trade-api/v2 |
| Kalshi sandbox (unconfirmed) | https://demo-api.kalshi.co |
| MLB Stats API | https://statsapi.mlb.com/api/v1.1/ |
| GUMBO docs (unofficial PDF) | https://bdata-research-blog-prod.s3.amazonaws.com/uploads/2019/03/GUMBOPDF3-29.pdf |
| Baseball Savant | https://baseballsavant.mlb.com |
| Baseball Savant CSV docs | https://baseballsavant.mlb.com/csv-docs |
| Retrosheet | https://www.retrosheet.org/game.htm |
| FanGraphs projections | https://www.fangraphs.com/projections |
| pybaseball | https://github.com/jldbc/pybaseball |
| MLB-StatsAPI wrapper | https://github.com/toddrob99/MLB-StatsAPI |
| Chadwick Bureau | https://github.com/chadwickbureau/chadwick |
| test data file | `statcast_live_831547.csv` — 271 pitches, TB @ PHI, 2026-03-23 |
