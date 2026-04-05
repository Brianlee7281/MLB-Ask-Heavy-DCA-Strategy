-- Orderbook logger schema (Phase 0).
-- Separate from main mlb_prediction.db — stored in data/orderbook_live.db.

CREATE TABLE IF NOT EXISTS game_sessions (
    game_pk          INTEGER PRIMARY KEY,
    date             TEXT NOT NULL,
    home_team        TEXT NOT NULL,
    away_team        TEXT NOT NULL,
    kalshi_ticker    TEXT,
    game_start_ts    REAL,
    game_end_ts      REAL,
    home_won         INTEGER,
    settlement_price REAL,
    total_snapshots  INTEGER DEFAULT 0,
    total_ask_heavy  INTEGER DEFAULT 0,
    monitoring_errors INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS orderbook_snapshots (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    game_pk          INTEGER NOT NULL,
    timestamp        REAL NOT NULL,

    -- Orderbook state
    best_bid         REAL NOT NULL,
    best_ask         REAL NOT NULL,
    mid              REAL NOT NULL,
    spread           REAL NOT NULL,
    bid_depth        INTEGER NOT NULL,
    ask_depth        INTEGER NOT NULL,
    depth_ratio      REAL NOT NULL,

    -- Full book (top levels, JSON)
    bid_levels       TEXT,
    ask_levels       TEXT,
    total_bid_depth  INTEGER,
    total_ask_depth  INTEGER,

    -- Game context (from GUMBO)
    inning           INTEGER,
    half_inning      TEXT,
    outs             INTEGER,
    home_score       INTEGER,
    away_score       INTEGER,
    runners_on       TEXT,
    current_pitcher_id INTEGER,

    -- Derived flags
    ask_heavy        INTEGER NOT NULL,
    home_favored     INTEGER NOT NULL,

    FOREIGN KEY (game_pk) REFERENCES game_sessions(game_pk)
);

CREATE INDEX IF NOT EXISTS idx_ob_game_ts ON orderbook_snapshots(game_pk, timestamp);
CREATE INDEX IF NOT EXISTS idx_ob_ask_heavy ON orderbook_snapshots(ask_heavy, timestamp);

CREATE TABLE IF NOT EXISTS ask_heavy_signals (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    game_pk          INTEGER NOT NULL,
    onset_ts         REAL NOT NULL,
    offset_ts        REAL,
    duration_seconds REAL,

    -- State at onset
    mid_at_onset     REAL NOT NULL,
    spread_at_onset  REAL NOT NULL,
    depth_ratio_at_onset REAL NOT NULL,
    bid_depth_at_onset   INTEGER NOT NULL,
    ask_depth_at_onset   INTEGER NOT NULL,
    home_favored     INTEGER NOT NULL,
    inning_at_onset  INTEGER,
    score_diff_at_onset INTEGER,

    -- Price evolution after onset
    mid_after_30s    REAL,
    mid_after_60s    REAL,
    mid_after_120s   REAL,
    mid_after_300s   REAL,
    mid_after_600s   REAL,

    -- Simulated fill tracking
    sim_fill_30s     INTEGER,
    sim_fill_60s     INTEGER,
    sim_fill_300s    INTEGER,
    sim_fill_price   REAL,
    sim_fill_time    REAL,

    -- Outcome (filled after settlement)
    home_won         INTEGER,
    theoretical_pnl  REAL,

    FOREIGN KEY (game_pk) REFERENCES game_sessions(game_pk)
);

CREATE INDEX IF NOT EXISTS idx_ah_game ON ask_heavy_signals(game_pk);

CREATE TABLE IF NOT EXISTS trades_observed (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    game_pk          INTEGER NOT NULL,
    timestamp        REAL NOT NULL,
    price            REAL NOT NULL,
    side             TEXT NOT NULL,
    quantity         INTEGER,

    best_bid_at_trade REAL,
    best_ask_at_trade REAL,
    depth_ratio_at_trade REAL,
    ask_heavy_at_trade INTEGER,

    FOREIGN KEY (game_pk) REFERENCES game_sessions(game_pk)
);

CREATE INDEX IF NOT EXISTS idx_trades_game_ts ON trades_observed(game_pk, timestamp);

-- Paper trading (DCA ask_heavy strategy)

CREATE TABLE IF NOT EXISTS paper_trades (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    game_pk           INTEGER NOT NULL,

    -- DCA summary
    n_entries         INTEGER NOT NULL DEFAULT 0,
    total_invested    REAL NOT NULL DEFAULT 0.0,
    game_cap          REAL NOT NULL,
    avg_entry_mid     REAL,
    first_entry_mid   REAL,
    first_entry_ts    REAL,
    last_entry_mid    REAL,
    last_entry_ts     REAL,
    min_entry_mid     REAL,
    max_entry_mid     REAL,

    -- Entry conditions
    avg_depth_ratio   REAL,
    avg_spread        REAL,
    avg_inning        REAL,

    -- Fill tracking
    n_sim_filled      INTEGER DEFAULT 0,
    avg_fill_time     REAL,
    fill_rate         REAL,

    -- Settlement (filled post-game)
    home_won          INTEGER,
    pnl_per_dollar    REAL,
    game_pnl          REAL,

    -- Status
    status            TEXT DEFAULT 'active',
    created_at        REAL NOT NULL,
    settled_at        REAL,

    FOREIGN KEY (game_pk) REFERENCES game_sessions(game_pk)
);

CREATE INDEX IF NOT EXISTS idx_pt_game ON paper_trades(game_pk);
CREATE INDEX IF NOT EXISTS idx_pt_status ON paper_trades(status);

CREATE TABLE IF NOT EXISTS paper_entries (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id          INTEGER NOT NULL,
    game_pk           INTEGER NOT NULL,
    entry_ts          REAL NOT NULL,
    entry_mid         REAL NOT NULL,
    entry_spread      REAL,
    entry_depth_ratio REAL,
    entry_amount      REAL NOT NULL,

    -- Sim fill
    sim_filled        INTEGER DEFAULT 0,
    sim_fill_price    REAL,
    sim_fill_ts       REAL,
    sim_fill_seconds  REAL,

    -- Game context at entry
    inning            INTEGER,
    home_score        INTEGER,
    away_score        INTEGER,

    FOREIGN KEY (trade_id) REFERENCES paper_trades(id),
    FOREIGN KEY (game_pk) REFERENCES game_sessions(game_pk)
);

CREATE INDEX IF NOT EXISTS idx_pe_trade ON paper_entries(trade_id);
