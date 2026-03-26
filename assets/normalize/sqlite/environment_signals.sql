PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS environment_signals (
    signal_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    signal_kind TEXT NOT NULL,
    metric TEXT NOT NULL,
    value REAL,
    unit TEXT NOT NULL,
    observed_at_utc TEXT,
    window_start_utc TEXT,
    window_end_utc TEXT,
    latitude REAL,
    longitude REAL,
    bbox_json TEXT CHECK (bbox_json IS NULL OR json_valid(bbox_json)),
    quality_flags_json TEXT NOT NULL CHECK (json_valid(quality_flags_json)),
    metadata_json TEXT NOT NULL CHECK (json_valid(metadata_json)),
    artifact_path TEXT NOT NULL,
    record_locator TEXT NOT NULL,
    sha256 TEXT,
    raw_json TEXT NOT NULL CHECK (json_valid(raw_json))
);

CREATE INDEX IF NOT EXISTS idx_environment_signals_run_round_source
    ON environment_signals(run_id, round_id, source_skill);

CREATE INDEX IF NOT EXISTS idx_environment_signals_run_round_metric
    ON environment_signals(run_id, round_id, metric);

CREATE TABLE IF NOT EXISTS observation_summaries (
    observation_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    observation_json TEXT NOT NULL CHECK (json_valid(observation_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_observation_summaries_run_round_metric
    ON observation_summaries(run_id, round_id, metric);
