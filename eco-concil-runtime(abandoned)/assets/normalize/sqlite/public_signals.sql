PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS public_signals (
    signal_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    signal_kind TEXT NOT NULL,
    external_id TEXT,
    title TEXT,
    text TEXT,
    url TEXT,
    author_name TEXT,
    channel_name TEXT,
    language TEXT,
    query_text TEXT,
    published_at_utc TEXT,
    captured_at_utc TEXT NOT NULL,
    engagement_json TEXT NOT NULL CHECK (json_valid(engagement_json)),
    metadata_json TEXT NOT NULL CHECK (json_valid(metadata_json)),
    artifact_path TEXT NOT NULL,
    record_locator TEXT NOT NULL,
    sha256 TEXT,
    raw_json TEXT NOT NULL CHECK (json_valid(raw_json))
);

CREATE INDEX IF NOT EXISTS idx_public_signals_run_round_source
    ON public_signals(run_id, round_id, source_skill);

CREATE INDEX IF NOT EXISTS idx_public_signals_run_round_kind
    ON public_signals(run_id, round_id, signal_kind);

CREATE TABLE IF NOT EXISTS claim_candidates (
    claim_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    claim_type TEXT NOT NULL,
    priority INTEGER NOT NULL,
    summary TEXT NOT NULL,
    statement TEXT NOT NULL,
    source_signal_ids_json TEXT NOT NULL CHECK (json_valid(source_signal_ids_json)),
    claim_json TEXT NOT NULL CHECK (json_valid(claim_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_claim_candidates_run_round_type
    ON claim_candidates(run_id, round_id, claim_type);
