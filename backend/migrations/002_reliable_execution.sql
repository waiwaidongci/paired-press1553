-- Schema v2: reliable run acceptance and execution.
-- Idempotent on PostgreSQL; applied at backend/worker startup via src.config.database.migrate().
-- Existing rows keep their meaning: NULL request_key = legacy (no dedup),
-- claim_seq 0 + NULL lease = never claimed, so queued/running rows are re-claimable.

ALTER TABLE runs ADD COLUMN IF NOT EXISTS request_key VARCHAR(64);
ALTER TABLE runs ADD COLUMN IF NOT EXISTS request_hash VARCHAR(64);
ALTER TABLE runs ADD COLUMN IF NOT EXISTS claim_seq INTEGER NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS claimed_by VARCHAR(80);
ALTER TABLE runs ADD COLUMN IF NOT EXISTS lease_until DOUBLE PRECISION;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS finished_at VARCHAR(40);
CREATE UNIQUE INDEX IF NOT EXISTS runs_request_key_key ON runs (request_key);
