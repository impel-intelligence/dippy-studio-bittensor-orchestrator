-- Track failed audit jobs along with the payloads that produced mismatched outputs.
CREATE TABLE audit_failures (
    id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    audit_job_id UUID NOT NULL UNIQUE,
    target_job_id UUID,
    miner_hotkey TEXT,
    netuid INTEGER,
    network TEXT,
    audit_payload JSONB,
    audit_response_payload JSONB,
    target_payload JSONB,
    target_response_payload JSONB,
    audit_image_hash TEXT,
    target_image_hash TEXT
);

CREATE INDEX idx_audit_failures_created_at ON audit_failures (created_at);
CREATE INDEX idx_audit_failures_hotkey ON audit_failures (miner_hotkey);
