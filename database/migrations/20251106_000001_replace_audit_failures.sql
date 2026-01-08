-- Replace audit_failures with an append-only audits table optimized for inserts.
DROP TABLE IF EXISTS audit_failures;

CREATE TABLE audits (
    hotkey TEXT PRIMARY KEY,
    failed_job JSONB NOT NULL,
    reference_job JSONB NOT NULL
)
WITH (
    fillfactor = 100,
    autovacuum_vacuum_insert_scale_factor = 0.2,
    autovacuum_analyze_scale_factor = 0.02
);

COMMENT ON TABLE audits IS 'Append-only audit snapshots keyed by hotkey. Records are never updated.';
