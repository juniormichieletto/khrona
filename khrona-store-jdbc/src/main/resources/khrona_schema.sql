CREATE TABLE IF NOT EXISTS khrona_jobs (
    id VARCHAR(255) PRIMARY KEY,
    definition_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS khrona_executions (
    id VARCHAR(36) PRIMARY KEY,
    job_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    scheduled_at TIMESTAMP NOT NULL,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    claimed_at TIMESTAMP,
    claimed_by VARCHAR(255),
    expires_at TIMESTAMP,
    attempt INT DEFAULT 0,
    error TEXT,
    payload_json TEXT,
    lock_key VARCHAR(255),
    correlation_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_khrona_executions_status_scheduled ON khrona_executions(status, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_khrona_executions_lock_status_expires ON khrona_executions(lock_key, status, expires_at);
CREATE INDEX IF NOT EXISTS idx_khrona_executions_status_expires ON khrona_executions(status, expires_at);
