CREATE TABLE IF NOT EXISTS khrona_jobs (
    id VARCHAR(255) PRIMARY KEY,
    description TEXT,
    retry_policy_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS khrona_executions (
    id VARCHAR(36) PRIMARY KEY,
    job_id VARCHAR(255) REFERENCES khrona_jobs(id),
    status VARCHAR(50) NOT NULL,
    scheduled_at TIMESTAMP NOT NULL,
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    claimed_at TIMESTAMP NULL,
    claimed_by VARCHAR(255),
    attempt INT DEFAULT 0,
    error TEXT,
    payload_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_khrona_executions_status_scheduled ON khrona_executions(status, scheduled_at);
