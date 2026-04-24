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

-- Index creation can be tricky across DBs, using a simpler approach if possible
-- or just letting it fail if it already exists if the driver allows.
-- For MySQL 8.0.30+ and Postgres/H2 this works:
CREATE INDEX IF NOT EXISTS idx_khrona_executions_status_scheduled ON khrona_executions(status, scheduled_at);
