CREATE TABLE IF NOT EXISTS khrona_jobs (
    id VARCHAR(255) PRIMARY KEY,
    description TEXT,
    retry_policy_json TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS khrona_executions (
    id UUID PRIMARY KEY,
    job_id VARCHAR(255) REFERENCES khrona_jobs(id),
    status VARCHAR(50) NOT NULL,
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    claimed_at TIMESTAMP WITH TIME ZONE,
    claimed_by VARCHAR(255),
    attempt INT DEFAULT 0,
    error TEXT,
    payload_json TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_khrona_executions_status_scheduled ON khrona_executions(status, scheduled_at);
