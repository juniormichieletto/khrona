package io.khrona.store.jdbc

import io.khrona.core.JobExecution
import java.sql.Connection
import java.time.Instant
import java.util.*

interface JdbcDialect {
    fun upsertJobSql(): String
    fun upsertExecutionSql(): String
    fun claimExecutionSql(): String
    fun listEligibleExecutionsSql(): String
    
    // Optional: add methods for parameter binding if they differ significantly
}

class PostgresDialect : JdbcDialect {
    override fun upsertJobSql(): String = """
        INSERT INTO khrona_jobs (id, description, retry_policy_json)
        VALUES (?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET
            description = EXCLUDED.description,
            retry_policy_json = EXCLUDED.retry_policy_json
    """.trimIndent()

    override fun upsertExecutionSql(): String = """
        INSERT INTO khrona_executions (id, job_id, status, scheduled_at, attempt, payload_json)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET
            status = EXCLUDED.status,
            scheduled_at = EXCLUDED.scheduled_at,
            attempt = EXCLUDED.attempt,
            payload_json = EXCLUDED.payload_json
    """.trimIndent()

    override fun claimExecutionSql(): String = """
        UPDATE khrona_executions
        SET status = 'CLAIMED', claimed_at = ?, claimed_by = ?
        WHERE id = (
            SELECT id FROM khrona_executions
            WHERE id = ? AND status = 'PENDING'
            FOR UPDATE SKIP LOCKED
        )
    """.trimIndent()

    override fun listEligibleExecutionsSql(): String = 
        "SELECT * FROM khrona_executions WHERE status = 'PENDING' AND scheduled_at <= ? ORDER BY scheduled_at ASC"
}

class H2Dialect : JdbcDialect {
    override fun upsertJobSql(): String = """
        MERGE INTO khrona_jobs (id, description, retry_policy_json) KEY (id) VALUES (?, ?, ?)
    """.trimIndent()

    override fun upsertExecutionSql(): String = """
        MERGE INTO khrona_executions (id, job_id, status, scheduled_at, attempt, payload_json) KEY (id) VALUES (?, ?, ?, ?, ?, ?)
    """.trimIndent()

    override fun claimExecutionSql(): String = """
        UPDATE khrona_executions
        SET status = 'CLAIMED', claimed_at = ?, claimed_by = ?
        WHERE id = ? AND status = 'PENDING'
    """.trimIndent()

    override fun listEligibleExecutionsSql(): String = 
        "SELECT * FROM khrona_executions WHERE status = 'PENDING' AND scheduled_at <= ? ORDER BY scheduled_at ASC"
}

class MySqlDialect : JdbcDialect {
    override fun upsertJobSql(): String = """
        INSERT INTO khrona_jobs (id, description, retry_policy_json)
        VALUES (?, ?, ?)
        ON DUPLICATE KEY UPDATE
            description = VALUES(description),
            retry_policy_json = VALUES(retry_policy_json)
    """.trimIndent()

    override fun upsertExecutionSql(): String = """
        INSERT INTO khrona_executions (id, job_id, status, scheduled_at, attempt, payload_json)
        VALUES (?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            status = VALUES(status),
            scheduled_at = VALUES(scheduled_at),
            attempt = VALUES(attempt),
            payload_json = VALUES(payload_json)
    """.trimIndent()

    override fun claimExecutionSql(): String = """
        UPDATE khrona_executions
        SET status = 'CLAIMED', claimed_at = ?, claimed_by = ?
        WHERE id = (
            SELECT id FROM (
                SELECT id FROM khrona_executions
                WHERE id = ? AND status = 'PENDING'
                FOR UPDATE SKIP LOCKED
            ) as t
        )
    """.trimIndent()

    override fun listEligibleExecutionsSql(): String = 
        "SELECT * FROM khrona_executions WHERE status = 'PENDING' AND scheduled_at <= ? ORDER BY scheduled_at ASC"
}

class OracleDialect : JdbcDialect {
    override fun upsertJobSql(): String = """
        MERGE INTO khrona_jobs t
        USING (SELECT ? id, ? description, ? retry_policy_json FROM dual) s
        ON (t.id = s.id)
        WHEN MATCHED THEN
            UPDATE SET t.description = s.description, t.retry_policy_json = s.retry_policy_json
        WHEN NOT MATCHED THEN
            INSERT (id, description, retry_policy_json) VALUES (s.id, s.description, s.retry_policy_json)
    """.trimIndent()

    override fun upsertExecutionSql(): String = """
        MERGE INTO khrona_executions t
        USING (SELECT ? id, ? job_id, ? status, ? scheduled_at, ? attempt, ? payload_json FROM dual) s
        ON (t.id = s.id)
        WHEN MATCHED THEN
            UPDATE SET t.status = s.status, t.scheduled_at = s.scheduled_at, t.attempt = s.attempt, t.payload_json = s.payload_json
        WHEN NOT MATCHED THEN
            INSERT (id, job_id, status, scheduled_at, attempt, payload_json)
            VALUES (s.id, s.job_id, s.status, s.scheduled_at, s.attempt, s.payload_json)
    """.trimIndent()

    override fun claimExecutionSql(): String = """
        UPDATE khrona_executions
        SET status = 'CLAIMED', claimed_at = ?, claimed_by = ?
        WHERE id = (
            SELECT id FROM khrona_executions
            WHERE id = ? AND status = 'PENDING'
            FOR UPDATE SKIP LOCKED
        )
    """.trimIndent()

    override fun listEligibleExecutionsSql(): String = 
        "SELECT * FROM khrona_executions WHERE status = 'PENDING' AND scheduled_at <= ? ORDER BY scheduled_at ASC"
}


