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
    fun heartbeatSql(): String
    fun isLockHeldSql(excludeId: Boolean = false): String
    fun resetExpiredExecutionsSql(): String
}

class PostgresDialect : JdbcDialect {
    override fun upsertJobSql(): String = """
        INSERT INTO khrona_jobs (id, definition_json)
        VALUES (?, ?)
        ON CONFLICT (id) DO UPDATE SET
            definition_json = EXCLUDED.definition_json
    """.trimIndent()

    override fun upsertExecutionSql(): String = """
        INSERT INTO khrona_executions (id, job_id, status, scheduled_at, attempt, payload_json, lock_key)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET
            status = EXCLUDED.status,
            scheduled_at = EXCLUDED.scheduled_at,
            attempt = EXCLUDED.attempt,
            payload_json = EXCLUDED.payload_json,
            lock_key = EXCLUDED.lock_key
    """.trimIndent()

    override fun claimExecutionSql(): String = """
        UPDATE khrona_executions
        SET status = 'CLAIMED', claimed_at = ?, claimed_by = ?, expires_at = ?, started_at = ?
        WHERE id = (
            SELECT id FROM khrona_executions
            WHERE id = ? 
            AND (status = 'PENDING' OR ((status = 'CLAIMED' OR status = 'RUNNING') AND expires_at < ?))
            FOR UPDATE SKIP LOCKED
        )
    """.trimIndent()

    override fun listEligibleExecutionsSql(): String = """
        SELECT * FROM khrona_executions 
        WHERE (status = 'PENDING' OR ((status = 'CLAIMED' OR status = 'RUNNING') AND expires_at < ?))
        AND scheduled_at <= ? 
        ORDER BY scheduled_at ASC
    """.trimIndent()

    override fun heartbeatSql(): String = """
        UPDATE khrona_executions SET expires_at = ? 
        WHERE id = ? AND (status = 'CLAIMED' OR status = 'RUNNING')
    """.trimIndent()

    override fun isLockHeldSql(excludeId: Boolean): String = """
        SELECT COUNT(*) FROM khrona_executions 
        WHERE lock_key = ? AND (status = 'CLAIMED' OR status = 'RUNNING') 
        AND (expires_at IS NULL OR expires_at > ?)
        ${if (excludeId) "AND id != ?" else ""}
    """.trimIndent()

    override fun resetExpiredExecutionsSql(): String = """
        UPDATE khrona_executions 
        SET status = 'PENDING', claimed_at = NULL, claimed_by = NULL, expires_at = NULL, started_at = NULL
        WHERE (status = 'CLAIMED' OR status = 'RUNNING') AND expires_at < ?
    """.trimIndent()
}

class H2Dialect : JdbcDialect {
    override fun upsertJobSql(): String = """
        MERGE INTO khrona_jobs (id, definition_json) KEY (id) VALUES (?, ?)
    """.trimIndent()

    override fun upsertExecutionSql(): String = """
        MERGE INTO khrona_executions (id, job_id, status, scheduled_at, attempt, payload_json, lock_key) KEY (id) VALUES (?, ?, ?, ?, ?, ?, ?)
    """.trimIndent()

    override fun claimExecutionSql(): String = """
        UPDATE khrona_executions
        SET status = 'CLAIMED', claimed_at = ?, claimed_by = ?, expires_at = ?, started_at = ?
        WHERE id = ? AND (status = 'PENDING' OR ((status = 'CLAIMED' OR status = 'RUNNING') AND expires_at < ?))
    """.trimIndent()

    override fun listEligibleExecutionsSql(): String = """
        SELECT * FROM khrona_executions 
        WHERE (status = 'PENDING' OR ((status = 'CLAIMED' OR status = 'RUNNING') AND expires_at < ?))
        AND scheduled_at <= ? 
        ORDER BY scheduled_at ASC
    """.trimIndent()

    override fun heartbeatSql(): String = """
        UPDATE khrona_executions SET expires_at = ? 
        WHERE id = ? AND (status = 'CLAIMED' OR status = 'RUNNING')
    """.trimIndent()

    override fun isLockHeldSql(excludeId: Boolean): String = """
        SELECT COUNT(*) FROM khrona_executions 
        WHERE lock_key = ? AND (status = 'CLAIMED' OR status = 'RUNNING') 
        AND (expires_at IS NULL OR expires_at > ?)
        ${if (excludeId) "AND id != ?" else ""}
    """.trimIndent()

    override fun resetExpiredExecutionsSql(): String = """
        UPDATE khrona_executions 
        SET status = 'PENDING', claimed_at = NULL, claimed_by = NULL, expires_at = NULL, started_at = NULL
        WHERE (status = 'CLAIMED' OR status = 'RUNNING') AND expires_at < ?
    """.trimIndent()
}

class MySqlDialect : JdbcDialect {
    override fun upsertJobSql(): String = """
        INSERT INTO khrona_jobs (id, definition_json)
        VALUES (?, ?)
        ON DUPLICATE KEY UPDATE
            definition_json = VALUES(definition_json)
    """.trimIndent()

    override fun upsertExecutionSql(): String = """
        INSERT INTO khrona_executions (id, job_id, status, scheduled_at, attempt, payload_json, lock_key)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            status = VALUES(status),
            scheduled_at = VALUES(scheduled_at),
            attempt = VALUES(attempt),
            payload_json = VALUES(payload_json),
            lock_key = VALUES(lock_key)
    """.trimIndent()

    override fun claimExecutionSql(): String = """
        UPDATE khrona_executions
        SET status = 'CLAIMED', claimed_at = ?, claimed_by = ?, expires_at = ?, started_at = ?
        WHERE id = (
            SELECT id FROM (
                SELECT id FROM khrona_executions
                WHERE id = ? 
                AND (status = 'PENDING' OR ((status = 'CLAIMED' OR status = 'RUNNING') AND expires_at < ?))
                FOR UPDATE SKIP LOCKED
            ) as t
        )
    """.trimIndent()

    override fun listEligibleExecutionsSql(): String = """
        SELECT * FROM khrona_executions 
        WHERE (status = 'PENDING' OR ((status = 'CLAIMED' OR status = 'RUNNING') AND expires_at < ?))
        AND scheduled_at <= ? 
        ORDER BY scheduled_at ASC
    """.trimIndent()

    override fun heartbeatSql(): String = """
        UPDATE khrona_executions SET expires_at = ? 
        WHERE id = ? AND (status = 'CLAIMED' OR status = 'RUNNING')
    """.trimIndent()

    override fun isLockHeldSql(excludeId: Boolean): String = """
        SELECT COUNT(*) FROM khrona_executions 
        WHERE lock_key = ? AND (status = 'CLAIMED' OR status = 'RUNNING') 
        AND (expires_at IS NULL OR expires_at > ?)
        ${if (excludeId) "AND id != ?" else ""}
    """.trimIndent()

    override fun resetExpiredExecutionsSql(): String = """
        UPDATE khrona_executions 
        SET status = 'PENDING', claimed_at = NULL, claimed_by = NULL, expires_at = NULL, started_at = NULL
        WHERE (status = 'CLAIMED' OR status = 'RUNNING') AND expires_at < ?
    """.trimIndent()
}

class OracleDialect : JdbcDialect {
    override fun upsertJobSql(): String = """
        MERGE INTO khrona_jobs t
        USING (SELECT ? id, ? definition_json FROM dual) s
        ON (t.id = s.id)
        WHEN MATCHED THEN
            UPDATE SET t.definition_json = s.definition_json
        WHEN NOT MATCHED THEN
            INSERT (id, definition_json) VALUES (s.id, s.definition_json)
    """.trimIndent()

    override fun upsertExecutionSql(): String = """
        MERGE INTO khrona_executions t
        USING (SELECT ? id, ? job_id, ? status, ? scheduled_at, ? attempt, ? payload_json, ? lock_key FROM dual) s
        ON (t.id = s.id)
        WHEN MATCHED THEN
            UPDATE SET t.status = s.status, t.scheduled_at = s.scheduled_at, t.attempt = s.attempt, t.payload_json = s.payload_json, t.lock_key = s.lock_key
        WHEN NOT MATCHED THEN
            INSERT (id, job_id, status, scheduled_at, attempt, payload_json, lock_key)
            VALUES (s.id, s.job_id, s.status, s.scheduled_at, s.attempt, s.payload_json, s.lock_key)
    """.trimIndent()

    override fun claimExecutionSql(): String = """
        UPDATE khrona_executions
        SET status = 'CLAIMED', claimed_at = ?, claimed_by = ?, expires_at = ?, started_at = ?
        WHERE id = ? AND (status = 'PENDING' OR ((status = 'CLAIMED' OR status = 'RUNNING') AND expires_at < ?))
    """.trimIndent()

    override fun listEligibleExecutionsSql(): String = """
        SELECT * FROM khrona_executions 
        WHERE (status = 'PENDING' OR ((status = 'CLAIMED' OR status = 'RUNNING') AND expires_at < ?))
        AND scheduled_at <= ? 
        ORDER BY scheduled_at ASC
    """.trimIndent()

    override fun heartbeatSql(): String = """
        UPDATE khrona_executions SET expires_at = ? 
        WHERE id = ? AND (status = 'CLAIMED' OR status = 'RUNNING')
    """.trimIndent()

    override fun isLockHeldSql(excludeId: Boolean): String = """
        SELECT COUNT(*) FROM khrona_executions 
        WHERE lock_key = ? AND (status = 'CLAIMED' OR status = 'RUNNING') 
        AND (expires_at IS NULL OR expires_at > ?)
        ${if (excludeId) "AND id != ?" else ""}
    """.trimIndent()

    override fun resetExpiredExecutionsSql(): String = """
        UPDATE khrona_executions 
        SET status = 'PENDING', claimed_at = NULL, claimed_by = NULL, expires_at = NULL, started_at = NULL
        WHERE (status = 'CLAIMED' OR status = 'RUNNING') AND expires_at < ?
    """.trimIndent()
}



