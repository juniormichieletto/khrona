package io.khrona.store.jdbc

interface JdbcDialect {
    fun upsertJobSql(): String
    fun upsertExecutionSql(): String
    fun claimExecutionSql(): String
    fun listEligibleExecutionsSql(): String

    /**
     * Returns the SQL UPDATE statement used to atomically extend an active execution lease.
     *
     * [JdbcJobStore.heartbeat] binds four positional `?` parameters in order:
     * 1. `expires_at` (`java.sql.Timestamp`) — the new lease expiration timestamp (`now + leaseDuration`).
     * 2. `id` (`String`) — the execution UUID string.
     * 3. `claimed_by` (`String`) — the `workerId` requesting the renewal.
     * 4. `now` (`java.sql.Timestamp`) — the current timestamp; the row must have `expires_at > ?` and status in (`CLAIMED`, `RUNNING`).
     */
    fun heartbeatSql(): String
    fun isLockHeldSql(excludeId: Boolean = false): String
    fun resetExpiredExecutionsSql(): String
    fun cleanupCompletedExecutionsSql(statusCount: Int): String
}

class PostgresDialect : JdbcDialect {
    override fun upsertJobSql(): String = """
        INSERT INTO khrona_jobs (id, definition_json)
        VALUES (?, ?)
        ON CONFLICT (id) DO UPDATE SET
            definition_json = EXCLUDED.definition_json
    """.trimIndent()

    override fun upsertExecutionSql(): String = """
        INSERT INTO khrona_executions (id, job_id, status, scheduled_at, attempt, payload_json, lock_key, correlation_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET
            status = EXCLUDED.status,
            scheduled_at = EXCLUDED.scheduled_at,
            attempt = EXCLUDED.attempt,
            payload_json = EXCLUDED.payload_json,
            lock_key = EXCLUDED.lock_key,
            correlation_id = EXCLUDED.correlation_id
        WHERE khrona_executions.status = 'PENDING' OR EXCLUDED.status <> 'PENDING'
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
        LIMIT ?
    """.trimIndent()

    override fun heartbeatSql(): String = """
        UPDATE khrona_executions SET expires_at = ?
        WHERE id = ? AND claimed_by = ? AND expires_at > ? AND (status = 'CLAIMED' OR status = 'RUNNING')
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

    override fun cleanupCompletedExecutionsSql(statusCount: Int): String {
        val placeholders = (1..statusCount).joinToString(",") { "?" }
        return """
            DELETE FROM khrona_executions
            WHERE id IN (
                SELECT id FROM khrona_executions
                WHERE status IN ($placeholders) AND completed_at < ?
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            )
        """.trimIndent()
    }
}

class H2Dialect : JdbcDialect {
    override fun upsertJobSql(): String = """
        MERGE INTO khrona_jobs (id, definition_json) KEY (id) VALUES (?, ?)
    """.trimIndent()

    override fun upsertExecutionSql(): String = """
        MERGE INTO khrona_executions t
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?)) s(id, job_id, status, scheduled_at, attempt, payload_json, lock_key, correlation_id)
        ON t.id = s.id
        WHEN MATCHED AND (t.status = 'PENDING' OR s.status <> 'PENDING') THEN
            UPDATE SET
                status = s.status,
                scheduled_at = s.scheduled_at,
                attempt = s.attempt,
                payload_json = s.payload_json,
                lock_key = s.lock_key,
                correlation_id = s.correlation_id
        WHEN NOT MATCHED THEN
            INSERT (id, job_id, status, scheduled_at, attempt, payload_json, lock_key, correlation_id)
            VALUES (s.id, s.job_id, s.status, s.scheduled_at, s.attempt, s.payload_json, s.lock_key, s.correlation_id)
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
        LIMIT ?
    """.trimIndent()

    override fun heartbeatSql(): String = """
        UPDATE khrona_executions SET expires_at = ?
        WHERE id = ? AND claimed_by = ? AND expires_at > ? AND (status = 'CLAIMED' OR status = 'RUNNING')
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

    override fun cleanupCompletedExecutionsSql(statusCount: Int): String {
        val placeholders = (1..statusCount).joinToString(",") { "?" }
        return """
            DELETE FROM khrona_executions
            WHERE id IN (
                SELECT id FROM khrona_executions
                WHERE status IN ($placeholders) AND completed_at < ?
                LIMIT ?
            )
        """.trimIndent()
    }
}

class MySqlDialect : JdbcDialect {
    override fun upsertJobSql(): String = """
        INSERT INTO khrona_jobs (id, definition_json)
        VALUES (?, ?)
        ON DUPLICATE KEY UPDATE
            definition_json = VALUES(definition_json)
    """.trimIndent()

    override fun upsertExecutionSql(): String = """
        INSERT INTO khrona_executions (id, job_id, status, scheduled_at, attempt, payload_json, lock_key, correlation_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            scheduled_at = IF(khrona_executions.status = 'PENDING' OR VALUES(status) <> 'PENDING', VALUES(scheduled_at), khrona_executions.scheduled_at),
            attempt = IF(khrona_executions.status = 'PENDING' OR VALUES(status) <> 'PENDING', VALUES(attempt), khrona_executions.attempt),
            payload_json = IF(khrona_executions.status = 'PENDING' OR VALUES(status) <> 'PENDING', VALUES(payload_json), khrona_executions.payload_json),
            lock_key = IF(khrona_executions.status = 'PENDING' OR VALUES(status) <> 'PENDING', VALUES(lock_key), khrona_executions.lock_key),
            correlation_id = IF(khrona_executions.status = 'PENDING' OR VALUES(status) <> 'PENDING', VALUES(correlation_id), khrona_executions.correlation_id),
            status = IF(khrona_executions.status = 'PENDING' OR VALUES(status) <> 'PENDING', VALUES(status), khrona_executions.status)
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
        LIMIT ?
    """.trimIndent()

    override fun heartbeatSql(): String = """
        UPDATE khrona_executions SET expires_at = ?
        WHERE id = ? AND claimed_by = ? AND expires_at > ? AND (status = 'CLAIMED' OR status = 'RUNNING')
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

    override fun cleanupCompletedExecutionsSql(statusCount: Int): String {
        val placeholders = (1..statusCount).joinToString(",") { "?" }
        return """
            DELETE khrona_executions FROM khrona_executions
            JOIN (
                SELECT id FROM khrona_executions
                WHERE status IN ($placeholders) AND completed_at < ?
                LIMIT ?
                FOR UPDATE SKIP LOCKED
            ) AS t ON khrona_executions.id = t.id
        """.trimIndent()
    }
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
        USING (SELECT ? id, ? job_id, ? status, ? scheduled_at, ? attempt, ? payload_json, ? lock_key, ? correlation_id FROM dual) s
        ON (t.id = s.id)
        WHEN MATCHED THEN
            UPDATE SET t.status = s.status, t.scheduled_at = s.scheduled_at, t.attempt = s.attempt, t.payload_json = s.payload_json, t.lock_key = s.lock_key, t.correlation_id = s.correlation_id
            WHERE t.status = 'PENDING' OR s.status <> 'PENDING'
        WHEN NOT MATCHED THEN
            INSERT (id, job_id, status, scheduled_at, attempt, payload_json, lock_key, correlation_id)
            VALUES (s.id, s.job_id, s.status, s.scheduled_at, s.attempt, s.payload_json, s.lock_key, s.correlation_id)
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
        FETCH FIRST ? ROWS ONLY
    """.trimIndent()

    override fun heartbeatSql(): String = """
        UPDATE khrona_executions SET expires_at = ?
        WHERE id = ? AND claimed_by = ? AND expires_at > ? AND (status = 'CLAIMED' OR status = 'RUNNING')
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

    override fun cleanupCompletedExecutionsSql(statusCount: Int): String {
        val placeholders = (1..statusCount).joinToString(",") { "?" }
        return """
            DELETE FROM khrona_executions
            WHERE id IN (
                SELECT id FROM khrona_executions
                WHERE status IN ($placeholders) AND completed_at < ?
                FETCH FIRST ? ROWS ONLY
            )
        """.trimIndent()
    }

    fun selectCompletedExecutionsForCleanupSql(statusCount: Int): String {
        val placeholders = (1..statusCount).joinToString(",") { "?" }
        return """
            SELECT id FROM khrona_executions
            WHERE status IN ($placeholders) AND completed_at < ?
            FOR UPDATE SKIP LOCKED
        """.trimIndent()
    }
}
