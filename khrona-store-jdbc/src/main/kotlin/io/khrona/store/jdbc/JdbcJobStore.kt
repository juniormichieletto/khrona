package io.khrona.store.jdbc

import io.khrona.core.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.encodeToString
import kotlinx.serialization.decodeFromString
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.util.*
import javax.sql.DataSource

class JdbcJobStore(private val dataSource: DataSource) : JobStore {

    private val json = Json { ignoreUnknownKeys = true }
    private val isPostgres: Boolean by lazy {
        dataSource.connection.use { it.metaData.databaseProductName.contains("PostgreSQL", ignoreCase = true) }
    }

    fun migrate() {
        val sql = this::class.java.getResource("/khrona_schema.sql")?.readText()
            ?: throw IllegalStateException("Schema file not found")
        
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(sql)
            }
        }
    }

    override suspend fun saveJob(job: JobDefinition) {
        dataSource.connection.use { conn ->
            if (isPostgres) {
                val sql = """
                    INSERT INTO khrona_jobs (id, description, retry_policy_json)
                    VALUES (?, ?, ?)
                    ON CONFLICT (id) DO UPDATE SET
                        description = EXCLUDED.description,
                        retry_policy_json = EXCLUDED.retry_policy_json
                """.trimIndent()
                conn.prepareStatement(sql).use { stmt ->
                    stmt.setString(1, job.id)
                    stmt.setString(2, job.description)
                    stmt.setString(3, json.encodeToString(job.retryPolicy))
                    stmt.executeUpdate()
                }
            } else {
                // H2 / Generic compatible UPSERT
                val existsSql = "SELECT 1 FROM khrona_jobs WHERE id = ?"
                val exists = conn.prepareStatement(existsSql).use { stmt ->
                    stmt.setString(1, job.id)
                    stmt.executeQuery().next()
                }
                
                if (exists) {
                    val sql = "UPDATE khrona_jobs SET description = ?, retry_policy_json = ? WHERE id = ?"
                    conn.prepareStatement(sql).use { stmt ->
                        stmt.setString(1, job.description)
                        stmt.setString(2, json.encodeToString(job.retryPolicy))
                        stmt.setString(3, job.id)
                        stmt.executeUpdate()
                    }
                } else {
                    val sql = "INSERT INTO khrona_jobs (id, description, retry_policy_json) VALUES (?, ?, ?)"
                    conn.prepareStatement(sql).use { stmt ->
                        stmt.setString(1, job.id)
                        stmt.setString(2, job.description)
                        stmt.setString(3, json.encodeToString(job.retryPolicy))
                        stmt.executeUpdate()
                    }
                }
            }
        }
    }

    override suspend fun getJob(jobId: String): JobDefinition? {
        dataSource.connection.use { conn ->
            val sql = "SELECT * FROM khrona_jobs WHERE id = ?"
            conn.prepareStatement(sql).use { stmt ->
                stmt.setString(1, jobId)
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    return JobDefinition(
                        id = rs.getString("id"),
                        description = rs.getString("description"),
                        handler = { },
                        trigger = IntervalTrigger(Duration.ZERO),
                        retryPolicy = json.decodeFromString(rs.getString("retry_policy_json"))
                    )
                }
            }
        }
        return null
    }

    override suspend fun listJobs(): List<JobDefinition> {
        val jobs = mutableListOf<JobDefinition>()
        dataSource.connection.use { conn ->
            val sql = "SELECT * FROM khrona_jobs"
            conn.prepareStatement(sql).use { stmt ->
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    jobs.add(JobDefinition(
                        id = rs.getString("id"),
                        description = rs.getString("description"),
                        handler = { },
                        trigger = IntervalTrigger(Duration.ZERO),
                        retryPolicy = json.decodeFromString(rs.getString("retry_policy_json"))
                    ))
                }
            }
        }
        return jobs
    }

    override suspend fun saveExecution(execution: JobExecution) {
        dataSource.connection.use { conn ->
            if (isPostgres) {
                val sql = """
                    INSERT INTO khrona_executions (id, job_id, status, scheduled_at, attempt, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO UPDATE SET
                        status = EXCLUDED.status,
                        scheduled_at = EXCLUDED.scheduled_at,
                        attempt = EXCLUDED.attempt,
                        payload_json = EXCLUDED.payload_json
                """.trimIndent()
                conn.prepareStatement(sql).use { stmt ->
                    stmt.setObject(1, execution.id)
                    stmt.setString(2, execution.jobId)
                    stmt.setString(3, execution.status.name)
                    stmt.setTimestamp(4, Timestamp.from(execution.scheduledAt))
                    stmt.setInt(5, execution.attempt)
                    stmt.setString(6, execution.payload?.toString())
                    stmt.executeUpdate()
                }
            } else {
                val existsSql = "SELECT 1 FROM khrona_executions WHERE id = ?"
                val exists = conn.prepareStatement(existsSql).use { stmt ->
                    stmt.setObject(1, execution.id)
                    stmt.executeQuery().next()
                }
                
                if (exists) {
                    val sql = "UPDATE khrona_executions SET status = ?, scheduled_at = ?, attempt = ?, payload_json = ? WHERE id = ?"
                    conn.prepareStatement(sql).use { stmt ->
                        stmt.setString(1, execution.status.name)
                        stmt.setTimestamp(2, Timestamp.from(execution.scheduledAt))
                        stmt.setInt(3, execution.attempt)
                        stmt.setString(4, execution.payload?.toString())
                        stmt.setObject(5, execution.id)
                        stmt.executeUpdate()
                    }
                } else {
                    val sql = "INSERT INTO khrona_executions (id, job_id, status, scheduled_at, attempt, payload_json) VALUES (?, ?, ?, ?, ?, ?)"
                    conn.prepareStatement(sql).use { stmt ->
                        stmt.setObject(1, execution.id)
                        stmt.setString(2, execution.jobId)
                        stmt.setString(3, execution.status.name)
                        stmt.setTimestamp(4, Timestamp.from(execution.scheduledAt))
                        stmt.setInt(5, execution.attempt)
                        stmt.setString(6, execution.payload?.toString())
                        stmt.executeUpdate()
                    }
                }
            }
        }
    }

    override suspend fun updateExecutionStatus(id: UUID, status: ExecutionStatus, error: String?) {
        dataSource.connection.use { conn ->
            val sql = "UPDATE khrona_executions SET status = ?, error = ?, completed_at = ? WHERE id = ?"
            conn.prepareStatement(sql).use { stmt ->
                stmt.setString(1, status.name)
                stmt.setString(2, error)
                stmt.setTimestamp(3, if (status == ExecutionStatus.SUCCESS || status == ExecutionStatus.FAILED || status == ExecutionStatus.DEAD_LETTERED) Timestamp.from(Instant.now()) else null)
                stmt.setObject(4, id)
                stmt.executeUpdate()
            }
        }
    }

    override suspend fun getExecution(id: UUID): JobExecution? {
        dataSource.connection.use { conn ->
            val sql = "SELECT * FROM khrona_executions WHERE id = ?"
            conn.prepareStatement(sql).use { stmt ->
                stmt.setObject(1, id)
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    return mapExecution(rs)
                }
            }
        }
        return null
    }

    override suspend fun listEligibleExecutions(now: Instant): List<JobExecution> {
        val executions = mutableListOf<JobExecution>()
        dataSource.connection.use { conn ->
            val sql = "SELECT * FROM khrona_executions WHERE status = 'PENDING' AND scheduled_at <= ? ORDER BY scheduled_at ASC"
            conn.prepareStatement(sql).use { stmt ->
                stmt.setTimestamp(1, Timestamp.from(now))
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    executions.add(mapExecution(rs))
                }
            }
        }
        return executions
    }

    override suspend fun claimExecution(id: UUID, workerId: String, leaseDuration: Duration): Boolean {
        dataSource.connection.use { conn ->
            val sql = """
                UPDATE khrona_executions
                SET status = 'CLAIMED', claimed_at = ?, claimed_by = ?
                WHERE id = ? AND status = 'PENDING'
            """.trimIndent()
            conn.prepareStatement(sql).use { stmt ->
                val now = Instant.now()
                stmt.setTimestamp(1, Timestamp.from(now))
                stmt.setString(2, workerId)
                stmt.setObject(3, id)
                return stmt.executeUpdate() > 0
            }
        }
    }

    private fun mapExecution(rs: ResultSet): JobExecution {
        return JobExecution(
            id = rs.getObject("id") as UUID,
            jobId = rs.getString("job_id"),
            status = ExecutionStatus.valueOf(rs.getString("status")),
            scheduledAt = rs.getTimestamp("scheduled_at").toInstant(),
            startedAt = rs.getTimestamp("started_at")?.toInstant(),
            completedAt = rs.getTimestamp("completed_at")?.toInstant(),
            attempt = rs.getInt("attempt"),
            error = rs.getString("error"),
            payload = rs.getString("payload_json")
        )
    }
}
