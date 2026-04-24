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

class JdbcJobStore(
    private val dataSource: DataSource,
    private val dialect: JdbcDialect = resolveDialect(dataSource)
) : JobStore {

    private val json = Json { ignoreUnknownKeys = true }

    fun migrate() {
        var sql = this::class.java.getResource("/khrona_schema.sql")?.readText()
            ?: throw IllegalStateException("Schema file not found")
        
        if (dialect is OracleDialect) {
            sql = sql.replace("TEXT", "CLOB")
                .replace("IF NOT EXISTS", "") // Oracle 23c supports it but older versions don't, and sometimes it's picky
        }
        
        dataSource.connection.use { conn ->
            conn.autoCommit = false
            try {
                conn.createStatement().use { stmt ->
                    // Split by semicolon but be careful with potential semicolons in strings/etc.
                    // For our simple schema, splitting by semicolon is fine.
                    sql.split(";").filter { it.trim().isNotEmpty() }.forEach { statement ->
                        stmt.execute(statement)
                    }
                }
                conn.commit()
            } catch (e: Exception) {
                conn.rollback()
                throw e
            } finally {
                conn.autoCommit = true
            }
        }
    }

    override suspend fun saveJob(job: JobDefinition) {
        dataSource.connection.use { conn ->
            conn.prepareStatement(dialect.upsertJobSql()).use { stmt ->
                stmt.setString(1, job.id)
                stmt.setString(2, job.description)
                stmt.setString(3, json.encodeToString(job.retryPolicy))
                stmt.executeUpdate()
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
            conn.prepareStatement(dialect.upsertExecutionSql()).use { stmt ->
                stmt.setString(1, execution.id.toString())
                stmt.setString(2, execution.jobId)
                stmt.setString(3, execution.status.name)
                stmt.setTimestamp(4, Timestamp.from(execution.scheduledAt))
                stmt.setInt(5, execution.attempt)
                stmt.setString(6, execution.payload?.toString())
                stmt.executeUpdate()
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
                stmt.setString(4, id.toString())
                stmt.executeUpdate()
            }
        }
    }

    override suspend fun getExecution(id: UUID): JobExecution? {
        dataSource.connection.use { conn ->
            val sql = "SELECT * FROM khrona_executions WHERE id = ?"
            conn.prepareStatement(sql).use { stmt ->
                stmt.setString(1, id.toString())
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
            conn.prepareStatement(dialect.listEligibleExecutionsSql()).use { stmt ->
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
            conn.prepareStatement(dialect.claimExecutionSql()).use { stmt ->
                val now = Instant.now()
                stmt.setTimestamp(1, Timestamp.from(now))
                stmt.setString(2, workerId)
                stmt.setString(3, id.toString())
                return stmt.executeUpdate() > 0
            }
        }
    }

    private fun mapExecution(rs: ResultSet): JobExecution {
        val idStr = rs.getString("id")
        return JobExecution(
            id = UUID.fromString(idStr),
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

    companion object {
        fun resolveDialect(dataSource: DataSource): JdbcDialect {
            return dataSource.connection.use { conn ->
                val name = conn.metaData.databaseProductName.lowercase()
                when {
                    name.contains("postgresql") -> PostgresDialect()
                    name.contains("mysql") || name.contains("mariadb") -> MySqlDialect()
                    name.contains("oracle") -> OracleDialect()
                    name.contains("h2") -> H2Dialect()
                    else -> H2Dialect() // Fallback to generic
                }
            }
        }
    }
}
