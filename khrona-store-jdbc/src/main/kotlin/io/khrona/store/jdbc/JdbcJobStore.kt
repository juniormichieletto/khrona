package io.khrona.store.jdbc

import io.khrona.core.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.encodeToString
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import org.slf4j.LoggerFactory
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
    private val log = LoggerFactory.getLogger(JdbcJobStore::class.java)

    private val json = Json { 
        ignoreUnknownKeys = true 
        serializersModule = SerializersModule {
            polymorphic(Trigger::class) {
                subclass(IntervalTrigger::class)
                subclass(CronTrigger::class)
            }
        }
    }

    fun migrate() {
        var sql = this::class.java.getResource("/khrona_schema.sql")?.readText()
            ?: throw IllegalStateException("Schema file not found")
        
        if (dialect is OracleDialect) {
            sql = sql.replace("TEXT", "CLOB")
                .replace("IF NOT EXISTS", "") 
        }

        if (dialect is MySqlDialect) {
            // MySQL doesn't support IF NOT EXISTS for indexes in a simple way
            sql = sql.replace("CREATE INDEX IF NOT EXISTS", "CREATE INDEX")
        }
        
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                sql.split(";").forEach { part ->
                    if (part.trim().isNotEmpty()) {
                        try {
                            stmt.execute(part)
                        } catch (e: Exception) {
                            if (part.contains("CREATE INDEX") && (e.message?.contains("already exists", ignoreCase = true) == true || e.message?.contains("Duplicate key name", ignoreCase = true) == true)) {
                                log.debug("Index already exists, skipping: {}", part)
                            } else {
                                log.warn("Migration step failed: {}", part, e)
                            }
                        }
                    }
                }
            }
        }
    }

    override suspend fun saveJob(job: JobDefinition) {
        dataSource.connection.use { conn ->
            conn.prepareStatement(dialect.upsertJobSql()).use { stmt ->
                stmt.setString(1, job.id)
                stmt.setString(2, json.encodeToString(job))
                stmt.executeUpdate()
            }
        }
    }

    override suspend fun getJob(jobId: String): JobDefinition? {
        dataSource.connection.use { conn ->
            val sql = "SELECT definition_json FROM khrona_jobs WHERE id = ?"
            conn.prepareStatement(sql).use { stmt ->
                stmt.setString(1, jobId)
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    return json.decodeFromString<JobDefinition>(rs.getString("definition_json"))
                }
            }
        }
        return null
    }

    override suspend fun listJobs(): List<JobDefinition> {
        val jobs = mutableListOf<JobDefinition>()
        dataSource.connection.use { conn ->
            val sql = "SELECT definition_json FROM khrona_jobs"
            conn.prepareStatement(sql).use { stmt ->
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    jobs.add(json.decodeFromString<JobDefinition>(rs.getString("definition_json")))
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
                
                val payloadJson = execution.payload?.let { 
                    try {
                        when (it) {
                            is String -> "\"$it\""
                            is Number, is Boolean -> it.toString()
                            is Map<*, *> -> {
                                val map = it.map { e -> e.key.toString() to e.value.toJsonElement() }.toMap()
                                json.encodeToString(kotlinx.serialization.json.JsonObject(map))
                            }
                            is Iterable<*> -> {
                                val list = it.map { e -> e.toJsonElement() }
                                json.encodeToString(kotlinx.serialization.json.JsonArray(list))
                            }
                            else -> json.encodeToString(it)
                        }
                    } catch (e: Exception) {
                        it.toString()
                    }
                }
                stmt.setString(6, payloadJson)
                stmt.setString(7, execution.lockKey)
                stmt.setString(8, execution.correlationId)
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
                val isTerminal = status == ExecutionStatus.SUCCESS || 
                                status == ExecutionStatus.FAILED || 
                                status == ExecutionStatus.DEAD_LETTERED || 
                                status == ExecutionStatus.MISFIRED ||
                                status == ExecutionStatus.SUPERSEDED
                stmt.setTimestamp(3, if (isTerminal) Timestamp.from(Instant.now()) else null)
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
                stmt.setTimestamp(2, Timestamp.from(now))
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
                val expiresAt = now.plus(leaseDuration)
                stmt.setTimestamp(1, Timestamp.from(now))
                stmt.setString(2, workerId)
                stmt.setTimestamp(3, Timestamp.from(expiresAt))
                stmt.setTimestamp(4, Timestamp.from(now))
                stmt.setString(5, id.toString())
                stmt.setTimestamp(6, Timestamp.from(now))
                return stmt.executeUpdate() > 0
            }
        }
    }

    override suspend fun heartbeat(id: UUID, leaseDuration: Duration): Boolean {
        dataSource.connection.use { conn ->
            conn.prepareStatement(dialect.heartbeatSql()).use { stmt ->
                val expiresAt = Instant.now().plus(leaseDuration)
                stmt.setTimestamp(1, Timestamp.from(expiresAt))
                stmt.setString(2, id.toString())
                return stmt.executeUpdate() > 0
            }
        }
    }

    override suspend fun isLockHeld(lockKey: String, excludeExecutionId: UUID?): Boolean {
        dataSource.connection.use { conn ->
            val sql = dialect.isLockHeldSql(excludeExecutionId != null)
            conn.prepareStatement(sql).use { stmt ->
                stmt.setString(1, lockKey)
                stmt.setTimestamp(2, Timestamp.from(Instant.now()))
                if (excludeExecutionId != null) {
                    stmt.setString(3, excludeExecutionId.toString())
                }
                val rs = stmt.executeQuery()
                return rs.next() && rs.getInt(1) > 0
            }
        }
    }

    override suspend fun resetExpiredExecutions(now: Instant): Int {
        dataSource.connection.use { conn ->
            conn.prepareStatement(dialect.resetExpiredExecutionsSql()).use { stmt ->
                stmt.setTimestamp(1, Timestamp.from(now))
                return stmt.executeUpdate()
            }
        }
    }

    override suspend fun supersedeExecutionsByLockKey(lockKey: String): List<UUID> {
        val superseded = mutableListOf<UUID>()
        dataSource.connection.use { conn ->
            // First find which ones we are going to update to return them
            val selectSql = "SELECT id FROM khrona_executions WHERE lock_key = ? AND status IN ('CLAIMED', 'RUNNING')"
            conn.prepareStatement(selectSql).use { stmt ->
                stmt.setString(1, lockKey)
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    superseded.add(UUID.fromString(rs.getString("id")))
                }
            }
            
            val updateSql = "UPDATE khrona_executions SET status = ?, completed_at = ? WHERE lock_key = ? AND status IN ('CLAIMED', 'RUNNING')"
            conn.prepareStatement(updateSql).use { stmt ->
                stmt.setString(1, ExecutionStatus.SUPERSEDED.name)
                stmt.setTimestamp(2, Timestamp.from(Instant.now()))
                stmt.setString(3, lockKey)
                stmt.executeUpdate()
            }
        }
        return superseded
    }

    private fun Any?.toJsonElement(): kotlinx.serialization.json.JsonElement {
        return when (this) {
            null -> kotlinx.serialization.json.JsonNull
            is Number -> kotlinx.serialization.json.JsonPrimitive(this)
            is Boolean -> kotlinx.serialization.json.JsonPrimitive(this)
            is String -> kotlinx.serialization.json.JsonPrimitive(this)
            is Map<*, *> -> {
                val map = this.map { it.key.toString() to it.value.toJsonElement() }.toMap()
                kotlinx.serialization.json.JsonObject(map)
            }
            is Iterable<*> -> {
                val list = this.map { it.toJsonElement() }
                kotlinx.serialization.json.JsonArray(list)
            }
            else -> kotlinx.serialization.json.JsonPrimitive(this.toString())
        }
    }

    private fun kotlinx.serialization.json.JsonElement.toAny(): Any? {
        return when (this) {
            is kotlinx.serialization.json.JsonNull -> null
            is kotlinx.serialization.json.JsonPrimitive -> {
                if (isString) content
                else if (content == "true" || content == "false") content.toBoolean()
                else content.toLongOrNull() ?: content.toDoubleOrNull() ?: content
            }
            is kotlinx.serialization.json.JsonObject -> mapValues { it.value.toAny() }
            is kotlinx.serialization.json.JsonArray -> map { it.toAny() }
        }
    }

    private fun mapExecution(rs: ResultSet): JobExecution {
        val idStr = rs.getString("id")
        val payloadJson = rs.getString("payload_json")
        val payload = payloadJson?.let {
            try {
                val element = json.parseToJsonElement(it)
                element.toAny()
            } catch (e: Exception) {
                it // Fallback to raw string
            }
        }
        
        return JobExecution(
            id = UUID.fromString(idStr),
            jobId = rs.getString("job_id"),
            status = ExecutionStatus.valueOf(rs.getString("status")),
            scheduledAt = rs.getTimestamp("scheduled_at").toInstant(),
            startedAt = rs.getTimestamp("started_at")?.toInstant(),
            completedAt = rs.getTimestamp("completed_at")?.toInstant(),
            expiresAt = rs.getTimestamp("expires_at")?.toInstant(),
            attempt = rs.getInt("attempt"),
            workerId = rs.getString("claimed_by"),
            lockKey = rs.getString("lock_key"),
            error = rs.getString("error"),
            payload = payload,
            correlationId = rs.getString("correlation_id")
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
                    else -> H2Dialect() 
                }
            }
        }
    }
}
