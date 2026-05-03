package io.khrona.store.jdbc

import io.khrona.core.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.encodeToString
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.util.*
import javax.sql.DataSource

class JdbcJobStore(
    private val dataSource: DataSource,
    dialect: JdbcDialect? = null,
    private val dispatcher: kotlinx.coroutines.CoroutineDispatcher = kotlinx.coroutines.Dispatchers.IO
) : JobStore {
    private val log = LoggerFactory.getLogger(JdbcJobStore::class.java)
    
    private var _dialect: JdbcDialect? = dialect
    private val dialectMutex = Mutex()

    private suspend fun getDialect(): JdbcDialect {
        _dialect?.let { return it }
        return dialectMutex.withLock {
            _dialect ?: inJdbcContext {
                resolveDialect(dataSource)
            }.also { resolved ->
                _dialect = resolved
            }
        }
    }

    private val json = Json { 
        ignoreUnknownKeys = true 
        serializersModule = SerializersModule {
            polymorphic(Trigger::class) {
                subclass(IntervalTrigger::class)
                subclass(CronTrigger::class)
                subclass(OneTimeTrigger::class)
            }
        }
    }

    private suspend fun <T> inJdbcContext(block: suspend () -> T): T {
        return kotlinx.coroutines.withContext(dispatcher) {
            block()
        }
    }

    suspend fun migrate() = inJdbcContext {
        var sql = this::class.java.getResource("/khrona_schema.sql")?.readText()
            ?: throw IllegalStateException("Schema file not found")
        
        val dialect = getDialect()
        if (dialect is OracleDialect) {
            sql = sql.replace("TEXT", "CLOB")
                .replace("IF NOT EXISTS", "") 
        }

        if (dialect is MySqlDialect) {
            // MySQL doesn't support IF NOT EXISTS for indexes in a simple way
            sql = sql.replace("CREATE INDEX IF NOT EXISTS", "CREATE INDEX")
        }
        
        dataSource.connection.use { conn ->
            val originalAutoCommit = conn.autoCommit
            conn.autoCommit = false
            try {
                conn.createStatement().use { stmt ->
                    sql.split(";").forEach { part ->
                        val statement = part.trim()
                        if (statement.isNotEmpty()) {
                            try {
                                stmt.execute(statement)
                            } catch (e: Exception) {
                                if (isIgnorableMigrationError(statement, e)) {
                                    log.debug("Idempotent migration step already applied, skipping: {}", statement)
                                } else {
                                    throw e
                                }
                            }
                        }
                    }
                }
                conn.commit()
            } catch (e: Exception) {
                conn.rollback()
                throw e
            } finally {
                conn.autoCommit = originalAutoCommit
            }
        }
    }

    private fun isIgnorableMigrationError(statement: String, error: Exception): Boolean {
        val message = error.message.orEmpty()
        val isCreateIndex = statement.startsWith("CREATE INDEX", ignoreCase = true)
        val isCreateTable = statement.startsWith("CREATE TABLE", ignoreCase = true)
        return (isCreateIndex && (
            message.contains("already exists", ignoreCase = true) ||
                message.contains("Duplicate key name", ignoreCase = true)
            )) ||
            ((isCreateIndex || isCreateTable) && message.contains("ORA-00955", ignoreCase = true))
    }

    override suspend fun saveJob(job: JobDefinition): Unit = inJdbcContext {
        val dialect = getDialect()
        dataSource.connection.use { conn ->
            conn.prepareStatement(dialect.upsertJobSql()).use { stmt ->
                stmt.setString(1, job.id)
                stmt.setString(2, json.encodeToString(job))
                stmt.executeUpdate()
            }
        }
    }

    override suspend fun getJob(jobId: String): JobDefinition? = inJdbcContext {
        dataSource.connection.use { conn ->
            val sql = "SELECT definition_json FROM khrona_jobs WHERE id = ?"
            conn.prepareStatement(sql).use { stmt ->
                stmt.setString(1, jobId)
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    return@inJdbcContext json.decodeFromString<JobDefinition>(rs.getString("definition_json"))
                }
            }
        }
        return@inJdbcContext null
    }

    override suspend fun listJobs(): List<JobDefinition> = inJdbcContext {
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
        return@inJdbcContext jobs
    }

    override suspend fun saveExecution(execution: JobExecution): Unit = inJdbcContext {
        val dialect = getDialect()
        dataSource.connection.use { conn ->
            conn.prepareStatement(dialect.upsertExecutionSql()).use { stmt ->
                stmt.setString(1, execution.id.toString())
                stmt.setString(2, execution.jobId)
                stmt.setString(3, execution.status.name)
                stmt.setTimestamp(4, Timestamp.from(execution.scheduledAt))
                stmt.setInt(5, execution.attempt)
                
                val payloadJson = execution.payload?.let { 
                    json.encodeToString(it.toJsonElement("$"))
                }
                stmt.setString(6, payloadJson)
                stmt.setString(7, execution.lockKey)
                stmt.setString(8, execution.correlationId)
                stmt.executeUpdate()
            }
        }
    }

    override suspend fun updateExecutionStatus(id: UUID, status: ExecutionStatus, error: String?): Unit = inJdbcContext {
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

    override suspend fun getExecution(id: UUID): JobExecution? = inJdbcContext {
        dataSource.connection.use { conn ->
            val sql = "SELECT * FROM khrona_executions WHERE id = ?"
            conn.prepareStatement(sql).use { stmt ->
                stmt.setString(1, id.toString())
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    return@inJdbcContext mapExecution(rs)
                }
            }
        }
        return@inJdbcContext null
    }

    override suspend fun listEligibleExecutions(now: Instant, limit: Int): List<JobExecution> = inJdbcContext {
        val dialect = getDialect()
        val executions = mutableListOf<JobExecution>()
        dataSource.connection.use { conn ->
            conn.prepareStatement(dialect.listEligibleExecutionsSql()).use { stmt ->
                stmt.setTimestamp(1, Timestamp.from(now))
                stmt.setTimestamp(2, Timestamp.from(now))
                stmt.setInt(3, limit)
                val rs = stmt.executeQuery()
                while (rs.next()) {
                    executions.add(mapExecution(rs))
                }
            }
        }
        return@inJdbcContext executions
    }

    override suspend fun claimExecution(id: UUID, workerId: String, leaseDuration: Duration): Boolean = inJdbcContext {
        val dialect = getDialect()
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
                return@inJdbcContext stmt.executeUpdate() > 0
            }
        }
    }

    override suspend fun heartbeat(id: UUID, leaseDuration: Duration): Boolean = inJdbcContext {
        val dialect = getDialect()
        dataSource.connection.use { conn ->
            conn.prepareStatement(dialect.heartbeatSql()).use { stmt ->
                val expiresAt = Instant.now().plus(leaseDuration)
                stmt.setTimestamp(1, Timestamp.from(expiresAt))
                stmt.setString(2, id.toString())
                return@inJdbcContext stmt.executeUpdate() > 0
            }
        }
    }

    override suspend fun isLockHeld(lockKey: String, excludeExecutionId: UUID?): Boolean = inJdbcContext {
        val dialect = getDialect()
        dataSource.connection.use { conn ->
            val sql = dialect.isLockHeldSql(excludeExecutionId != null)
            conn.prepareStatement(sql).use { stmt ->
                stmt.setString(1, lockKey)
                stmt.setTimestamp(2, Timestamp.from(Instant.now()))
                if (excludeExecutionId != null) {
                    stmt.setString(3, excludeExecutionId.toString())
                }
                val rs = stmt.executeQuery()
                return@inJdbcContext rs.next() && rs.getInt(1) > 0
            }
        }
    }

    override suspend fun resetExpiredExecutions(now: Instant): Int = inJdbcContext {
        val dialect = getDialect()
        dataSource.connection.use { conn ->
            conn.prepareStatement(dialect.resetExpiredExecutionsSql()).use { stmt ->
                stmt.setTimestamp(1, Timestamp.from(now))
                return@inJdbcContext stmt.executeUpdate()
            }
        }
    }

    override suspend fun supersedeExecutionsByLockKey(lockKey: String, excludeExecutionId: UUID?): List<UUID> = inJdbcContext {
        val superseded = mutableListOf<UUID>()
        dataSource.connection.use { conn ->
            val originalAutoCommit = conn.autoCommit
            conn.autoCommit = false
            try {
                val selectSql = buildString {
                    append("SELECT id FROM khrona_executions WHERE lock_key = ? AND status IN ('CLAIMED', 'RUNNING')")
                    if (excludeExecutionId != null) append(" AND id != ?")
                    append(" FOR UPDATE")
                }
                conn.prepareStatement(selectSql).use { stmt ->
                    stmt.setString(1, lockKey)
                    if (excludeExecutionId != null) {
                        stmt.setString(2, excludeExecutionId.toString())
                    }
                    val rs = stmt.executeQuery()
                    while (rs.next()) {
                        superseded.add(UUID.fromString(rs.getString("id")))
                    }
                }

                if (superseded.isNotEmpty()) {
                    val placeholders = superseded.joinToString(",") { "?" }
                    val updateSql = "UPDATE khrona_executions SET status = ?, completed_at = ? WHERE id IN ($placeholders)"
                    conn.prepareStatement(updateSql).use { stmt ->
                        stmt.setString(1, ExecutionStatus.SUPERSEDED.name)
                        stmt.setTimestamp(2, Timestamp.from(Instant.now()))
                        superseded.forEachIndexed { index, id ->
                            stmt.setString(index + 3, id.toString())
                        }
                        stmt.executeUpdate()
                    }
                }

                conn.commit()
            } catch (e: Exception) {
                conn.rollback()
                throw e
            } finally {
                conn.autoCommit = originalAutoCommit
            }
        }
        return@inJdbcContext superseded
    }

    private fun Any?.toJsonElement(path: String): kotlinx.serialization.json.JsonElement {
        return when (this) {
            null -> kotlinx.serialization.json.JsonNull
            is Number -> {
                if (!this.toDouble().isFinite()) throw IllegalArgumentException("Unsupported numeric value at $path: $this")
                kotlinx.serialization.json.JsonPrimitive(this)
            }
            is Boolean -> kotlinx.serialization.json.JsonPrimitive(this)
            is String -> kotlinx.serialization.json.JsonPrimitive(this)
            is Map<*, *> -> {
                val map = this.map { 
                    val key = it.key as? String 
                        ?: throw IllegalArgumentException("Unsupported map key type at $path: ${it.key?.javaClass?.name ?: "null"} (only String keys are supported)")
                    key to it.value.toJsonElement("$path.$key") 
                }.toMap()
                kotlinx.serialization.json.JsonObject(map)
            }
            is Iterable<*> -> {
                val list = this.mapIndexed { index, e -> e.toJsonElement("$path[$index]") }
                kotlinx.serialization.json.JsonArray(list)
            }
            else -> throw IllegalArgumentException("Unsupported payload type at $path: ${this.javaClass.name}")
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
                throw IllegalStateException("Failed to parse payload_json for execution $idStr", e)
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
