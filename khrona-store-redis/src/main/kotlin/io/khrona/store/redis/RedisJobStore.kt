package io.khrona.store.redis

import io.khrona.core.CronTrigger
import io.khrona.core.ExecutionStatus
import io.khrona.core.IntervalTrigger
import io.khrona.core.JobDefinition
import io.khrona.core.JobExecution
import io.khrona.core.JobStore
import io.khrona.core.OneTimeTrigger
import io.khrona.core.Trigger
import io.lettuce.core.RedisClient
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.SetArgs
import io.lettuce.core.api.StatefulRedisConnection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import java.time.Duration
import java.time.Instant
import java.util.UUID

class RedisJobStore private constructor(
    private val client: RedisClient,
    private val connection: StatefulRedisConnection<String, String>,
    namespace: String,
    private val ownsClient: Boolean
) : JobStore, AutoCloseable {
    constructor(redisUri: String, namespace: String = "khrona") : this(
        client = RedisClient.create(redisUri),
        namespace = namespace,
        ownsClient = true
    )

    constructor(client: RedisClient, namespace: String = "khrona") : this(
        client = client,
        namespace = namespace,
        ownsClient = false
    )

    private constructor(client: RedisClient, namespace: String, ownsClient: Boolean) : this(
        client = client,
        connection = client.connect(),
        namespace = namespace,
        ownsClient = ownsClient
    )

    private val keys = RedisKeys(namespace)
    private val commands = connection.sync()

    override suspend fun saveJob(job: JobDefinition): Unit = inRedisContext {
        commands.hset(keys.jobs, job.id, json.encodeToString(job))
    }

    override suspend fun getJob(jobId: String): JobDefinition? = inRedisContext {
        commands.hget(keys.jobs, jobId)?.let { json.decodeFromString<JobDefinition>(it) }
    }

    override suspend fun listJobs(): List<JobDefinition> = inRedisContext {
        commands.hvals(keys.jobs).map { json.decodeFromString<JobDefinition>(it) }
    }

    override suspend fun saveExecution(execution: JobExecution): Unit = inRedisContext {
        saveStoredExecution(execution.toStored())
    }

    override suspend fun updateExecutionStatus(id: UUID, status: ExecutionStatus, error: String?): Unit = inRedisContext {
        val current = getStoredExecution(id) ?: return@inRedisContext
        val updated = current.copy(
            status = status.name,
            error = error,
            completedAt = if (status.isTerminal()) Instant.now().toString() else current.completedAt
        )
        saveStoredExecution(updated)
    }

    override suspend fun getExecution(id: UUID): JobExecution? = inRedisContext {
        getStoredExecution(id)?.toExecution()
    }

    override suspend fun listEligibleExecutions(now: Instant, limit: Int): List<JobExecution> = inRedisContext {
        val ids = commands.eval<List<String>>(
            LIST_ELIGIBLE_SCRIPT,
            ScriptOutputType.MULTI,
            arrayOf(keys.pending, keys.claimed, keys.running),
            now.toScore(),
            limit.toString()
        )

        ids.mapNotNull { id -> getStoredExecution(UUID.fromString(id))?.toExecution() }
            .filter { execution ->
                val expiresAt = execution.expiresAt
                execution.scheduledAt <= now &&
                    (execution.status == ExecutionStatus.PENDING ||
                        ((execution.status == ExecutionStatus.CLAIMED || execution.status == ExecutionStatus.RUNNING) &&
                            expiresAt != null &&
                            expiresAt <= now))
            }
            .take(limit)
    }

    override suspend fun claimExecution(id: UUID, workerId: String, leaseDuration: Duration): Boolean = inRedisContext {
        val now = Instant.now()
        val current = getStoredExecution(id) ?: return@inRedisContext false
        val status = ExecutionStatus.valueOf(current.status)
        val expiresAt = current.expiresAt?.let { Instant.parse(it) }
        val claimable = status == ExecutionStatus.PENDING ||
            ((status == ExecutionStatus.CLAIMED || status == ExecutionStatus.RUNNING) &&
                expiresAt != null &&
                expiresAt <= now)

        if (!claimable) {
            return@inRedisContext false
        }

        val lockAcquired = commands.set(
            keys.claimLock(id.toString()),
            workerId,
            SetArgs.Builder.nx().px(leaseDuration.toRedisTtlMillis())
        ) != null
        if (!lockAcquired) {
            return@inRedisContext false
        }

        saveStoredExecution(
            current.copy(
                status = ExecutionStatus.CLAIMED.name,
                workerId = workerId,
                startedAt = now.toString(),
                expiresAt = now.plus(leaseDuration).toString()
            )
        )
        true
    }

    override suspend fun heartbeat(id: UUID, leaseDuration: Duration): Boolean = inRedisContext {
        val current = getStoredExecution(id) ?: return@inRedisContext false
        val status = ExecutionStatus.valueOf(current.status)
        if (status != ExecutionStatus.CLAIMED && status != ExecutionStatus.RUNNING) {
            return@inRedisContext false
        }

        saveStoredExecution(current.copy(expiresAt = Instant.now().plus(leaseDuration).toString()))
        commands.pexpire(keys.claimLock(id.toString()), leaseDuration.toRedisTtlMillis())
        true
    }

    override suspend fun isLockHeld(lockKey: String, excludeExecutionId: UUID?): Boolean = inRedisContext {
        val now = Instant.now()
        commands.smembers(keys.lock(lockKey)).any { id ->
            val executionId = UUID.fromString(id)
            if (executionId == excludeExecutionId) {
                false
            } else {
                val execution = getStoredExecution(executionId)?.toExecution()
                val expiresAt = execution?.expiresAt
                execution != null &&
                    (execution.status == ExecutionStatus.CLAIMED || execution.status == ExecutionStatus.RUNNING) &&
                    (expiresAt == null || expiresAt > now)
            }
        }
    }

    override suspend fun resetExpiredExecutions(now: Instant): Int = inRedisContext {
        val ids = commands.eval<List<String>>(
            LIST_EXPIRED_SCRIPT,
            ScriptOutputType.MULTI,
            arrayOf(keys.claimed, keys.running),
            now.toScore()
        )

        var count = 0
        ids.distinct().forEach { id ->
            val executionId = UUID.fromString(id)
            val current = getStoredExecution(executionId) ?: return@forEach
            val status = ExecutionStatus.valueOf(current.status)
            if ((status == ExecutionStatus.CLAIMED || status == ExecutionStatus.RUNNING) &&
                current.expiresAt?.let { Instant.parse(it) <= now } == true
            ) {
                saveStoredExecution(
                    current.copy(
                        status = ExecutionStatus.PENDING.name,
                        workerId = null,
                        startedAt = null,
                        expiresAt = null
                    )
                )
                count++
            }
        }
        count
    }

    override suspend fun supersedeExecutionsByLockKey(lockKey: String, excludeExecutionId: UUID?): List<UUID> = inRedisContext {
        val superseded = mutableListOf<UUID>()
        commands.smembers(keys.lock(lockKey)).forEach { id ->
            val executionId = UUID.fromString(id)
            if (executionId == excludeExecutionId) return@forEach

            val current = getStoredExecution(executionId) ?: return@forEach
            val status = ExecutionStatus.valueOf(current.status)
            if (status == ExecutionStatus.CLAIMED || status == ExecutionStatus.RUNNING) {
                saveStoredExecution(
                    current.copy(
                        status = ExecutionStatus.SUPERSEDED.name,
                        completedAt = Instant.now().toString()
                    )
                )
                superseded.add(executionId)
            }
        }
        superseded
    }

    override fun close() {
        connection.close()
        if (ownsClient) {
            client.shutdown()
        }
    }

    private fun saveStoredExecution(execution: StoredExecution) {
        commands.hset(keys.executions, execution.id, json.encodeToString(execution))
        removeFromIndexes(execution.id)
        addToIndexes(execution)
    }

    private fun getStoredExecution(id: UUID): StoredExecution? {
        return commands.hget(keys.executions, id.toString())?.let { json.decodeFromString<StoredExecution>(it) }
    }

    private fun removeFromIndexes(id: String) {
        commands.zrem(keys.pending, id)
        commands.zrem(keys.claimed, id)
        commands.zrem(keys.running, id)
        commands.smembers(keys.executionLocks(id)).forEach { lockKey ->
            commands.srem(lockKey, id)
        }
        commands.del(keys.executionLocks(id))
    }

    private fun addToIndexes(execution: StoredExecution) {
        when (ExecutionStatus.valueOf(execution.status)) {
            ExecutionStatus.PENDING -> commands.zadd(keys.pending, execution.scheduledAt.toScore(), execution.id)
            ExecutionStatus.CLAIMED -> execution.expiresAt?.let {
                commands.zadd(keys.claimed, it.toScore(), execution.id)
                commands.pexpire(keys.claimLock(execution.id), Instant.parse(it).ttlFromNowMillis())
            }
            ExecutionStatus.RUNNING -> execution.expiresAt?.let {
                commands.zadd(keys.running, it.toScore(), execution.id)
                commands.pexpire(keys.claimLock(execution.id), Instant.parse(it).ttlFromNowMillis())
            }
            ExecutionStatus.SUCCESS,
            ExecutionStatus.FAILED,
            ExecutionStatus.DEAD_LETTERED,
            ExecutionStatus.MISFIRED,
            ExecutionStatus.SUPERSEDED -> commands.del(keys.claimLock(execution.id))
        }
        if (ExecutionStatus.valueOf(execution.status) == ExecutionStatus.PENDING) {
            commands.del(keys.claimLock(execution.id))
        }
        addLockIndex(execution)
    }

    private fun addLockIndex(execution: StoredExecution) {
        val status = ExecutionStatus.valueOf(execution.status)
        if ((status == ExecutionStatus.CLAIMED || status == ExecutionStatus.RUNNING) && execution.lockKey != null) {
            val lockIndex = keys.lock(execution.lockKey)
            commands.sadd(lockIndex, execution.id)
            commands.sadd(keys.executionLocks(execution.id), lockIndex)
        }
    }

    private suspend fun <T> inRedisContext(block: () -> T): T {
        return withContext(Dispatchers.IO) {
            block()
        }
    }

    companion object {
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

        private const val LIST_ELIGIBLE_SCRIPT = """
            local ids = {}
            local seen = {}
            local limit = tonumber(ARGV[2])
            local function appendFrom(key)
                if #ids >= limit then return end
                local values = redis.call('ZRANGEBYSCORE', key, '-inf', ARGV[1], 'LIMIT', 0, limit)
                for _, id in ipairs(values) do
                    if not seen[id] and #ids < limit then
                        seen[id] = true
                        table.insert(ids, id)
                    end
                end
            end
            appendFrom(KEYS[1])
            appendFrom(KEYS[2])
            appendFrom(KEYS[3])
            return ids
        """

        private const val LIST_EXPIRED_SCRIPT = """
            local ids = {}
            local seen = {}
            for _, key in ipairs(KEYS) do
                local values = redis.call('ZRANGEBYSCORE', key, '-inf', ARGV[1])
                for _, id in ipairs(values) do
                    if not seen[id] then
                        seen[id] = true
                        table.insert(ids, id)
                    end
                end
            end
            return ids
        """
    }
}

@Serializable
private data class StoredExecution(
    val id: String,
    val jobId: String,
    val scheduledAt: String,
    val startedAt: String? = null,
    val completedAt: String? = null,
    val expiresAt: String? = null,
    val status: String = ExecutionStatus.PENDING.name,
    val attempt: Int = 0,
    val workerId: String? = null,
    val lockKey: String? = null,
    val error: String? = null,
    val payloadJson: String? = null,
    val correlationId: String? = id
)

private fun JobExecution.toStored(): StoredExecution {
    return StoredExecution(
        id = id.toString(),
        jobId = jobId,
        scheduledAt = scheduledAt.toString(),
        startedAt = startedAt?.toString(),
        completedAt = completedAt?.toString(),
        expiresAt = expiresAt?.toString(),
        status = status.name,
        attempt = attempt,
        workerId = workerId,
        lockKey = lockKey,
        error = error,
        payloadJson = payload?.let { RedisPayloadJson.encode(it) },
        correlationId = correlationId
    )
}

private fun StoredExecution.toExecution(): JobExecution {
    return JobExecution(
        id = UUID.fromString(id),
        jobId = jobId,
        scheduledAt = Instant.parse(scheduledAt),
        startedAt = startedAt?.let { Instant.parse(it) },
        completedAt = completedAt?.let { Instant.parse(it) },
        expiresAt = expiresAt?.let { Instant.parse(it) },
        status = ExecutionStatus.valueOf(status),
        attempt = attempt,
        workerId = workerId,
        lockKey = lockKey,
        error = error,
        payload = payloadJson?.let { RedisPayloadJson.decode(it) },
        correlationId = correlationId
    )
}

private data class RedisKeys(private val namespace: String) {
    val jobs = "$namespace:jobs"
    val executions = "$namespace:executions"
    val pending = "$namespace:pending"
    val claimed = "$namespace:claimed"
    val running = "$namespace:running"

    fun lock(lockKey: String) = "$namespace:locks:$lockKey"
    fun executionLocks(id: String) = "$namespace:execution-locks:$id"
    fun claimLock(id: String) = "$namespace:claim-locks:$id"
}

private object RedisPayloadJson {
    private val json = Json

    fun encode(payload: Any?): String = json.encodeToString(payload.toJsonElement("$"))

    fun decode(payloadJson: String): Any? = json.parseToJsonElement(payloadJson).toAny()

    private fun Any?.toJsonElement(path: String): JsonElement {
        return when (this) {
            null -> JsonNull
            is Number -> {
                if (!this.toDouble().isFinite()) throw IllegalArgumentException("Unsupported numeric value at $path: $this")
                JsonPrimitive(this)
            }
            is Boolean -> JsonPrimitive(this)
            is String -> JsonPrimitive(this)
            is Map<*, *> -> JsonObject(
                this.map {
                    val key = it.key as? String
                        ?: throw IllegalArgumentException("Unsupported map key type at $path: ${it.key?.javaClass?.name ?: "null"} (only String keys are supported)")
                    key to it.value.toJsonElement("$path.$key")
                }.toMap()
            )
            is Iterable<*> -> JsonArray(this.mapIndexed { index, value -> value.toJsonElement("$path[$index]") })
            else -> throw IllegalArgumentException("Unsupported payload type at $path: ${this.javaClass.name}")
        }
    }

    private fun JsonElement.toAny(): Any? {
        return when (this) {
            JsonNull -> null
            is JsonPrimitive -> {
                if (isString) {
                    content
                } else {
                    booleanOrNull ?: longOrNull ?: doubleOrNull ?: contentOrNull
                }
            }
            is JsonObject -> mapValues { it.value.toAny() }
            is JsonArray -> map { it.toAny() }
        }
    }
}

private fun ExecutionStatus.isTerminal(): Boolean {
    return this == ExecutionStatus.SUCCESS ||
        this == ExecutionStatus.FAILED ||
        this == ExecutionStatus.DEAD_LETTERED ||
        this == ExecutionStatus.MISFIRED ||
        this == ExecutionStatus.SUPERSEDED
}

private fun Instant.toScore(): String = toEpochMilli().toString()
private fun String.toScore(): Double = Instant.parse(this).toEpochMilli().toDouble()
private fun Duration.toRedisTtlMillis(): Long = toMillis().coerceAtLeast(1)
private fun Instant.ttlFromNowMillis(): Long = Duration.between(Instant.now(), this).toRedisTtlMillis()
