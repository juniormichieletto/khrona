package io.khrona.store.redis

import io.khrona.core.CronTrigger
import io.khrona.core.ExecutionStatus
import io.khrona.core.IntervalTrigger
import io.khrona.core.JobDefinition
import io.khrona.core.JobExecution
import io.khrona.core.JobStore
import io.khrona.core.OneTimeTrigger
import io.khrona.core.Trigger
import io.lettuce.core.ClientOptions
import io.lettuce.core.RedisException
import io.lettuce.core.RedisURI
import io.lettuce.core.RedisClient
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.TimeoutOptions
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
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.UUID

data class RedisJobStoreConfig(
    val redisUri: String,
    val namespace: String = "khrona",
    val commandTimeout: Duration = Duration.ofSeconds(5),
    val autoReconnect: Boolean = true,
    val requestQueueSize: Int = 10_000,
    val shutdownQuietPeriod: Duration = Duration.ofMillis(100),
    val shutdownTimeout: Duration = Duration.ofSeconds(2)
) {
    init {
        require(redisUri.isNotBlank()) { "redisUri must not be blank" }
        require(namespace.isNotBlank()) { "namespace must not be blank" }
        require(!commandTimeout.isNegative && !commandTimeout.isZero) { "commandTimeout must be positive" }
        require(requestQueueSize > 0) { "requestQueueSize must be positive" }
        require(!shutdownQuietPeriod.isNegative) { "shutdownQuietPeriod must not be negative" }
        require(!shutdownTimeout.isNegative && !shutdownTimeout.isZero) { "shutdownTimeout must be positive" }
    }
}

class KhronaRedisOomException(message: String, cause: Throwable) : RuntimeException(message, cause)

class RedisJobStore private constructor(
    private val client: RedisClient,
    private val connection: StatefulRedisConnection<String, String>,
    namespace: String,
    private val ownsClient: Boolean,
    private val config: RedisJobStoreConfig? = null
) : JobStore, AutoCloseable {
    constructor(config: RedisJobStoreConfig) : this(
        client = createClient(config),
        namespace = config.namespace,
        ownsClient = true,
        config = config
    )

    constructor(redisUri: String, namespace: String = "khrona") : this(
        config = RedisJobStoreConfig(redisUri = redisUri, namespace = namespace)
    )

    constructor(client: RedisClient, namespace: String = "khrona") : this(
        client = client,
        namespace = namespace,
        ownsClient = false
    )

    private constructor(
        client: RedisClient,
        namespace: String,
        ownsClient: Boolean,
        config: RedisJobStoreConfig? = null
    ) : this(
        client = client,
        connection = client.connect(),
        namespace = namespace,
        ownsClient = ownsClient,
        config = config
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
        val completedAt = if (status.isTerminal()) Instant.now().toString() else current.completedAt
        val expiresAt = current.expiresAt?.let { Instant.parse(it) }
        commands.eval<Long>(
            UPDATE_EXECUTION_STATUS_SCRIPT,
            ScriptOutputType.INTEGER,
            arrayOf(keys.executions, keys.pending, keys.claimed, keys.running, keys.executionLocks(id.toString())),
            id.toString(),
            status.name,
            error ?: "",
            status.isTerminal().toString(),
            completedAt ?: "",
            current.scheduledAt.toScore().toString(),
            expiresAt?.toEpochMilli()?.toString() ?: "",
            expiresAt?.ttlFromNowMillis()?.toString() ?: "",
            keys.lockPrefix,
            keys.claimLockPrefix
        )
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
        val expiresAt = now.plus(leaseDuration)
        val claimed = commands.eval<Long>(
            CLAIM_EXECUTION_SCRIPT,
            ScriptOutputType.INTEGER,
            arrayOf(
                keys.executions,
                keys.pending,
                keys.claimed,
                keys.running,
                keys.claimLock(id.toString()),
                keys.executionLocks(id.toString())
            ),
            id.toString(),
            workerId,
            now.toString(),
            expiresAt.toString(),
            now.toEpochMilli().toString(),
            expiresAt.toEpochMilli().toString(),
            leaseDuration.toRedisTtlMillis().toString(),
            keys.lockPrefix
        )
        claimed == 1L
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
        commands.eval<List<String>>(
            SUPERSEDE_BY_LOCK_SCRIPT,
            ScriptOutputType.MULTI,
            arrayOf(keys.executions, keys.lock(lockKey), keys.claimed, keys.running),
            excludeExecutionId?.toString() ?: "",
            Instant.now().toString(),
            keys.executionLocksPrefix,
            keys.claimLockPrefix
        ).map { UUID.fromString(it) }
    }

    override fun close() {
        connection.close()
        if (ownsClient) {
            if (config != null) {
                client.shutdown(config.shutdownQuietPeriod, config.shutdownTimeout)
            } else {
                client.shutdown()
            }
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
            try {
                block()
            } catch (e: RedisException) {
                if (e.message?.contains("OOM", ignoreCase = true) == true) {
                    log.error("Redis command failed because Redis reported OOM", e)
                    throw KhronaRedisOomException("Redis command failed because Redis reported OOM", e)
                }
                log.error("Redis command failed", e)
                throw e
            }
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(RedisJobStore::class.java)

        private fun createClient(config: RedisJobStoreConfig): RedisClient {
            val redisUri = RedisURI.create(config.redisUri).apply {
                timeout = config.commandTimeout
            }
            return RedisClient.create(redisUri).also { client ->
                client.setOptions(
                    ClientOptions.builder()
                        .autoReconnect(config.autoReconnect)
                        .requestQueueSize(config.requestQueueSize)
                        .timeoutOptions(TimeoutOptions.enabled(config.commandTimeout))
                        .build()
                )
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

        private const val CLAIM_EXECUTION_SCRIPT = """
            local id = ARGV[1]
            local execution_json = redis.call('HGET', KEYS[1], id)
            if not execution_json then
                return 0
            end

            local execution = cjson.decode(execution_json)
            local status = execution['status'] or 'PENDING'
            local claimable = false

            if status == 'PENDING' then
                claimable = true
            elseif status == 'CLAIMED' then
                local score = redis.call('ZSCORE', KEYS[3], id)
                claimable = score ~= false and tonumber(score) <= tonumber(ARGV[5])
            elseif status == 'RUNNING' then
                local score = redis.call('ZSCORE', KEYS[4], id)
                claimable = score ~= false and tonumber(score) <= tonumber(ARGV[5])
            end

            if not claimable then
                return 0
            end

            redis.call('ZREM', KEYS[2], id)
            redis.call('ZREM', KEYS[3], id)
            redis.call('ZREM', KEYS[4], id)

            local lock_indexes = redis.call('SMEMBERS', KEYS[6])
            for _, lock_index in ipairs(lock_indexes) do
                redis.call('SREM', lock_index, id)
            end
            redis.call('DEL', KEYS[6])

            execution['status'] = 'CLAIMED'
            execution['workerId'] = ARGV[2]
            execution['startedAt'] = ARGV[3]
            execution['expiresAt'] = ARGV[4]
            redis.call('HSET', KEYS[1], id, cjson.encode(execution))
            redis.call('ZADD', KEYS[3], ARGV[6], id)
            redis.call('SET', KEYS[5], ARGV[2], 'PX', ARGV[7])

            local lock_key = execution['lockKey']
            if lock_key ~= nil and lock_key ~= cjson.null then
                local lock_index = ARGV[8] .. lock_key
                redis.call('SADD', lock_index, id)
                redis.call('SADD', KEYS[6], lock_index)
            end

            return 1
        """

        private const val SUPERSEDE_BY_LOCK_SCRIPT = """
            local superseded = {}
            local ids = redis.call('SMEMBERS', KEYS[2])

            for _, id in ipairs(ids) do
                local execution_json = redis.call('HGET', KEYS[1], id)
                if not execution_json then
                    redis.call('SREM', KEYS[2], id)
                elseif id ~= ARGV[1] then
                    local execution = cjson.decode(execution_json)
                    local status = execution['status'] or 'PENDING'
                    if status == 'CLAIMED' or status == 'RUNNING' then
                        execution['status'] = 'SUPERSEDED'
                        execution['completedAt'] = ARGV[2]
                        local execution_locks_key = ARGV[3] .. id
                        local lock_indexes = redis.call('SMEMBERS', execution_locks_key)
                        for _, lock_index in ipairs(lock_indexes) do
                            redis.call('SREM', lock_index, id)
                        end

                        redis.call('HSET', KEYS[1], id, cjson.encode(execution))
                        redis.call('ZREM', KEYS[3], id)
                        redis.call('ZREM', KEYS[4], id)
                        redis.call('DEL', execution_locks_key)
                        redis.call('DEL', ARGV[4] .. id)
                        table.insert(superseded, id)
                    end
                end
            end

            return superseded
        """

        private const val UPDATE_EXECUTION_STATUS_SCRIPT = """
            local id = ARGV[1]
            local execution_json = redis.call('HGET', KEYS[1], id)
            if not execution_json then
                return 0
            end

            local execution = cjson.decode(execution_json)

            redis.call('ZREM', KEYS[2], id)
            redis.call('ZREM', KEYS[3], id)
            redis.call('ZREM', KEYS[4], id)

            local lock_indexes = redis.call('SMEMBERS', KEYS[5])
            for _, lock_index in ipairs(lock_indexes) do
                redis.call('SREM', lock_index, id)
            end
            redis.call('DEL', KEYS[5])

            execution['status'] = ARGV[2]
            if ARGV[3] == '' then
                execution['error'] = cjson.null
            else
                execution['error'] = ARGV[3]
            end
            if ARGV[4] == 'true' and ARGV[5] ~= '' then
                execution['completedAt'] = ARGV[5]
            end

            redis.call('HSET', KEYS[1], id, cjson.encode(execution))

            if ARGV[2] == 'PENDING' then
                redis.call('ZADD', KEYS[2], ARGV[6], id)
                redis.call('DEL', ARGV[10] .. id)
            elseif ARGV[2] == 'CLAIMED' then
                if ARGV[7] ~= '' then
                    redis.call('ZADD', KEYS[3], ARGV[7], id)
                    redis.call('PEXPIRE', ARGV[10] .. id, ARGV[8])
                end
            elseif ARGV[2] == 'RUNNING' then
                if ARGV[7] ~= '' then
                    redis.call('ZADD', KEYS[4], ARGV[7], id)
                    redis.call('PEXPIRE', ARGV[10] .. id, ARGV[8])
                end
            else
                redis.call('DEL', ARGV[10] .. id)
            end

            local lock_key = execution['lockKey']
            if (ARGV[2] == 'CLAIMED' or ARGV[2] == 'RUNNING') and lock_key ~= nil and lock_key ~= cjson.null then
                local lock_index = ARGV[9] .. lock_key
                redis.call('SADD', lock_index, id)
                redis.call('SADD', KEYS[5], lock_index)
            end

            return 1
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
    val lockPrefix = "$namespace:locks:"
    val executionLocksPrefix = "$namespace:execution-locks:"
    val claimLockPrefix = "$namespace:claim-locks:"

    fun lock(lockKey: String) = "$lockPrefix$lockKey"
    fun executionLocks(id: String) = "$executionLocksPrefix$id"
    fun claimLock(id: String) = "$claimLockPrefix$id"
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
