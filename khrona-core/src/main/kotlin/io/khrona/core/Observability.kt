package io.khrona.core

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

enum class ScheduleReason {
    INITIAL_SEED,
    RECURRING_NEXT,
    RETRY,
    MANUAL_TRIGGER
}

enum class ExecutionOutcome {
    SUCCESS,
    FAILED_RETRY,
    DEAD_LETTERED,
    MISFIRED,
    SUPERSEDED,
    CANCELLED,
    SHUTDOWN_CHECKPOINT
}

enum class HeartbeatFailureReason {
    LOST_OWNERSHIP_OR_EXPIRED,
    STORE_ERROR,
    LEASE_BUDGET_EXCEEDED
}

fun interface ErrorDiagnosticFormatter {
    fun format(error: Throwable): String

    companion object {
        const val DEFAULT_MAX_LENGTH = 512

        fun typeName(error: Throwable): String =
            error.javaClass.simpleName.ifEmpty { error.javaClass.name }

        fun bounded(maxLength: Int = DEFAULT_MAX_LENGTH): ErrorDiagnosticFormatter {
            require(maxLength > 3) { "maxLength must be greater than 3" }
            return ErrorDiagnosticFormatter { error ->
                val type = typeName(error)
                val rawMessage = error.message?.trim()
                val formatted = if (rawMessage.isNullOrEmpty()) type else "$type: $rawMessage"
                if (formatted.length <= maxLength) {
                    formatted
                } else {
                    formatted.take(maxLength - 3) + "..."
                }
            }
        }

        val DEFAULT: ErrorDiagnosticFormatter = bounded(DEFAULT_MAX_LENGTH)
    }
}

interface KhronaObserver {
    suspend fun <T> aroundExecution(
        context: ExecutionContext,
        block: suspend () -> T
    ): T = block()

    fun onExecutionScheduled(jobId: String, reason: ScheduleReason) {}

    fun onExecutionStarted(context: ExecutionContext, lag: Duration) {}

    fun onExecutionCompleted(
        context: ExecutionContext,
        outcome: ExecutionOutcome,
        duration: Duration,
        errorType: String? = null
    ) {}

    fun onRetryScheduled(
        context: ExecutionContext,
        nextAttempt: Int,
        delay: Duration,
        errorType: String
    ) {}

    fun onHeartbeatSucceeded(context: ExecutionContext) {}

    fun onHeartbeatFailed(context: ExecutionContext, reason: HeartbeatFailureReason) {}

    fun onStaleExecutionsRecovered(count: Int) {}

    fun onLockContention(jobId: String, lockKey: String, policy: ConcurrencyPolicy) {}

    fun onDeadLetterCountSampled(count: Long) {}

    fun onRetentionCleanup(deletedCount: Int) {}

    fun onShutdownCompleted(duration: Duration, clean: Boolean, remainingActiveCount: Int) {}

    fun onStoreOperation(
        operation: String,
        duration: Duration,
        success: Boolean,
        errorType: String? = null
    ) {}

    companion object {
        val NO_OP: KhronaObserver = object : KhronaObserver {}
    }
}

class CompositeKhronaObserver(
    val observers: List<KhronaObserver>
) : KhronaObserver {

    private inline fun notifyAll(action: (KhronaObserver) -> Unit) {
        for (observer in observers) {
            safeNotify { action(observer) }
        }
    }

    override suspend fun <T> aroundExecution(
        context: ExecutionContext,
        block: suspend () -> T
    ): T {
        val chained = observers.foldRight(block) { observer, next ->
            { invokeAroundSafely(observer, context, next) }
        }
        return chained()
    }

    override fun onExecutionScheduled(jobId: String, reason: ScheduleReason) =
        notifyAll { it.onExecutionScheduled(jobId, reason) }

    override fun onExecutionStarted(context: ExecutionContext, lag: Duration) =
        notifyAll { it.onExecutionStarted(context, lag) }

    override fun onExecutionCompleted(
        context: ExecutionContext,
        outcome: ExecutionOutcome,
        duration: Duration,
        errorType: String?
    ) = notifyAll { it.onExecutionCompleted(context, outcome, duration, errorType) }

    override fun onRetryScheduled(
        context: ExecutionContext,
        nextAttempt: Int,
        delay: Duration,
        errorType: String
    ) = notifyAll { it.onRetryScheduled(context, nextAttempt, delay, errorType) }

    override fun onHeartbeatSucceeded(context: ExecutionContext) =
        notifyAll { it.onHeartbeatSucceeded(context) }

    override fun onHeartbeatFailed(context: ExecutionContext, reason: HeartbeatFailureReason) =
        notifyAll { it.onHeartbeatFailed(context, reason) }

    override fun onStaleExecutionsRecovered(count: Int) =
        notifyAll { it.onStaleExecutionsRecovered(count) }

    override fun onLockContention(jobId: String, lockKey: String, policy: ConcurrencyPolicy) =
        notifyAll { it.onLockContention(jobId, lockKey, policy) }

    override fun onDeadLetterCountSampled(count: Long) =
        notifyAll { it.onDeadLetterCountSampled(count) }

    override fun onRetentionCleanup(deletedCount: Int) =
        notifyAll { it.onRetentionCleanup(deletedCount) }

    override fun onShutdownCompleted(duration: Duration, clean: Boolean, remainingActiveCount: Int) =
        notifyAll { it.onShutdownCompleted(duration, clean, remainingActiveCount) }

    override fun onStoreOperation(
        operation: String,
        duration: Duration,
        success: Boolean,
        errorType: String?
    ) = notifyAll { it.onStoreOperation(operation, duration, success, errorType) }
}

private val observerLog = LoggerFactory.getLogger(KhronaObserver::class.java)

internal inline fun safeNotify(action: () -> Unit) {
    try {
        action()
    } catch (e: Exception) {
        observerLog.warn("KhronaObserver callback failed", e)
    }
}

internal suspend fun <T> invokeAroundSafely(
    observer: KhronaObserver,
    context: ExecutionContext,
    block: suspend () -> T
): T {
    val blockInvoked = AtomicBoolean(false)
    val blockOutcome = CompletableDeferred<Result<T>>()

    suspend fun executeGuarded(): T = try {
        val value = block()
        blockOutcome.complete(Result.success(value))
        value
    } catch (t: Throwable) {
        blockOutcome.complete(Result.failure(t))
        throw t
    }

    suspend fun runOnceOrAwait(): T =
        if (blockInvoked.compareAndSet(false, true)) {
            executeGuarded()
        } else {
            blockOutcome.await().getOrThrow()
        }

    val wrappedBlock: suspend () -> T = {
        if (!blockInvoked.compareAndSet(false, true)) {
            throw IllegalStateException("KhronaObserver.aroundExecution must not invoke handler block more than once")
        }
        executeGuarded()
    }

    try {
        observer.aroundExecution(context, wrappedBlock)
    } catch (t: Throwable) {
        if (blockInvoked.get()) {
            if (!blockOutcome.isCompleted && t is CancellationException && !currentCoroutineContext().isActive) {
                throw t
            }
            return blockOutcome.await().getOrThrow()
        }
        if (t is CancellationException && !currentCoroutineContext().isActive) throw t
        observerLog.warn("KhronaObserver.aroundExecution failed before invoking handler", t)
        return runOnceOrAwait()
    }

    if (!blockInvoked.get()) {
        observerLog.warn("KhronaObserver.aroundExecution returned without invoking handler; executing handler directly")
    }
    return runOnceOrAwait()
}

class ObservableJobStore(
    private val delegate: JobStore,
    private val observer: KhronaObserver
) : JobStore {

    private suspend fun <T> observe(operation: String, block: suspend () -> T): T {
        val startNanos = System.nanoTime()
        return try {
            val result = block()
            val duration = Duration.ofNanos((System.nanoTime() - startNanos).coerceAtLeast(0L))
            safeNotify { observer.onStoreOperation(operation, duration, success = true, errorType = null) }
            result
        } catch (ce: CancellationException) {
            throw ce
        } catch (e: Exception) {
            val duration = Duration.ofNanos((System.nanoTime() - startNanos).coerceAtLeast(0L))
            val errorType = ErrorDiagnosticFormatter.typeName(e)
            safeNotify { observer.onStoreOperation(operation, duration, success = false, errorType = errorType) }
            throw e
        }
    }

    override suspend fun saveJob(job: JobDefinition) =
        observe("saveJob") { delegate.saveJob(job) }

    override suspend fun getJob(jobId: String): JobDefinition? =
        observe("getJob") { delegate.getJob(jobId) }

    override suspend fun listJobs(): List<JobDefinition> =
        observe("listJobs") { delegate.listJobs() }

    override suspend fun saveExecution(execution: JobExecution) =
        observe("saveExecution") { delegate.saveExecution(execution) }

    override suspend fun seedInitialExecution(execution: JobExecution): Boolean =
        observe("seedInitialExecution") { delegate.seedInitialExecution(execution) }

    override suspend fun updateExecutionStatus(
        id: UUID,
        status: ExecutionStatus,
        error: String?,
        workerId: String?
    ): Boolean = observe("updateExecutionStatus") {
        delegate.updateExecutionStatus(id, status, error, workerId)
    }

    override suspend fun completeExecution(
        id: UUID,
        status: ExecutionStatus,
        error: String?,
        workerId: String?,
        nextExecution: JobExecution?
    ): Boolean = observe("completeExecution") {
        delegate.completeExecution(id, status, error, workerId, nextExecution)
    }

    override suspend fun getExecution(id: UUID): JobExecution? =
        observe("getExecution") { delegate.getExecution(id) }

    override suspend fun listExecutions(query: JobExecutionQuery): List<JobExecution> =
        observe("listExecutions") { delegate.listExecutions(query) }

    override suspend fun countExecutions(query: JobExecutionQuery): Long =
        observe("countExecutions") { delegate.countExecutions(query) }

    override suspend fun checkHealth(): StoreHealth =
        observe("checkHealth") { delegate.checkHealth() }

    override suspend fun listEligibleExecutions(now: Instant, limit: Int): List<JobExecution> =
        observe("listEligibleExecutions") { delegate.listEligibleExecutions(now, limit) }

    override suspend fun listEligibleExecutions(
        now: Instant,
        limit: Int,
        jobIds: Set<String>?
    ): List<JobExecution> = observe("listEligibleExecutions") {
        delegate.listEligibleExecutions(now, limit, jobIds)
    }

    override suspend fun claimExecution(id: UUID, workerId: String, leaseDuration: Duration): Boolean =
        observe("claimExecution") { delegate.claimExecution(id, workerId, leaseDuration) }

    override suspend fun heartbeat(id: UUID, workerId: String, leaseDuration: Duration): Boolean =
        observe("heartbeat") { delegate.heartbeat(id, workerId, leaseDuration) }

    override suspend fun isLockHeld(lockKey: String, excludeExecutionId: UUID?): Boolean =
        observe("isLockHeld") { delegate.isLockHeld(lockKey, excludeExecutionId) }

    override suspend fun resetExpiredExecutions(now: Instant): Int =
        observe("resetExpiredExecutions") { delegate.resetExpiredExecutions(now) }

    override suspend fun supersedeExecutionsByLockKey(lockKey: String, excludeExecutionId: UUID?): List<UUID> =
        observe("supersedeExecutionsByLockKey") { delegate.supersedeExecutionsByLockKey(lockKey, excludeExecutionId) }

    override suspend fun cleanupCompletedExecutions(
        before: Instant,
        statuses: Set<ExecutionStatus>,
        limit: Int
    ): Int = observe("cleanupCompletedExecutions") {
        delegate.cleanupCompletedExecutions(before, statuses, limit)
    }
}

fun JobStore.observedBy(observer: KhronaObserver): JobStore =
    ObservableJobStore(this, observer)
