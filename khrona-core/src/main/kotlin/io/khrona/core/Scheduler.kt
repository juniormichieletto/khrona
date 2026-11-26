package io.khrona.core

import kotlinx.coroutines.*
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.toJavaDuration

class Scheduler(
    val config: KhronaConfig,
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob()),
    private val clock: Clock = Clock.systemUTC()
) {
    private val log = LoggerFactory.getLogger(Scheduler::class.java)
    private val store: JobStore
        get() = config.store ?: throw IllegalStateException("JobStore must be configured")
    private val handlerRegistry = HandlerRegistry()
    private data class LeaseDeadline(val expiresAt: Instant, val startedAtNanos: Long, val duration: Duration) {
        fun remaining(now: Instant): Duration = minOf(
            Duration.between(now, expiresAt),
            duration.minusNanos(System.nanoTime() - startedAtNanos)
        )
    }

    private val activeJobs = ConcurrentHashMap<UUID, Job>()
    private data class ActiveClaim(
        val jobId: String,
        val owner: String,
        val store: JobStore,
        val executionContext: ExecutionContext,
        val startedAtNanos: AtomicLong = AtomicLong(0L),
        val completionRecorded: AtomicBoolean = AtomicBoolean(false)
    )

    private val activeClaims = ConcurrentHashMap<UUID, ActiveClaim>()
    private val workerId = UUID.randomUUID().toString()
    private val heartbeatInterval: Duration
        get() = config.heartbeatInterval ?: config.executionLeaseDuration.dividedBy(2)
    @Volatile
    private var job: Job? = null
    @Volatile
    private var stopping = false
    @Volatile
    private var lastPollAt: Instant? = null
    @Volatile
    private var lastSuccessfulPollAt: Instant? = null
    private val consecutivePollFailures = AtomicInteger(0)
    @Volatile
    private var lastPollErrorType: String? = null
    internal var random: kotlin.random.Random = kotlin.random.Random.Default

    init {
        validateConfig()
        config.jobs.forEach {
            validateJob(it)
            handlerRegistry.register(it.id, it.handler)
        }
    }

    private fun validateConfig() {
        checkNotNull(config.store) { "JobStore must be configured" }
        config.validate()
    }

    fun start() {
        if (job != null) return
        stopping = false
        lastPollAt = null
        lastSuccessfulPollAt = null
        consecutivePollFailures.set(0)
        lastPollErrorType = null
        log.info("Starting Khrona Scheduler (workerId: $workerId, pollingInterval: ${config.pollingInterval})")

        val correlationId = MDC.get("correlationId")

        job = scope.launch {
            var jobsInitialized = false
            var lastRecovery = Instant.MIN
            var lastDeadLetterSample = Instant.MIN
            var nextCleanupAt = initialCleanupTime(Instant.now(clock))
            while (isActive) {
                try {
                    if (!jobsInitialized) {
                        // Register jobs from config into store (retried if store is down at startup)
                        config.jobs.forEach { persistJobDefinition(it, store) }
                        if (!isActive) break
                        config.jobs.forEach { jobDef ->
                            seedFirstExecution(jobDef, correlationId, store)
                        }
                        jobsInitialized = true
                        if (!isActive) break
                    }

                    val now = Instant.now(clock)
                    // Periodic recovery of stale executions
                    if (Duration.between(lastRecovery, now) > Duration.ofMinutes(1)) {
                        val recovered = store.resetExpiredExecutions(now)
                        if (recovered > 0) {
                            log.info("Recovered $recovered stale executions")
                            safeNotify { config.observer.onStaleExecutionsRecovered(recovered) }
                        }
                        lastRecovery = now
                    }

                    // Periodic cleanup of completed executions
                    if (config.retentionPolicy.enabled && !now.isBefore(nextCleanupAt)) {
                        val cutoff = now.minus(config.retentionPolicy.maxAge.toJavaDuration())
                        var totalCleaned = 0
                        var batchCount = 0
                        while (batchCount < 5) {
                            val cleaned = store.cleanupCompletedExecutions(
                                before = cutoff,
                                statuses = config.retentionPolicy.statuses,
                                limit = config.retentionPolicy.batchSize
                            )
                            totalCleaned += cleaned
                            batchCount++
                            if (cleaned < config.retentionPolicy.batchSize) break
                        }
                        if (totalCleaned > 0) {
                            log.info("Cleaned up $totalCleaned completed executions older than $cutoff")
                            safeNotify { config.observer.onRetentionCleanup(totalCleaned) }
                        }
                        nextCleanupAt = nextCleanupTime(now)
                    }

                    // Periodic DLQ count sampling (skipped when telemetry is disabled; failures never block polling)
                    if (isTelemetryEnabled() && Duration.between(lastDeadLetterSample, now) >= config.deadLetterSamplingInterval) {
                        try {
                            val deadLetterCount = store.countExecutions(
                                JobExecutionQuery(statuses = setOf(ExecutionStatus.DEAD_LETTERED))
                            )
                            safeNotify { config.observer.onDeadLetterCountSampled(deadLetterCount) }
                        } catch (ce: CancellationException) {
                            throw ce
                        } catch (e: Exception) {
                            log.warn("Failed to sample dead-letter execution count", e)
                        } finally {
                            lastDeadLetterSample = now
                        }
                    }

                    pollAndExecute()

                    val completedPollAt = Instant.now(clock)
                    lastPollAt = completedPollAt
                    lastSuccessfulPollAt = completedPollAt
                    consecutivePollFailures.set(0)
                    lastPollErrorType = null
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    log.error("Error in scheduler loop", e)
                    lastPollAt = Instant.now(clock)
                    consecutivePollFailures.incrementAndGet()
                    lastPollErrorType = ErrorDiagnosticFormatter.typeName(e)
                }
                // TODO: Consider Adaptive Delay in the future. 
                // Currently using fixed polling to ensure discovery of jobs added by other nodes 
                // without needing a distributed wake-up signal (e.g. Postgres NOTIFY).
                delay(config.pollingInterval.toMillis())
            }
        }
    }

    suspend fun health(): SchedulerHealth {
        val storeHealth = try {
            store.checkHealth()
        } catch (ce: CancellationException) {
            throw ce
        } catch (e: Exception) {
            StoreHealth(
                status = HealthStatus.DOWN,
                latency = Duration.ZERO,
                errorType = ErrorDiagnosticFormatter.typeName(e),
                detail = formatError(e)
            )
        }
        val activeJob = this.job
        val isStopping = this.stopping
        val lastPoll = this.lastPollAt
        val lastSuccess = this.lastSuccessfulPollAt
        val failures = consecutivePollFailures.get()
        val lastErrorType = this.lastPollErrorType
        val now = Instant.now(clock)

        val status = when {
            isStopping || activeJob == null || !activeJob.isActive -> HealthStatus.STOPPED
            storeHealth.status == HealthStatus.DOWN || failures >= config.healthPollFailureThreshold -> HealthStatus.DOWN
            lastPoll == null -> HealthStatus.STARTING
            failures > 0 || isPollLoopStalled(lastPoll, now) -> HealthStatus.DEGRADED
            else -> HealthStatus.UP
        }

        return SchedulerHealth(
            status = status,
            storeHealth = storeHealth,
            workerId = workerId,
            activeExecutions = activeJobs.size,
            lastPollAt = lastPoll,
            lastSuccessfulPollAt = lastSuccess,
            consecutivePollFailures = failures,
            lastPollErrorType = lastErrorType,
            stopping = isStopping
        )
    }

    private fun isPollLoopStalled(lastPoll: Instant, now: Instant): Boolean {
        val tripledPolling = config.pollingInterval.multipliedBy(3)
        val minStallWindow = Duration.ofSeconds(5)
        val stallThreshold = if (tripledPolling > minStallWindow) tripledPolling else minStallWindow
        return Duration.between(lastPoll, now) > stallThreshold
    }

    private fun isTelemetryEnabled(): Boolean {
        val observer = config.observer
        return observer !== KhronaObserver.NO_OP &&
            (observer !is CompositeKhronaObserver || observer.observers.any { it !== KhronaObserver.NO_OP })
    }

    private fun formatError(error: Throwable): String = try {
        config.errorFormatter.format(error)
    } catch (formatterEx: Exception) {
        log.warn("ErrorDiagnosticFormatter failed; falling back to error type name only", formatterEx)
        ErrorDiagnosticFormatter.typeName(error)
    }

    private fun elapsedSince(startedAtNanos: Long): Duration {
        if (startedAtNanos <= 0L) return Duration.ZERO
        return Duration.ofNanos((System.nanoTime() - startedAtNanos).coerceAtLeast(0L))
    }

    private fun buildExecutionContext(
        execution: JobExecution,
        correlationId: String = execution.correlationId ?: execution.id.toString()
    ): ExecutionContext = ExecutionContext(
        executionId = execution.id,
        jobId = execution.jobId,
        attempt = execution.attempt,
        scheduledAt = execution.scheduledAt,
        payload = execution.payload,
        correlationId = correlationId,
        isStopping = { stopping }
    )

    private fun recordCompletion(
        claim: ActiveClaim?,
        context: ExecutionContext,
        outcome: ExecutionOutcome,
        startedAtNanos: Long,
        errorType: String? = null
    ) {
        if (claim != null && !claim.completionRecorded.compareAndSet(false, true)) return
        safeNotify {
            config.observer.onExecutionCompleted(
                context = context,
                outcome = outcome,
                duration = elapsedSince(startedAtNanos),
                errorType = errorType
            )
        }
    }

    private fun initialCleanupTime(now: Instant): Instant {
        val jitter = config.retentionPolicy.cleanupJitter
        if (!config.retentionPolicy.enabled || jitter <= 0.0) return Instant.MIN
        val intervalMillis = config.retentionPolicy.cleanupInterval.inWholeMilliseconds
        val offsetMillis = (intervalMillis * jitter * random.nextDouble()).toLong()
        return now.plusMillis(offsetMillis)
    }

    private fun nextCleanupTime(now: Instant): Instant {
        val intervalMillis = config.retentionPolicy.cleanupInterval.inWholeMilliseconds
        val jitter = config.retentionPolicy.cleanupJitter
        if (jitter <= 0.0) return now.plusMillis(intervalMillis)
        val factor = 1.0 + (random.nextDouble() * 2.0 - 1.0) * jitter
        val delayMillis = (intervalMillis * factor).toLong().coerceAtLeast(1L)
        return now.plusMillis(delayMillis)
    }

    private fun validateJob(jobDef: JobDefinition) {
        val trigger = jobDef.trigger
        val now = Instant.now(clock)
        
        when (trigger) {
            is IntervalTrigger -> {
                if (trigger.interval < config.pollingInterval) {
                    throw IllegalArgumentException("Job ${jobDef.id} has a trigger interval (${trigger.interval}) smaller than the scheduler polling interval (${config.pollingInterval})")
                }
            }
            is CronTrigger -> {
                val next = trigger.nextExecutionTime(now)
                val following = next?.let { trigger.nextExecutionTime(it.plusMillis(1)) }
                
                if (next != null && following != null) {
                    val gap = Duration.between(next, following)
                    if (gap < config.pollingInterval) {
                        throw IllegalArgumentException("Job ${jobDef.id} has a cron frequency ($gap) smaller than the scheduler polling interval (${config.pollingInterval})")
                    }
                }
            }
            is OneTimeTrigger -> {
                // One-time triggers don't have frequency constraints
            }
        }
    }

    internal suspend fun pollAndExecute() {
        if (activeJobs.size >= config.maxConcurrentExecutions) {
            log.debug("Skipping poll because active executions (${activeJobs.size}) reached maxConcurrentExecutions (${config.maxConcurrentExecutions})")
            return
        }
        // A poll and every claim it creates stay attached to this store even if config changes.
        val store = this.store
        val now = Instant.now(clock)
        var candidateJobIds = handlerRegistry.registeredJobIds()
        var remainingBatchBudget = config.pollBatchSize

        do {
            val blockedJobIds = mutableSetOf<String>()
            val eligible = store.listEligibleExecutions(now, remainingBatchBudget, candidateJobIds)

            eligible.forEach { execution ->
                if (activeJobs.containsKey(execution.id)) {
                    log.trace("[${execution.jobId}] Skipping execution ${execution.id} because it is already active locally")
                    return@forEach
                }

                // Check if we have a handler for this job before claiming
                if (!handlerRegistry.hasHandler(execution.jobId)) {
                    log.trace("[${execution.jobId}] Skipping execution ${execution.id} because no local handler is registered")
                    return@forEach
                }

                // Re-fetch job def in case it changed or for fresh state
                val jobDef = loadJobDefinition(execution.jobId, store) ?: run {
                    blockedJobIds.add(execution.jobId)
                    return@forEach
                }

                if (jobDef.paused) {
                    log.debug("[${execution.jobId}] Skipping execution ${execution.id} because job is paused")
                    blockedJobIds.add(execution.jobId)
                    return@forEach
                }

                // Check for misfire
                if (Duration.between(execution.scheduledAt, now) > config.misfireThreshold) {
                    if (jobDef.misfirePolicy == MisfirePolicy.IGNORE) {
                        val claimOwner = "$workerId:${UUID.randomUUID()}"
                        if (store.claimExecution(execution.id, claimOwner, config.executionLeaseDuration)) {
                            handleMisfire(execution, jobDef, now, claimOwner, store)
                            remainingBatchBudget--
                        }
                        return@forEach
                    } else {
                        log.info("[${execution.jobId}] Misfire detected (scheduled at ${execution.scheduledAt}), firing now due to FIRE_NOW policy")
                    }
                }

                // Check for distributed lock if policy is FORBID
                if (jobDef.concurrencyPolicy == ConcurrencyPolicy.FORBID && jobDef.lockKey != null) {
                    if (store.isLockHeld(jobDef.lockKey)) {
                        log.debug("[${execution.jobId}] Skipping execution ${execution.id} because lock ${jobDef.lockKey} is held")
                        safeNotify { config.observer.onLockContention(execution.jobId, jobDef.lockKey, ConcurrencyPolicy.FORBID) }
                        blockedJobIds.add(execution.jobId)
                        return@forEach
                    }
                }

                if (activeJobs.size >= config.maxConcurrentExecutions) {
                    log.debug("Stopping claim loop because active executions (${activeJobs.size}) reached maxConcurrentExecutions (${config.maxConcurrentExecutions})")
                    return
                }

                val claimOwner = "$workerId:${UUID.randomUUID()}"
                val leaseDuration = config.executionLeaseDuration
                val initialLeaseDeadline = newLeaseDeadline(leaseDuration)
                if (store.claimExecution(execution.id, claimOwner, leaseDuration)) {
                    val placeholderJob = Job()
                    val previousJob = activeJobs.putIfAbsent(execution.id, placeholderJob)
                    if (previousJob != null) {
                        placeholderJob.complete()
                        store.updateExecutionStatus(execution.id, ExecutionStatus.PENDING, workerId = claimOwner)
                        return@forEach
                    }
                    val executionContext = buildExecutionContext(execution)
                    val claim = ActiveClaim(
                        jobId = execution.jobId,
                        owner = claimOwner,
                        store = store,
                        executionContext = executionContext
                    )
                    activeClaims[execution.id] = claim

                    var launched = false
                    try {
                        if (stopping || !currentCoroutineContext().isActive) {
                            return@forEach
                        }

                        // Handle REPLACE only after the replacement has been claimed.
                        if (jobDef.concurrencyPolicy == ConcurrencyPolicy.REPLACE && jobDef.lockKey != null) {
                            val supersededIds = store.supersedeExecutionsByLockKey(
                                lockKey = jobDef.lockKey,
                                excludeExecutionId = execution.id
                            )
                            supersededIds.forEach { id ->
                                val supersededClaim = activeClaims[id]
                                if (supersededClaim != null) {
                                    recordCompletion(
                                        claim = supersededClaim,
                                        context = supersededClaim.executionContext,
                                        outcome = ExecutionOutcome.SUPERSEDED,
                                        startedAtNanos = supersededClaim.startedAtNanos.get()
                                    )
                                } else {
                                    emitRemoteSupersededCompletion(id, execution, store)
                                }
                                activeJobs[id]?.cancel("Superseded by a new execution for lock ${jobDef.lockKey}")
                            }
                        }

                        // Double-check lock AFTER claiming but before launching
                        // This handles the race where two executions for the same lock were claimed 
                        // by different nodes (if duplicate executions existed) or by the same node.
                        if (jobDef.concurrencyPolicy == ConcurrencyPolicy.FORBID && jobDef.lockKey != null) {
                            // We need to be careful here: isLockHeld will see OURSELVES if we just claimed it.
                            // But isLockHeld currently checks if ANY execution for that key is CLAIMED/RUNNING.
                            // To be safe, we should check if any OTHER execution is holding the lock.
                            if (isAnyOtherExecutionHoldingLock(execution.id, jobDef.lockKey, store)) {
                                log.warn("[${execution.jobId}] Release claim for ${execution.id} because another execution already holds the lock ${jobDef.lockKey}")
                                safeNotify { config.observer.onLockContention(execution.jobId, jobDef.lockKey, ConcurrencyPolicy.FORBID) }
                                store.updateExecutionStatus(execution.id, ExecutionStatus.PENDING, workerId = claimOwner)
                                blockedJobIds.add(execution.jobId)
                                return@forEach
                            }
                        }

                        if (stopping || !currentCoroutineContext().isActive) {
                            return@forEach
                        }

                        val executionJob = scope.launch(start = CoroutineStart.LAZY) {
                            val selfJob = coroutineContext[Job]!!
                            try {
                                executeJobWithHeartbeat(execution, jobDef, leaseDuration, claim, initialLeaseDeadline)
                            } finally {
                                activeJobs.remove(execution.id, selfJob)
                                activeClaims.remove(execution.id, claim)
                            }
                        }
                        activeJobs.replace(execution.id, placeholderJob, executionJob)
                        placeholderJob.complete()
                        if (stopping || !executionJob.start()) {
                            executionJob.cancel("Scheduler stopping")
                            activeJobs.remove(execution.id, executionJob)
                            activeClaims.remove(execution.id, claim)
                        } else {
                            launched = true
                            remainingBatchBudget--
                        }
                    } finally {
                        if (!launched) {
                            placeholderJob.complete()
                            activeJobs.remove(execution.id, placeholderJob)
                            activeClaims.remove(execution.id, claim)
                            try {
                                withContext(NonCancellable) {
                                    store.updateExecutionStatus(
                                        execution.id,
                                        ExecutionStatus.PENDING,
                                        "Released during scheduler shutdown",
                                        workerId = claim.owner
                                    )
                                }
                            } catch (ex: Exception) {
                                log.error("Failed to release unclaimed execution ${execution.id}", ex)
                            }
                        }
                    }
                }
            }

            candidateJobIds = candidateJobIds - blockedJobIds
        } while (blockedJobIds.isNotEmpty() && candidateJobIds.isNotEmpty() && remainingBatchBudget > 0)
    }

    private suspend fun loadJobDefinition(jobId: String, store: JobStore): JobDefinition? {
        return try {
            store.getJob(jobId)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.error("[$jobId] Failed to load job definition; skipping job for this poll cycle", e)
            null
        }
    }

    private suspend fun isAnyOtherExecutionHoldingLock(currentExecutionId: UUID, lockKey: String, store: JobStore): Boolean {
        return store.isLockHeld(lockKey, excludeExecutionId = currentExecutionId)
    }

    private suspend fun emitRemoteSupersededCompletion(
        supersededId: UUID,
        replacingExecution: JobExecution,
        store: JobStore
    ) {
        val supersededExecution = try {
            store.getExecution(supersededId)
        } catch (ce: CancellationException) {
            throw ce
        } catch (e: Exception) {
            log.warn("Failed to load superseded execution $supersededId from store for observability", e)
            null
        }
        val context = if (supersededExecution != null) {
            buildExecutionContext(supersededExecution)
        } else {
            ExecutionContext(
                executionId = supersededId,
                jobId = replacingExecution.jobId,
                attempt = 0,
                scheduledAt = replacingExecution.scheduledAt,
                payload = null,
                correlationId = supersededId.toString(),
                isStopping = { stopping }
            )
        }
        val elapsed = supersededExecution?.startedAt?.let { startedAt ->
            val raw = Duration.between(startedAt, Instant.now(clock))
            if (raw.isNegative) Duration.ZERO else raw
        } ?: Duration.ZERO
        safeNotify {
            config.observer.onExecutionCompleted(
                context = context,
                outcome = ExecutionOutcome.SUPERSEDED,
                duration = elapsed,
                errorType = null
            )
        }
    }

    private suspend fun handleMisfire(execution: JobExecution, jobDef: JobDefinition, now: Instant, claimOwner: String, store: JobStore) {
        log.info("[${execution.jobId}] Misfire detected (scheduled at ${execution.scheduledAt}), ignoring due to IGNORE policy")
        val next = jobDef.trigger.nextExecutionTime(now)
            ?.truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
        val nextExecution = next?.let {
            val deterministicId = UUID.nameUUIDFromBytes("${jobDef.id}:$it".toByteArray())
            JobExecution(
                id = deterministicId,
                jobId = jobDef.id,
                scheduledAt = it,
                lockKey = jobDef.lockKey
                // Do NOT propagate correlationId to next recurring run
            )
        }
        val completed = store.completeExecution(
            id = execution.id,
            status = ExecutionStatus.MISFIRED,
            error = "Misfire threshold exceeded",
            workerId = claimOwner,
            nextExecution = nextExecution
        )
        if (completed) {
            recordCompletion(
                claim = null,
                context = buildExecutionContext(execution),
                outcome = ExecutionOutcome.MISFIRED,
                startedAtNanos = 0L
            )
            if (nextExecution != null) {
                safeNotify { config.observer.onExecutionScheduled(jobDef.id, ScheduleReason.RECURRING_NEXT) }
            }
        }
    }

    private fun newLeaseDeadline(duration: Duration): LeaseDeadline {
        val startedAtNanos = System.nanoTime()
        return LeaseDeadline(Instant.now(clock).plus(duration), startedAtNanos, duration)
    }

    private suspend fun executeJobWithHeartbeat(
        execution: JobExecution,
        jobDef: JobDefinition,
        leaseDuration: Duration,
        claim: ActiveClaim,
        initialLeaseDeadline: LeaseDeadline
    ) {
        coroutineScope {
            if (initialLeaseDeadline.remaining(Instant.now(clock)) <= Duration.ZERO) {
                safeNotify {
                    config.observer.onHeartbeatFailed(
                        claim.executionContext,
                        HeartbeatFailureReason.LEASE_BUDGET_EXCEEDED
                    )
                }
                return@coroutineScope
            }
            val leaseDeadline = AtomicReference(initialLeaseDeadline)
            val watchdogJob = launch {
                while (isActive) {
                    val now = Instant.now(clock)
                    val deadline = leaseDeadline.get()
                    val remaining = deadline.remaining(now)
                    if (remaining <= Duration.ZERO) {
                        log.warn("[${execution.jobId}] Execution ${execution.id} lease expired at ${deadline.expiresAt} without renewal; cancelling handler")
                        safeNotify {
                            config.observer.onHeartbeatFailed(
                                claim.executionContext,
                                HeartbeatFailureReason.LEASE_BUDGET_EXCEEDED
                            )
                        }
                        this@coroutineScope.cancel("Execution ${execution.id} lease expired")
                        break
                    }
                    delay(minOf(heartbeatInterval.toMillis().coerceAtLeast(1L), remaining.toMillis().coerceAtLeast(1L)))
                }
            }
            val heartbeatJob = launch {
                while (isActive) {
                    delay(heartbeatInterval.toMillis())
                    // A response can arrive long after the store persisted the renewal.
                    // Budget from request start so network latency never extends ownership.
                    val renewalDeadline = newLeaseDeadline(leaseDuration)
                    val renewed = try {
                        val ok = claim.store.heartbeat(execution.id, claim.owner, leaseDuration)
                        if (ok) {
                            leaseDeadline.set(renewalDeadline)
                            safeNotify { config.observer.onHeartbeatSucceeded(claim.executionContext) }
                        } else {
                            safeNotify {
                                config.observer.onHeartbeatFailed(
                                    claim.executionContext,
                                    HeartbeatFailureReason.LOST_OWNERSHIP_OR_EXPIRED
                                )
                            }
                        }
                        ok
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        log.error("[${execution.jobId}] Error during heartbeat for execution ${execution.id}", e)
                        safeNotify {
                            config.observer.onHeartbeatFailed(
                                claim.executionContext,
                                HeartbeatFailureReason.STORE_ERROR
                            )
                        }
                        leaseDeadline.get().remaining(Instant.now(clock)) > Duration.ZERO
                    }
                    if (!renewed) {
                        log.warn("[${execution.jobId}] Failed to heartbeat for execution ${execution.id}, it might have been reclaimed or superseded")
                        this@coroutineScope.cancel("Execution ${execution.id} is no longer valid (superseded or reclaimed)")
                        break
                    }
                }
            }

            try {
                executeJob(execution, jobDef, claim)
            } finally {
                heartbeatJob.cancel()
                watchdogJob.cancel()
            }
        }
    }

    private suspend fun executeJob(execution: JobExecution, jobDef: JobDefinition, claim: ActiveClaim) {
        val executionContext = claim.executionContext
        val correlationId = executionContext.correlationId
        withContext(MDCContext(mapOf("correlationId" to correlationId))) {
            log.info("[${execution.jobId}] [$correlationId] Executing job (execution: ${execution.id})")
            if (!claim.store.updateExecutionStatus(execution.id, ExecutionStatus.RUNNING, workerId = claim.owner)) {
                log.warn("[${execution.jobId}] [$correlationId] Execution ${execution.id} is no longer owned by ${claim.owner} before start; aborting")
                return@withContext
            }

            val rawLag = Duration.between(execution.scheduledAt, Instant.now(clock))
            val lag = if (rawLag.isNegative) Duration.ZERO else rawLag
            val executionStartNanos = System.nanoTime()
            claim.startedAtNanos.set(executionStartNanos)
            safeNotify { config.observer.onExecutionStarted(executionContext, lag) }

            val handlerFailure: Throwable? = try {
                val handler = handlerRegistry.get(jobDef.id)
                    ?: throw IllegalStateException("No handler registered for job ${jobDef.id}")

                invokeAroundSafely(config.observer, executionContext) {
                    if (jobDef.timeout != null) {
                        withTimeout(jobDef.timeout.toMillis()) {
                            executionContext.handler(execution.payload)
                        }
                    } else {
                        executionContext.handler(execution.payload)
                    }
                }
                null
            } catch (e: ShutdownCheckpointException) {
                log.info("[${execution.jobId}] [$correlationId] Execution ${execution.id} stopped at shutdown checkpoint; releasing back to PENDING")
                withContext(NonCancellable) {
                    try {
                        claim.store.updateExecutionStatus(
                            id = execution.id,
                            status = ExecutionStatus.PENDING,
                            error = "Interrupted at shutdown checkpoint",
                            workerId = claim.owner
                        )
                    } catch (releaseEx: Exception) {
                        log.error("[${execution.jobId}] [$correlationId] Failed to release execution ${execution.id} after shutdown checkpoint", releaseEx)
                    }
                }
                recordCompletion(
                    claim = claim,
                    context = executionContext,
                    outcome = ExecutionOutcome.SHUTDOWN_CHECKPOINT,
                    startedAtNanos = executionStartNanos
                )
                return@withContext
            } catch (e: Throwable) {
                if (e is CancellationException && e !is TimeoutCancellationException) {
                    log.info("[${execution.jobId}] [$correlationId] Job was cancelled (superseded or shutdown)")
                    // We don't update status to FAILED here if it was already updated to SUPERSEDED
                    // But to be safe, we can ensure it's not DEAD_LETTERED
                    return@withContext
                }
                e
            }

            if (handlerFailure == null) {
                val nextExecution = buildNextRecurringExecution(execution, jobDef)
                if (completeExecutionWithRetry(
                        store = claim.store,
                        id = execution.id,
                        status = ExecutionStatus.SUCCESS,
                        workerId = claim.owner,
                        nextExecution = nextExecution
                    )
                ) {
                    log.info("[${execution.jobId}] [$correlationId] Job finished successfully")
                    recordCompletion(
                        claim = claim,
                        context = executionContext,
                        outcome = ExecutionOutcome.SUCCESS,
                        startedAtNanos = executionStartNanos
                    )
                    if (nextExecution != null) {
                        safeNotify { config.observer.onExecutionScheduled(jobDef.id, ScheduleReason.RECURRING_NEXT) }
                    }
                } else {
                    log.warn("[${execution.jobId}] [$correlationId] Execution ${execution.id} lost ownership before completion; skipping SUCCESS and next-run scheduling")
                }
            } else {
                log.error("[${execution.jobId}] [$correlationId] Job failed", handlerFailure)
                val errorType = ErrorDiagnosticFormatter.typeName(handlerFailure)
                val formattedError = formatError(handlerFailure)
                val nextAttempt = execution.attempt + 1
                if (nextAttempt < jobDef.retryPolicy.maxAttempts) {
                    val nextDelay = jobDef.retryPolicy.calculateDelay(nextAttempt)
                    val nextRun = Instant.now(clock).plus(nextDelay)
                    val retryExecution = JobExecution(
                        jobId = execution.jobId,
                        scheduledAt = nextRun,
                        attempt = nextAttempt,
                        payload = execution.payload,
                        lockKey = jobDef.lockKey,
                        correlationId = correlationId // Propagate correlationId to retries of the SAME execution
                    )

                    if (completeExecutionWithRetry(
                            store = claim.store,
                            id = execution.id,
                            status = ExecutionStatus.FAILED,
                            error = formattedError,
                            workerId = claim.owner,
                            nextExecution = retryExecution
                        )
                    ) {
                        log.info("[${execution.jobId}] [$correlationId] Scheduled retry at $nextRun (attempt $nextAttempt)")
                        safeNotify {
                            config.observer.onRetryScheduled(
                                context = executionContext,
                                nextAttempt = nextAttempt,
                                delay = nextDelay,
                                errorType = errorType
                            )
                        }
                        safeNotify { config.observer.onExecutionScheduled(jobDef.id, ScheduleReason.RETRY) }
                        recordCompletion(
                            claim = claim,
                            context = executionContext,
                            outcome = ExecutionOutcome.FAILED_RETRY,
                            startedAtNanos = executionStartNanos,
                            errorType = errorType
                        )
                    } else {
                        log.warn("[${execution.jobId}] [$correlationId] Execution ${execution.id} lost ownership before failure transition; skipping retry scheduling")
                    }
                } else {
                    val nextExecution = buildNextRecurringExecution(execution, jobDef)
                    if (completeExecutionWithRetry(
                            store = claim.store,
                            id = execution.id,
                            status = ExecutionStatus.DEAD_LETTERED,
                            error = formattedError,
                            workerId = claim.owner,
                            nextExecution = nextExecution
                        )
                    ) {
                        log.warn("[${execution.jobId}] [$correlationId] Reached max attempts (${jobDef.retryPolicy.maxAttempts}) and is now DEAD_LETTERED")
                        recordCompletion(
                            claim = claim,
                            context = executionContext,
                            outcome = ExecutionOutcome.DEAD_LETTERED,
                            startedAtNanos = executionStartNanos,
                            errorType = errorType
                        )
                        if (nextExecution != null) {
                            safeNotify { config.observer.onExecutionScheduled(jobDef.id, ScheduleReason.RECURRING_NEXT) }
                        }
                    } else {
                        log.warn("[${execution.jobId}] [$correlationId] Execution ${execution.id} lost ownership before DEAD_LETTERED transition; skipping next-run scheduling")
                    }
                }
            }
        }
    }

    private suspend fun completeExecutionWithRetry(
        store: JobStore,
        id: UUID,
        status: ExecutionStatus,
        error: String? = null,
        workerId: String,
        nextExecution: JobExecution? = null
    ): Boolean {
        repeat(2) { attempt ->
            try {
                return store.completeExecution(
                    id = id,
                    status = status,
                    error = error,
                    workerId = workerId,
                    nextExecution = nextExecution
                )
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                if (attempt == 1) {
                    log.error("Failed to complete execution $id with status $status after retry", e)
                } else {
                    log.warn("Transient failure completing execution $id with status $status; retrying once", e)
                }
            }
        }
        return false
    }

    private fun buildNextRecurringExecution(execution: JobExecution, jobDef: JobDefinition): JobExecution? {
        // Use plusMillis(1) to ensure we look for the NEXT execution after this one
        // (important for inclusive triggers like OneTimeTrigger), and clamp to 'now'
        // when misfired so an overdue FIRE_NOW execution does not replay missed historical occurrences.
        val now = Instant.now(clock)
        val isMisfired = Duration.between(execution.scheduledAt, now) > config.misfireThreshold
        val baseline = if (isMisfired) {
            now.truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
        } else {
            execution.scheduledAt.plusMillis(1)
        }
        val next = jobDef.trigger.nextExecutionTime(baseline)
            ?.truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
            ?: return null
        val deterministicId = UUID.nameUUIDFromBytes("${jobDef.id}:$next".toByteArray())
        return JobExecution(
            id = deterministicId,
            jobId = jobDef.id,
            scheduledAt = next,
            lockKey = jobDef.lockKey
            // Do NOT propagate correlationId to next recurring run
        )
    }

    suspend fun stop() {
        val shutdownStartNanos = System.nanoTime()
        log.info("Stopping Khrona Scheduler")
        stopping = true
        val pollingJob = job
        job = null
        pollingJob?.cancel() // Stop polling for new jobs

        var cleanShutdown = true
        var remainingActiveCount = 0
        val totalTimeoutMs = config.shutdownTimeout.toMillis()
        withTimeoutOrNull(totalTimeoutMs + 5L) {
            withTimeoutOrNull(minOf(50L, totalTimeoutMs)) {
                pollingJob?.join()
            }

            if (activeJobs.isNotEmpty()) {
                // The last claim can disappear between a size check and reading an entry.
                // Copy by traversal instead of using Kotlin's size-based singleton shortcut.
                val remainingClaims = mutableMapOf<UUID, ActiveClaim>()
                activeClaims.forEach { executionId, claim -> remainingClaims[executionId] = claim }
                val graceTimeoutMs = (totalTimeoutMs * 0.7).toLong().coerceAtLeast(1L)
                val completedCleanly = withTimeoutOrNull(graceTimeoutMs) {
                    log.info("Waiting up to ${graceTimeoutMs}ms for ${activeJobs.size} active jobs to complete...")
                    activeJobs.values.joinAll()
                    true
                } ?: false

                if (completedCleanly) {
                    log.info("All active jobs completed cleanly during shutdown.")
                } else {
                    cleanShutdown = false
                    remainingActiveCount = activeJobs.size
                    log.warn("Shutdown timeout reached. Cancelling $remainingActiveCount remaining jobs.")
                    activeJobs.values.forEach { it.cancel("Scheduler stopping (timeout)") }

                    val releaseJob = scope.launch(Dispatchers.IO + NonCancellable) {
                        remainingClaims.forEach { (executionId, claim) ->
                            try {
                                val currentExec = claim.store.getExecution(executionId)
                                if (currentExec?.status == ExecutionStatus.CLAIMED || currentExec?.status == ExecutionStatus.RUNNING) {
                                    log.info("Releasing execution $executionId back to PENDING due to shutdown")
                                    claim.store.updateExecutionStatus(
                                        executionId,
                                        ExecutionStatus.PENDING,
                                        "Released during scheduler shutdown",
                                        workerId = claim.owner
                                    )
                                }
                            } catch (ce: CancellationException) {
                                throw ce
                            } catch (ex: Exception) {
                                log.error("Failed to release execution $executionId during shutdown", ex)
                            }
                        }
                    }
                    withTimeoutOrNull((totalTimeoutMs - graceTimeoutMs).coerceAtLeast(50L)) {
                        releaseJob.join()
                    }
                }
            }
        }

        activeJobs.clear()
        activeClaims.clear()
        val shutdownDuration = elapsedSince(shutdownStartNanos)
        safeNotify { config.observer.onShutdownCompleted(shutdownDuration, cleanShutdown, remainingActiveCount) }
        log.info("Khrona Scheduler stopped")
    }

    suspend fun registerJob(jobDef: JobDefinition) {
        val store = this.store
        validateJob(jobDef)
        handlerRegistry.register(jobDef.id, jobDef.handler)
        val correlationId = MDC.get("correlationId")
        persistJobDefinition(jobDef, store)
        seedFirstExecution(jobDef, correlationId, store)
    }

    private suspend fun persistJobDefinition(jobDef: JobDefinition, store: JobStore) {
        val existing = store.getJob(jobDef.id)
        val effectiveJob = if (existing != null && existing.paused && !jobDef.paused) {
            jobDef.copy(paused = true)
        } else {
            jobDef
        }
        store.saveJob(effectiveJob)
    }

    private suspend fun seedFirstExecution(jobDef: JobDefinition, correlationId: String?, store: JobStore) {
        // Truncate to seconds to ensure deterministic UUID matching across nodes
        val now = Instant.now(clock).truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
        val next = when (val trigger = jobDef.trigger) {
            is OneTimeTrigger -> trigger.nextExecutionTime(Instant.EPOCH)
            is IntervalTrigger -> {
                if (!trigger.initialDelay.isZero) {
                    now.plus(trigger.initialDelay).truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
                } else {
                    trigger.nextExecutionTime(now)
                }
            }
            else -> trigger.nextExecutionTime(now)
        }
        if (next != null) {
            // Use a deterministic UUID for the first execution to avoid duplicates when multiple nodes start.
            val deterministicId = UUID.nameUUIDFromBytes("${jobDef.id}:$next".toByteArray())
            val seeded = store.seedInitialExecution(
                JobExecution(
                    id = deterministicId,
                    jobId = jobDef.id,
                    scheduledAt = next,
                    lockKey = jobDef.lockKey,
                    correlationId = correlationId ?: deterministicId.toString()
                )
            )
            if (seeded) {
                safeNotify { config.observer.onExecutionScheduled(jobDef.id, ScheduleReason.INITIAL_SEED) }
            }
        }
    }

    /**
     * Manually triggers a job for immediate execution.
     * The job must have been previously registered.
     */
    suspend fun trigger(jobId: String, payload: Any? = null) {
        val store = this.store
        val jobDef = store.getJob(jobId) ?: throw IllegalArgumentException("Job $jobId not found")
        enqueueManualExecution(jobDef, payload, store)
    }

    suspend fun startJob(jobId: String, payload: Any? = null): JobControlResult {
        val store = this.store
        val jobDef = store.getJob(jobId)
            ?: return JobControlResult(success = false, reason = "Job '$jobId' not found")
        val executionId = enqueueManualExecution(jobDef, payload, store)
        val reason = if (jobDef.paused) {
            "Job '$jobId' is paused; execution '$executionId' queued as PENDING until resumed"
        } else {
            null
        }
        return JobControlResult(
            success = true,
            affectedExecutionIds = listOf(executionId),
            reason = reason
        )
    }

    private suspend fun enqueueManualExecution(jobDef: JobDefinition, payload: Any?, store: JobStore): UUID {
        val correlationId = MDC.get("correlationId")
        val now = Instant.now(clock).truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
        val executionId = UUID.randomUUID()
        store.saveExecution(
            JobExecution(
                id = executionId,
                jobId = jobDef.id,
                scheduledAt = now,
                payload = payload,
                lockKey = jobDef.lockKey,
                correlationId = correlationId ?: executionId.toString()
            )
        )
        safeNotify { config.observer.onExecutionScheduled(jobDef.id, ScheduleReason.MANUAL_TRIGGER) }
        return executionId
    }

    suspend fun pauseJob(jobId: String): JobControlResult = setJobPaused(jobId, paused = true)

    suspend fun resumeJob(jobId: String): JobControlResult = setJobPaused(jobId, paused = false)

    suspend fun listJobOverviews(): List<JobOverview> {
        val store = this.store
        val mergedJobs = LinkedHashMap<String, JobDefinition>()
        config.jobs.forEach { mergedJobs[it.id] = it }
        store.listJobs().forEach { mergedJobs[it.id] = it }
        return mergedJobs.values
            .sortedBy { it.id }
            .map { buildJobOverview(it, store) }
    }

    suspend fun getJobOverview(jobId: String): JobOverview? {
        val store = this.store
        val jobDef = store.getJob(jobId) ?: config.jobs.find { it.id == jobId } ?: return null
        return buildJobOverview(jobDef, store)
    }

    suspend fun listJobHistory(
        jobId: String,
        limit: Int = JobExecutionQuery.DEFAULT_LIMIT
    ): List<JobExecution> {
        return listExecutions(JobExecutionQuery(jobId = jobId, limit = limit))
    }

    suspend fun listExecutions(
        query: JobExecutionQuery = JobExecutionQuery()
    ): List<JobExecution> = store.listExecutions(query)

    suspend fun stopExecution(executionId: UUID): JobControlResult {
        val activeJob = activeJobs[executionId]
        val activeClaim = activeClaims[executionId]
        if (activeJob == null || activeClaim == null) {
            val existing = store.getExecution(executionId)
                ?: return JobControlResult(success = false, reason = "Execution '$executionId' not found")
            if (existing.status.isTerminal) {
                return JobControlResult(
                    success = false,
                    reason = "Execution '$executionId' is already terminal (${existing.status})"
                )
            }
            return JobControlResult(
                success = false,
                reason = "Execution '$executionId' is not active on this scheduler instance (status=${existing.status})"
            )
        }

        val cancelled = activeClaim.store.updateExecutionStatus(
            id = executionId,
            status = ExecutionStatus.CANCELLED,
            error = "Stopped locally",
            workerId = activeClaim.owner
        )
        if (!cancelled) {
            return JobControlResult(
                success = false,
                reason = "Execution '$executionId' is no longer active on this scheduler instance"
            )
        }

        recordCompletion(
            claim = activeClaim,
            context = activeClaim.executionContext,
            outcome = ExecutionOutcome.CANCELLED,
            startedAtNanos = activeClaim.startedAtNanos.get()
        )
        activeJob.cancel("Execution $executionId stopped locally")
        return JobControlResult(
            success = true,
            affectedExecutionIds = listOf(executionId)
        )
    }

    suspend fun stopJob(jobId: String): JobControlResult {
        val matchingIds = mutableListOf<UUID>()
        activeClaims.forEach { executionId, claim ->
            if (claim.jobId == jobId) {
                matchingIds.add(executionId)
            }
        }
        if (matchingIds.isEmpty()) {
            val exists = store.getJob(jobId) != null || config.jobs.any { it.id == jobId }
            val reason = if (!exists) {
                "Job '$jobId' not found"
            } else {
                "Job '$jobId' has no active executions on this scheduler instance"
            }
            return JobControlResult(success = false, reason = reason)
        }

        val stoppedIds = mutableListOf<UUID>()
        var failedCount = 0
        matchingIds.forEach { executionId ->
            val result = stopExecution(executionId)
            if (result.success) {
                stoppedIds.addAll(result.affectedExecutionIds)
            } else {
                failedCount++
            }
        }

        if (stoppedIds.isEmpty()) {
            return JobControlResult(
                success = false,
                reason = "No active executions could be stopped for job '$jobId'"
            )
        }

        val partialReason = if (failedCount > 0) {
            "Stopped ${stoppedIds.size} of ${matchingIds.size} active executions"
        } else {
            null
        }
        return JobControlResult(
            success = true,
            affectedExecutionIds = stoppedIds,
            reason = partialReason
        )
    }

    private suspend fun buildJobOverview(jobDef: JobDefinition, store: JobStore): JobOverview {
        return JobOverview(
            job = jobDef,
            paused = jobDef.paused,
            hasLocalHandler = handlerRegistry.hasHandler(jobDef.id),
            progress = buildJobProgress(jobDef.id, store)
        )
    }

    private suspend fun buildJobProgress(jobId: String, store: JobStore): JobProgress {
        val executions = store.listExecutions(
            JobExecutionQuery(jobId = jobId, limit = JobExecutionQuery.DEFAULT_LIMIT)
        )
        val latestExecution = executions.firstOrNull { it.status != ExecutionStatus.PENDING }
            ?: executions.firstOrNull()
        val nextPendingExecution = executions.asSequence()
            .filter { it.status == ExecutionStatus.PENDING }
            .minWithOrNull(compareBy<JobExecution> { it.scheduledAt }.thenBy { it.id.toString() })
        val activeExecutions = executions.filter {
            it.status == ExecutionStatus.CLAIMED || it.status == ExecutionStatus.RUNNING
        }
        val countsByStatus = executions.groupingBy { it.status }.eachCount()
        return JobProgress(
            latestExecution = latestExecution,
            nextPendingExecution = nextPendingExecution,
            activeExecutions = activeExecutions,
            countsByStatus = countsByStatus
        )
    }

    private suspend fun setJobPaused(jobId: String, paused: Boolean): JobControlResult {
        val store = this.store
        val existing = store.getJob(jobId)
            ?: return JobControlResult(success = false, reason = "Job '$jobId' not found")
        if (existing.paused == paused) {
            val stateLabel = if (paused) "paused" else "active"
            return JobControlResult(success = true, reason = "Job '$jobId' is already $stateLabel")
        }
        store.saveJob(existing.copy(paused = paused))
        return JobControlResult(success = true)
    }
}
