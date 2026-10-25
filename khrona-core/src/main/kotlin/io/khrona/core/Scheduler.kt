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
import kotlin.time.toJavaDuration

class Scheduler(
    val config: KhronaConfig,
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob()),
    private val clock: Clock = Clock.systemUTC()
) {
    private val log = LoggerFactory.getLogger(Scheduler::class.java)
    private val store = config.store ?: throw IllegalStateException("JobStore must be configured")
    private val handlerRegistry = HandlerRegistry()
    private val activeJobs = ConcurrentHashMap<UUID, Job>()
    private val activeClaimOwners = ConcurrentHashMap<UUID, String>()
    private val workerId = UUID.randomUUID().toString()
    private val heartbeatInterval = config.heartbeatInterval ?: config.executionLeaseDuration.dividedBy(2)
    private var job: Job? = null
    @Volatile
    private var stopping = false

    init {
        validateConfig()
        config.jobs.forEach {
            validateJob(it)
            handlerRegistry.register(it.id, it.handler)
        }
    }

    private fun validateConfig() {
        if (config.executionLeaseDuration.isNegative || config.executionLeaseDuration.isZero) {
            throw IllegalArgumentException("executionLeaseDuration must be positive")
        }
        if (heartbeatInterval.isNegative || heartbeatInterval.isZero) {
            throw IllegalArgumentException("heartbeatInterval must be positive")
        }
        if (heartbeatInterval >= config.executionLeaseDuration) {
            throw IllegalArgumentException("heartbeatInterval must be strictly less than executionLeaseDuration")
        }
        if (config.pollBatchSize <= 0) {
            throw IllegalArgumentException("pollBatchSize must be positive")
        }
        if (config.retentionPolicy.enabled) {
            if (!config.retentionPolicy.maxAge.isPositive()) {
                throw IllegalArgumentException("retentionPolicy.maxAge must be positive")
            }
            if (!config.retentionPolicy.cleanupInterval.isPositive()) {
                throw IllegalArgumentException("retentionPolicy.cleanupInterval must be positive")
            }
            if (config.retentionPolicy.batchSize <= 0) {
                throw IllegalArgumentException("retentionPolicy.batchSize must be positive")
            }
        }
    }

    fun start() {
        if (job != null) return
        stopping = false
        log.info("Starting Khrona Scheduler (workerId: $workerId, pollingInterval: ${config.pollingInterval})")

        val correlationId = MDC.get("correlationId")

        job = scope.launch {
            // Register jobs from config into store
            config.jobs.forEach { store.saveJob(it) }
            
            // Basic initial scheduling: for each job, create its first execution if none exist
            config.jobs.forEach { jobDef ->
                // Truncate to seconds to ensure deterministic UUID matching across nodes
                val now = Instant.now(clock).truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
                // For OneTimeTrigger, use EPOCH to pick up past executions.
                // For others, use 'now' to avoid catching up from 1970.
                val next = if (jobDef.trigger is OneTimeTrigger) {
                    jobDef.trigger.nextExecutionTime(Instant.EPOCH)
                } else {
                    jobDef.trigger.nextExecutionTime(now)
                }
                
                if (next != null) {
                    // Use a deterministic UUID for the first execution to avoid duplicates when multiple nodes start.
                    val deterministicId = UUID.nameUUIDFromBytes("${jobDef.id}:$next".toByteArray())
                    if (store.getExecution(deterministicId) == null) {
                        store.saveExecution(
                            JobExecution(
                                id = deterministicId,
                                jobId = jobDef.id,
                                scheduledAt = next,
                                lockKey = jobDef.lockKey,
                                correlationId = correlationId ?: deterministicId.toString()
                            )
                        )
                    }
                }
            }

            var lastRecovery = Instant.MIN
            var lastCleanup = Instant.MIN
            while (isActive) {
                try {
                    val now = Instant.now(clock)
                    // Periodic recovery of stale executions
                    if (Duration.between(lastRecovery, now) > Duration.ofMinutes(1)) {
                        val recovered = store.resetExpiredExecutions(now)
                        if (recovered > 0) log.info("Recovered $recovered stale executions")
                        lastRecovery = now
                    }

                    // Periodic cleanup of completed executions
                    if (config.retentionPolicy.enabled && Duration.between(lastCleanup, now) > config.retentionPolicy.cleanupInterval.toJavaDuration()) {
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
                        if (totalCleaned > 0) log.info("Cleaned up $totalCleaned completed executions older than $cutoff")
                        lastCleanup = now
                    }

                    pollAndExecute()
                } catch (e: Exception) {
                    log.error("Error in scheduler loop", e)
                }
                // TODO: Consider Adaptive Delay in the future. 
                // Currently using fixed polling to ensure discovery of jobs added by other nodes 
                // without needing a distributed wake-up signal (e.g. Postgres NOTIFY).
                delay(config.pollingInterval.toMillis())
            }
        }
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
        val now = Instant.now(clock)
        val eligible = store.listEligibleExecutions(now, config.pollBatchSize)
        
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
            val jobDef = store.getJob(execution.jobId) ?: return@forEach
            
            // Check for misfire
            if (Duration.between(execution.scheduledAt, now) > config.misfireThreshold) {
                if (jobDef.misfirePolicy == MisfirePolicy.IGNORE) {
                    val claimOwner = "$workerId:${UUID.randomUUID()}"
                    if (store.claimExecution(execution.id, claimOwner, config.executionLeaseDuration)) {
                        handleMisfire(execution, jobDef, now, claimOwner)
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
                    return@forEach
                }
            }

            val claimOwner = "$workerId:${UUID.randomUUID()}"
            if (store.claimExecution(execution.id, claimOwner, config.executionLeaseDuration)) {
                val placeholderJob = Job()
                val previousJob = activeJobs.putIfAbsent(execution.id, placeholderJob)
                if (previousJob != null) {
                    placeholderJob.complete()
                    store.updateExecutionStatus(execution.id, ExecutionStatus.PENDING, workerId = claimOwner)
                    return@forEach
                }
                activeClaimOwners[execution.id] = claimOwner

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
                        if (isAnyOtherExecutionHoldingLock(execution.id, jobDef.lockKey)) {
                            log.warn("[${execution.jobId}] Release claim for ${execution.id} because another execution already holds the lock ${jobDef.lockKey}")
                            store.updateExecutionStatus(execution.id, ExecutionStatus.PENDING, workerId = claimOwner)
                            return@forEach
                        }
                    }

                    if (stopping || !currentCoroutineContext().isActive) {
                        return@forEach
                    }

                    val executionJob = scope.launch(start = CoroutineStart.LAZY) {
                        val selfJob = coroutineContext[Job]!!
                        try {
                            executeJobWithHeartbeat(execution, jobDef, config.executionLeaseDuration, claimOwner)
                        } finally {
                            activeJobs.remove(execution.id, selfJob)
                            activeClaimOwners.remove(execution.id, claimOwner)
                        }
                    }
                    activeJobs.replace(execution.id, placeholderJob, executionJob)
                    placeholderJob.complete()
                    if (stopping || !executionJob.start()) {
                        executionJob.cancel("Scheduler stopping")
                        activeJobs.remove(execution.id, executionJob)
                        activeClaimOwners.remove(execution.id, claimOwner)
                    } else {
                        launched = true
                    }
                } finally {
                    if (!launched) {
                        placeholderJob.complete()
                        activeJobs.remove(execution.id, placeholderJob)
                        activeClaimOwners.remove(execution.id, claimOwner)
                        try {
                            withContext(NonCancellable) {
                                store.updateExecutionStatus(
                                    execution.id,
                                    ExecutionStatus.PENDING,
                                    "Released during scheduler shutdown",
                                    workerId = claimOwner
                                )
                            }
                        } catch (ex: Exception) {
                            log.error("Failed to release unclaimed execution ${execution.id}", ex)
                        }
                    }
                }
            }
        }
    }

    private suspend fun isAnyOtherExecutionHoldingLock(currentExecutionId: UUID, lockKey: String): Boolean {
        return store.isLockHeld(lockKey, excludeExecutionId = currentExecutionId)
    }

    private suspend fun handleMisfire(execution: JobExecution, jobDef: JobDefinition, now: Instant, claimOwner: String) {
        log.info("[${execution.jobId}] Misfire detected (scheduled at ${execution.scheduledAt}), ignoring due to IGNORE policy")
        if (!store.updateExecutionStatus(execution.id, ExecutionStatus.MISFIRED, "Misfire threshold exceeded", workerId = claimOwner)) {
            return
        }
        
        // Schedule next run
        val next = jobDef.trigger.nextExecutionTime(now)
            ?.truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
        if (next != null) {
            val deterministicId = UUID.nameUUIDFromBytes("${jobDef.id}:$next".toByteArray())
            if (store.getExecution(deterministicId) == null) {
                store.saveExecution(
                    JobExecution(
                        id = deterministicId,
                        jobId = jobDef.id,
                        scheduledAt = next,
                        lockKey = jobDef.lockKey
                        // Do NOT propagate correlationId to next recurring run
                    )
                )
            }
        }
    }

    private suspend fun executeJobWithHeartbeat(
        execution: JobExecution,
        jobDef: JobDefinition,
        leaseDuration: Duration,
        claimOwner: String
    ) {
        coroutineScope {
            val heartbeatJob = launch {
                while (isActive) {
                    delay(heartbeatInterval.toMillis())
                    try {
                        // Check if we are still RUNNING/CLAIMED (not superseded) while heartbeating
                        if (!store.heartbeat(execution.id, claimOwner, leaseDuration)) {
                            log.warn("[${execution.jobId}] Failed to heartbeat for execution ${execution.id}, it might have been reclaimed or superseded")
                            this@coroutineScope.cancel("Execution ${execution.id} is no longer valid (superseded or reclaimed)")
                            break
                        }
                    } catch (e: Exception) {
                        log.error("[${execution.jobId}] Error during heartbeat for execution ${execution.id}", e)
                    }
                }
            }

            try {
                executeJob(execution, jobDef, claimOwner)
            } finally {
                heartbeatJob.cancel()
            }
        }
    }

    private suspend fun executeJob(execution: JobExecution, jobDef: JobDefinition, claimOwner: String) {
        val correlationId = execution.correlationId ?: execution.id.toString()
        withContext(MDCContext(mapOf("correlationId" to correlationId))) {
            try {
                log.info("[${execution.jobId}] [$correlationId] Executing job (execution: ${execution.id})")
                if (!store.updateExecutionStatus(execution.id, ExecutionStatus.RUNNING, workerId = claimOwner)) {
                    log.warn("[${execution.jobId}] [$correlationId] Execution ${execution.id} is no longer owned by $claimOwner before start; aborting")
                    return@withContext
                }

                val handler = handlerRegistry.get(jobDef.id) 
                    ?: throw IllegalStateException("No handler registered for job ${jobDef.id}")
                
                if (jobDef.timeout != null) {
                    withTimeout(jobDef.timeout.toMillis()) {
                        handler(execution.payload)
                    }
                } else {
                    handler(execution.payload)
                }

                if (store.updateExecutionStatus(execution.id, ExecutionStatus.SUCCESS, workerId = claimOwner)) {
                    log.info("[${execution.jobId}] [$correlationId] Job finished successfully")
                    scheduleNextRunIfRecurring(execution, jobDef)
                } else {
                    log.warn("[${execution.jobId}] [$correlationId] Execution ${execution.id} lost ownership before completion; skipping SUCCESS and next-run scheduling")
                }
            } catch (e: Exception) {
                if (e is CancellationException && e !is TimeoutCancellationException) {
                    log.info("[${execution.jobId}] [$correlationId] Job was cancelled (superseded or shutdown)")
                    // We don't update status to FAILED here if it was already updated to SUPERSEDED
                    // But to be safe, we can ensure it's not DEAD_LETTERED
                    return@withContext
                }
                
                log.error("[${execution.jobId}] [$correlationId] Job failed", e)
                val nextAttempt = execution.attempt + 1
                if (nextAttempt < jobDef.retryPolicy.maxAttempts) {
                    val nextDelay = jobDef.retryPolicy.calculateDelay(nextAttempt)
                    val nextRun = Instant.now(clock).plus(nextDelay)

                    if (store.updateExecutionStatus(execution.id, ExecutionStatus.FAILED, e.message, workerId = claimOwner)) {
                        // Create a new execution for the retry
                        store.saveExecution(
                            JobExecution(
                                jobId = execution.jobId,
                                scheduledAt = nextRun,
                                attempt = nextAttempt,
                                payload = execution.payload,
                                lockKey = jobDef.lockKey,
                                correlationId = correlationId // Propagate correlationId to retries of the SAME execution
                            )
                        )
                        log.info("[${execution.jobId}] [$correlationId] Scheduled retry at $nextRun (attempt $nextAttempt)")
                    } else {
                        log.warn("[${execution.jobId}] [$correlationId] Execution ${execution.id} lost ownership before failure transition; skipping retry scheduling")
                    }
                } else {
                    if (store.updateExecutionStatus(execution.id, ExecutionStatus.DEAD_LETTERED, e.message, workerId = claimOwner)) {
                        log.warn("[${execution.jobId}] [$correlationId] Reached max attempts (${jobDef.retryPolicy.maxAttempts}) and is now DEAD_LETTERED")
                        scheduleNextRunIfRecurring(execution, jobDef)
                    } else {
                        log.warn("[${execution.jobId}] [$correlationId] Execution ${execution.id} lost ownership before DEAD_LETTERED transition; skipping next-run scheduling")
                    }
                }
            }
        }
    }

    private suspend fun scheduleNextRunIfRecurring(execution: JobExecution, jobDef: JobDefinition) {
        // Use plusMillis(1) to ensure we look for the NEXT execution after this one,
        // which is important for inclusive triggers like OneTimeTrigger.
        val next = jobDef.trigger.nextExecutionTime(execution.scheduledAt.plusMillis(1))
            ?.truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
        if (next != null) {
            val deterministicId = UUID.nameUUIDFromBytes("${jobDef.id}:$next".toByteArray())
            if (store.getExecution(deterministicId) == null) {
                store.saveExecution(
                    JobExecution(
                        id = deterministicId,
                        jobId = jobDef.id,
                        scheduledAt = next,
                        lockKey = jobDef.lockKey
                        // Do NOT propagate correlationId to next recurring run
                    )
                )
            }
        }
    }

    suspend fun stop() {
        log.info("Stopping Khrona Scheduler")
        stopping = true
        job?.cancel() // Stop polling for new jobs
        job = null
        
        if (activeJobs.isNotEmpty()) {
            val remainingOwners = activeJobs.keys.associateWith { activeClaimOwners[it] }
            try {
                withTimeout(config.shutdownTimeout.toMillis()) {
                    log.info("Waiting up to ${config.shutdownTimeout} for ${activeJobs.size} active jobs to complete...")
                    activeJobs.values.joinAll()
                    log.info("All active jobs completed cleanly during shutdown.")
                }
            } catch (e: TimeoutCancellationException) {
                log.warn("Shutdown timeout reached. Cancelling ${activeJobs.size} remaining jobs.")
                activeJobs.values.forEach { it.cancel("Scheduler stopping (timeout)") }
                
                // Wait for cancellations to finish
                activeJobs.values.joinAll()
                
                // Explicitly release any executions that didn't complete normally and are still owned by this claim
                remainingOwners.forEach { (executionId, claimOwner) ->
                    try {
                        val currentExec = store.getExecution(executionId)
                        if (currentExec?.status == ExecutionStatus.CLAIMED || currentExec?.status == ExecutionStatus.RUNNING) {
                            log.info("Releasing execution $executionId back to PENDING due to shutdown")
                            store.updateExecutionStatus(
                                executionId,
                                ExecutionStatus.PENDING,
                                "Released during scheduler shutdown",
                                workerId = claimOwner
                            )
                        }
                    } catch (ex: Exception) {
                        log.error("Failed to release execution $executionId during shutdown", ex)
                    }
                }
            }
        }
        
        activeJobs.clear()
        activeClaimOwners.clear()
        log.info("Khrona Scheduler stopped")
    }

    suspend fun registerJob(jobDef: JobDefinition) {
        validateJob(jobDef)
        handlerRegistry.register(jobDef.id, jobDef.handler)
        val correlationId = MDC.get("correlationId")
        store.saveJob(jobDef)
        // Schedule the first execution if it's a recurring/deferred job
        val now = Instant.now(clock).truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
        val next = if (jobDef.trigger is OneTimeTrigger) {
            jobDef.trigger.nextExecutionTime(Instant.EPOCH)
        } else {
            jobDef.trigger.nextExecutionTime(now)
        }
        if (next != null) {
            val deterministicId = UUID.nameUUIDFromBytes("${jobDef.id}:$next".toByteArray())
            if (store.getExecution(deterministicId) == null) {
                store.saveExecution(
                    JobExecution(
                        id = deterministicId,
                        jobId = jobDef.id,
                        scheduledAt = next,
                        lockKey = jobDef.lockKey,
                        correlationId = correlationId ?: deterministicId.toString()
                    )
                )
            }
        }
    }

    /**
     * Manually triggers a job for immediate execution.
     * The job must have been previously registered.
     */
    suspend fun trigger(jobId: String, payload: Any? = null) {
        val correlationId = MDC.get("correlationId")
        val jobDef = store.getJob(jobId) ?: throw IllegalArgumentException("Job $jobId not found")
        val now = Instant.now(clock).truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
        val executionId = UUID.randomUUID()
        store.saveExecution(
            JobExecution(
                id = executionId,
                jobId = jobId,
                scheduledAt = now,
                payload = payload,
                lockKey = jobDef.lockKey,
                correlationId = correlationId ?: executionId.toString()
            )
        )
    }
}
