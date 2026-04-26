package io.khrona.core

import kotlinx.coroutines.*
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.UUID

class Scheduler(
    val config: KhronaConfig,
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob()),
    private val clock: Clock = Clock.systemUTC()
) {
    private val log = LoggerFactory.getLogger(Scheduler::class.java)
    private val store = config.store ?: throw IllegalStateException("JobStore must be configured")
    private val workerId = UUID.randomUUID().toString()
    private var job: Job? = null

    fun start() {
        if (job != null) return
        log.info("Starting Khrona Scheduler (workerId: $workerId, pollingInterval: ${config.pollingInterval})")

        val correlationId = MDC.get("correlationId")

        // Validate jobs from config
        config.jobs.forEach { validateJob(it) }
        
        job = scope.launch {
            // Register jobs from config into store
            config.jobs.forEach { store.saveJob(it) }
            
            // Basic initial scheduling: for each job, create its first execution if none exist
            config.jobs.forEach { jobDef ->
                // Truncate to seconds to ensure deterministic UUID matching across nodes
                val now = Instant.now(clock).truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
                val next = jobDef.trigger.nextExecutionTime(now)
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
            while (isActive) {
                try {
                    val now = Instant.now(clock)
                    // Periodic recovery of stale executions
                    if (Duration.between(lastRecovery, now) > Duration.ofMinutes(1)) {
                        val recovered = store.resetExpiredExecutions(now)
                        if (recovered > 0) log.info("Recovered $recovered stale executions")
                        lastRecovery = now
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
        val eligible = store.listEligibleExecutions(now)
        
        eligible.forEach { execution ->
            // Re-fetch job def in case it changed or for fresh state
            val jobDef = store.getJob(execution.jobId) ?: return@forEach
            
            // Check for misfire
            if (Duration.between(execution.scheduledAt, now) > config.misfireThreshold) {
                if (jobDef.misfirePolicy == MisfirePolicy.IGNORE) {
                    handleMisfire(execution, jobDef)
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

            // TODO: Configurable lease duration
            val leaseDuration = Duration.ofMinutes(5)
            if (store.claimExecution(execution.id, workerId, leaseDuration)) {
                // Double-check lock AFTER claiming but before launching
                // This handles the race where two executions for the same lock were claimed 
                // by different nodes (if duplicate executions existed) or by the same node.
                if (jobDef.concurrencyPolicy == ConcurrencyPolicy.FORBID && jobDef.lockKey != null) {
                    // We need to be careful here: isLockHeld will see OURSELVES if we just claimed it.
                    // But isLockHeld currently checks if ANY execution for that key is CLAIMED/RUNNING.
                    // To be safe, we should check if any OTHER execution is holding the lock.
                    if (isAnyOtherExecutionHoldingLock(execution.id, jobDef.lockKey)) {
                        log.warn("[${execution.jobId}] Release claim for ${execution.id} because another execution already holds the lock ${jobDef.lockKey}")
                        store.updateExecutionStatus(execution.id, ExecutionStatus.PENDING) // Release it
                        return@forEach
                    }
                }

                scope.launch {
                    executeJobWithHeartbeat(execution, jobDef, leaseDuration)
                }
            }
        }
    }

    private suspend fun isAnyOtherExecutionHoldingLock(currentExecutionId: UUID, lockKey: String): Boolean {
        return store.isLockHeld(lockKey, excludeExecutionId = currentExecutionId)
    }

    private suspend fun handleMisfire(execution: JobExecution, jobDef: JobDefinition) {
        log.info("[${execution.jobId}] Misfire detected (scheduled at ${execution.scheduledAt}), ignoring due to IGNORE policy")
        store.updateExecutionStatus(execution.id, ExecutionStatus.MISFIRED, "Misfire threshold exceeded")
        
        // Schedule next run
        val next = jobDef.trigger.nextExecutionTime(Instant.now(clock))
        if (next != null) {
            store.saveExecution(
                JobExecution(
                    jobId = jobDef.id,
                    scheduledAt = next,
                    lockKey = jobDef.lockKey
                    // Do NOT propagate correlationId to next recurring run
                )
            )
        }
    }

    private suspend fun executeJobWithHeartbeat(execution: JobExecution, jobDef: JobDefinition, leaseDuration: Duration) {
        coroutineScope {
            val heartbeatJob = launch {
                while (isActive) {
                    delay(leaseDuration.toMillis() / 2)
                    try {
                        if (!store.heartbeat(execution.id, leaseDuration)) {
                            log.warn("[${execution.jobId}] Failed to heartbeat for execution ${execution.id}, it might have been reclaimed")
                            break
                        }
                    } catch (e: Exception) {
                        log.error("[${execution.jobId}] Error during heartbeat for execution ${execution.id}", e)
                    }
                }
            }

            try {
                executeJob(execution, jobDef)
            } finally {
                heartbeatJob.cancel()
            }
        }
    }

    private suspend fun executeJob(execution: JobExecution, jobDef: JobDefinition) {
        val correlationId = execution.correlationId ?: execution.id.toString()
        withContext(MDCContext(mapOf("correlationId" to correlationId))) {
            try {
                log.info("[${execution.jobId}] [$correlationId] Executing job (execution: ${execution.id})")
                store.updateExecutionStatus(execution.id, ExecutionStatus.RUNNING)

                jobDef.handler(execution.payload)

                store.updateExecutionStatus(execution.id, ExecutionStatus.SUCCESS)
                log.info("[${execution.jobId}] [$correlationId] Job finished successfully")

                // Schedule next run
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
            } catch (e: Exception) {
                log.error("[${execution.jobId}] [$correlationId] Job failed", e)
                val nextAttempt = execution.attempt + 1
                if (nextAttempt < jobDef.retryPolicy.maxAttempts) {
                    val nextDelay = jobDef.retryPolicy.calculateDelay(nextAttempt)
                    val nextRun = Instant.now(clock).plus(nextDelay)

                    store.updateExecutionStatus(execution.id, ExecutionStatus.FAILED, e.message)

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
                    store.updateExecutionStatus(execution.id, ExecutionStatus.DEAD_LETTERED, e.message)
                    log.warn("[${execution.jobId}] [$correlationId] Reached max attempts (${jobDef.retryPolicy.maxAttempts}) and is now DEAD_LETTERED")
                }
            }
        }
    }

    fun stop() {
        log.info("Stopping Khrona Scheduler")
        job?.cancel()
        job = null
    }

    fun registerJob(jobDef: JobDefinition) {
        validateJob(jobDef)
        val correlationId = MDC.get("correlationId")
        scope.launch {
            store.saveJob(jobDef)
            // Schedule the first execution if it's a recurring/deferred job
            val now = Instant.now(clock).truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
            val next = jobDef.trigger.nextExecutionTime(now)
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
    }

    /**
     * Manually triggers a job for immediate execution.
     * The job must have been previously registered.
     */
    fun trigger(jobId: String, payload: Any? = null) {
        val correlationId = MDC.get("correlationId")
        scope.launch {
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
}
