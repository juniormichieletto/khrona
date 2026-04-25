package io.khrona.core

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
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

        // Validate jobs from config
        config.jobs.forEach { validateJob(it) }
        
        job = scope.launch {
            // Register jobs from config into store
            config.jobs.forEach { store.saveJob(it) }
            
            // Basic initial scheduling: for each job, create its first execution if none exist
            config.jobs.forEach { jobDef ->
                val next = jobDef.trigger.nextExecutionTime(Instant.now(clock))
                if (next != null) {
                    store.saveExecution(JobExecution(jobId = jobDef.id, scheduledAt = next, lockKey = jobDef.lockKey))
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
        }
    }

    internal suspend fun pollAndExecute() {
        val now = Instant.now(clock)
        val eligible = store.listEligibleExecutions(now)
        
        eligible.forEach { execution ->
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
                scope.launch {
                    executeJobWithHeartbeat(execution, jobDef, leaseDuration)
                }
            }
        }
    }

    private suspend fun handleMisfire(execution: JobExecution, jobDef: JobDefinition) {
        log.info("[${execution.jobId}] Misfire detected (scheduled at ${execution.scheduledAt}), ignoring due to IGNORE policy")
        store.updateExecutionStatus(execution.id, ExecutionStatus.MISFIRED, "Misfire threshold exceeded")
        
        // Schedule next run
        val next = jobDef.trigger.nextExecutionTime(Instant.now(clock))
        if (next != null) {
            store.saveExecution(JobExecution(jobId = jobDef.id, scheduledAt = next, lockKey = jobDef.lockKey))
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
        try {
            log.info("[${execution.jobId}] Executing job (execution: ${execution.id})")
            store.updateExecutionStatus(execution.id, ExecutionStatus.RUNNING)
            
            jobDef.handler(execution.payload)
            
            store.updateExecutionStatus(execution.id, ExecutionStatus.SUCCESS)
            
            // Schedule next run
            val next = jobDef.trigger.nextExecutionTime(Instant.now(clock))
            if (next != null) {
                store.saveExecution(JobExecution(jobId = jobDef.id, scheduledAt = next, lockKey = jobDef.lockKey))
            }
        } catch (e: Exception) {
            log.error("[${execution.jobId}] Job failed", e)
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
                        lockKey = jobDef.lockKey
                    )
                )
                log.info("[${execution.jobId}] Scheduled retry at $nextRun (attempt $nextAttempt)")
            } else {
                store.updateExecutionStatus(execution.id, ExecutionStatus.DEAD_LETTERED, e.message)
                log.warn("[${execution.jobId}] Reached max attempts (${jobDef.retryPolicy.maxAttempts}) and is now DEAD_LETTERED")
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
        scope.launch {
            store.saveJob(jobDef)
            // Schedule the first execution if it's a recurring/deferred job
            val next = jobDef.trigger.nextExecutionTime(Instant.now(clock))
            if (next != null) {
                store.saveExecution(JobExecution(jobId = jobDef.id, scheduledAt = next, lockKey = jobDef.lockKey))
            }
        }
    }
}
