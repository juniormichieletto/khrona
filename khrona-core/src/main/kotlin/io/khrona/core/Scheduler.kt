package io.khrona.core

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.UUID

class Scheduler(
    private val config: KhronaConfig,
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob()),
    private val clock: Clock = Clock.systemUTC()
) {
    private val log = LoggerFactory.getLogger(Scheduler::class.java)
    private val store = config.store ?: throw IllegalStateException("JobStore must be configured")
    private val workerId = UUID.randomUUID().toString()
    private var job: Job? = null

    fun start() {
        if (job != null) return
        log.info("Starting Khrona Scheduler (workerId: $workerId)")
        
        job = scope.launch {
            // Register jobs from config into store
            config.jobs.forEach { store.saveJob(it) }
            
            // Basic initial scheduling: for each job, create its first execution if none exist
            // This is a simplification for v0.1
            config.jobs.forEach { jobDef ->
                val next = jobDef.trigger.nextExecutionTime(Instant.now(clock))
                if (next != null) {
                    store.saveExecution(JobExecution(jobId = jobDef.id, scheduledAt = next))
                }
            }

            while (isActive) {
                try {
                    pollAndExecute()
                } catch (e: Exception) {
                    log.error("Error in scheduler loop", e)
                }
                delay(1000) // Poll every second
            }
        }
    }

    private suspend fun pollAndExecute() {
        val now = Instant.now(clock)
        val eligible = store.listEligibleExecutions(now)
        
        eligible.forEach { execution ->
            if (store.claimExecution(execution.id, workerId, Duration.ofMinutes(5))) {
                scope.launch {
                    executeJob(execution)
                }
            }
        }
    }

    private suspend fun executeJob(execution: JobExecution) {
        val jobDef = store.getJob(execution.jobId) ?: return
        try {
            log.info("Executing job ${execution.jobId} (execution: ${execution.id})")
            store.updateExecutionStatus(execution.id, ExecutionStatus.RUNNING)
            
            jobDef.handler(execution.payload)
            
            store.updateExecutionStatus(execution.id, ExecutionStatus.SUCCESS)
            
            // Schedule next run
            val next = jobDef.trigger.nextExecutionTime(Instant.now(clock))
            if (next != null) {
                store.saveExecution(JobExecution(jobId = jobDef.id, scheduledAt = next))
            }
        } catch (e: Exception) {
            log.error("Job ${execution.jobId} failed", e)
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
                        payload = execution.payload
                    )
                )
                log.info("Scheduled retry for job ${execution.jobId} at $nextRun (attempt $nextAttempt)")
            } else {
                store.updateExecutionStatus(execution.id, ExecutionStatus.DEAD_LETTERED, e.message)
                log.warn("Job ${execution.jobId} reached max attempts (${jobDef.retryPolicy.maxAttempts}) and is now DEAD_LETTERED")
            }
        }
    }

    fun stop() {
        log.info("Stopping Khrona Scheduler")
        job?.cancel()
        job = null
    }

    fun registerJob(jobDef: JobDefinition) {
        scope.launch {
            store.saveJob(jobDef)
            // Schedule the first execution if it's a recurring/deferred job
            val next = jobDef.trigger.nextExecutionTime(Instant.now(clock))
            if (next != null) {
                store.saveExecution(JobExecution(jobId = jobDef.id, scheduledAt = next))
            }
        }
    }
}
