package io.khrona.store.memory

import io.khrona.core.ExecutionStatus
import io.khrona.core.JobDefinition
import io.khrona.core.JobExecution
import io.khrona.core.IntervalTrigger
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.*
import kotlinx.coroutines.test.runTest

class MemoryJobStoreTest {

    @Test
    fun `should save and retrieve jobs`() = runTest {
        val store = MemoryJobStore()
        val job = JobDefinition(
            id = "test-job",
            handler = {},
            trigger = IntervalTrigger(Duration.ofMinutes(1))
        )
        
        store.saveJob(job)
        
        val retrieved = store.getJob("test-job")
        assertNotNull(retrieved)
        assertEquals("test-job", retrieved?.id)
    }

    @Test
    fun `should list eligible executions`() = runTest {
        val store = MemoryJobStore()
        val now = Instant.now()
        
        val execution1 = JobExecution(jobId = "job1", scheduledAt = now.minusSeconds(10))
        val execution2 = JobExecution(jobId = "job2", scheduledAt = now.plusSeconds(10))
        
        store.saveExecution(execution1)
        store.saveExecution(execution2)
        
        val eligible = store.listEligibleExecutions(now)
        
        assertEquals(1, eligible.size)
        assertEquals(execution1.id, eligible[0].id)
    }

    @Test
    fun `should claim execution only once`() = runTest {
        val store = MemoryJobStore()
        val execution = JobExecution(jobId = "job1", scheduledAt = Instant.now())
        store.saveExecution(execution)
        
        val claimed1 = store.claimExecution(execution.id, "worker1", Duration.ofMinutes(1))
        val claimed2 = store.claimExecution(execution.id, "worker2", Duration.ofMinutes(1))
        
        assertTrue(claimed1)
        assertFalse(claimed2)
        
        val updated = store.getExecution(execution.id)
        assertEquals(ExecutionStatus.CLAIMED, updated?.status)
        assertEquals("worker1", updated?.workerId)
    }

    @Test
    fun `should update execution status`() = runTest {
        val store = MemoryJobStore()
        val execution = JobExecution(jobId = "job1", scheduledAt = Instant.now())
        store.saveExecution(execution)
        
        store.updateExecutionStatus(execution.id, ExecutionStatus.SUCCESS)
        
        val updated = store.getExecution(execution.id)
        assertEquals(ExecutionStatus.SUCCESS, updated?.status)
        assertNotNull(updated?.completedAt)
    }
}
