package io.khrona.store.memory

import io.khrona.core.JobStore
import io.khrona.core.testing.JobStoreContract

class MemoryJobStoreTest : JobStoreContract {
    override fun createStore(): JobStore = MemoryJobStore()
}
