package io.khrona.core

import java.util.concurrent.ConcurrentHashMap

class HandlerRegistry {
    private val handlers = ConcurrentHashMap<String, JobHandler>()

    fun register(jobId: String, handler: JobHandler) {
        handlers[jobId] = handler
    }

    fun get(jobId: String): JobHandler? {
        return handlers[jobId]
    }

    fun remove(jobId: String) {
        handlers.remove(jobId)
    }
}
