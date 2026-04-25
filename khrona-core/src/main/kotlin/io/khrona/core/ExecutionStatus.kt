package io.khrona.core

import kotlinx.serialization.Serializable

@Serializable
enum class ExecutionStatus {
    PENDING,
    CLAIMED,
    RUNNING,
    SUCCESS,
    FAILED,
    CANCELLED,
    DEAD_LETTERED,
    MISFIRED
}
