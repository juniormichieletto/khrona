package io.khrona.core

import kotlinx.serialization.Serializable

@Serializable
enum class ExecutionStatus {
    PENDING,
    CLAIMED,
    RUNNING,
    SUCCESS,
    FAILED,
    DEAD_LETTERED,
    MISFIRED,
    SUPERSEDED
}
