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
    SUPERSEDED,
    CANCELLED;

    val isTerminal: Boolean
        get() = when (this) {
            SUCCESS,
            FAILED,
            DEAD_LETTERED,
            MISFIRED,
            SUPERSEDED,
            CANCELLED -> true
            PENDING,
            CLAIMED,
            RUNNING -> false
        }
}
