package io.khrona.core

enum class ExecutionStatus {
    PENDING,
    CLAIMED,
    RUNNING,
    SUCCESS,
    FAILED,
    CANCELLED,
    DEAD_LETTERED
}
