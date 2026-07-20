# Spec: Dependent Job Triggers

## Overview

Khrona currently supports time-based triggers, one-time triggers, and manual triggering. Some applications need a simple way to schedule follow-up work after another Khrona job finishes, without embedding orchestration code inside the source job handler.

This feature adds completion-triggered jobs: a job can declare that it should be scheduled when another job reaches a configured terminal status. The feature must stay narrowly focused on job-to-job completion triggers and must not turn Khrona into a DAG or workflow engine.

## Objectives

- Let applications run job B after job A completes.
- Preserve Khrona's durable, at-least-once execution model.
- Keep dependency behavior portable across Memory, JDBC, Redis, and future stores.
- Make completion-triggered executions traceable back to the source execution.
- Avoid workflow-engine complexity such as fan-in, branching expressions, and payload mapping.

## Proposed API

```kotlin
job("fetch-prices") {
    every(5.minutes)
    execute {
        // fetch external prices
    }
}

job("recalculate-portfolio") {
    afterJob("fetch-prices")
    execute {
        // recalculate derived state
    }
}
```

The default trigger status should be `SUCCESS`:

```kotlin
afterJob("fetch-prices")
```

Applications can opt into other terminal statuses:

```kotlin
afterJob("fetch-prices", on = ExecutionStatus.DEAD_LETTERED)
afterJob("fetch-prices", on = setOf(ExecutionStatus.SUCCESS, ExecutionStatus.DEAD_LETTERED))
```

## Requirements

- **REQ-DJT1: Completion Trigger:** Khrona supports a trigger type that schedules a dependent job when a source job execution reaches a configured terminal status.
- **REQ-DJT2: Default Success Status:** `afterJob(sourceJobId)` defaults to triggering only after `SUCCESS`.
- **REQ-DJT3: Terminal Status Filtering:** Applications can configure one or more terminal statuses that should trigger the dependent job. Non-terminal statuses must not trigger dependent executions.
- **REQ-DJT4: One Dependent Per Source Execution:** Each matching completed source execution creates at most one dependent execution for each dependent job.
- **REQ-DJT5: Traceability:** Dependent executions store enough metadata to identify the source job ID and source execution ID that created them.
- **REQ-DJT6: Correlation Propagation:** Dependent executions propagate the source execution correlation ID by default so logs and operational views can trace job chains.
- **REQ-DJT7: Payload Boundary:** The first version does not transform or automatically copy arbitrary source payloads. Any payload behavior must be explicit and must not silently reuse mutable application objects.
- **REQ-DJT8: Durable Store Semantics:** With durable stores, dependent-trigger state must survive process restart and must not create duplicate dependent executions after a scheduler crash or multi-node race.
- **REQ-DJT9: Multi-Node Safety:** Multiple scheduler instances observing the same completed source execution must not enqueue duplicate dependent executions for the same dependent job.
- **REQ-DJT10: Cycle Prevention:** Khrona must reject direct self-dependencies and should detect simple dependency cycles at registration time when all involved job definitions are known locally.
- **REQ-DJT11: Existing Behavior Preservation:** Time-based, one-time, manual, retry, misfire, locking, pause/resume, local stop, and shutdown-checkpoint behavior must remain compatible with dependent triggers.
- **REQ-DJT12: Store Portability:** The implementation must work across Memory, JDBC, Redis, and test stores without requiring external queues, database triggers, or database-specific notification systems.
- **REQ-DJT13: Observability Integration:** Job management and history APIs should expose source execution metadata for dependent executions once both features exist.
- **REQ-DJT14: Documentation Boundary:** Documentation must describe dependent triggers as simple completion-triggered scheduling, not full workflow orchestration.

## Non-Goals

- Full DAG or workflow orchestration.
- Branching condition expressions.
- Fan-in semantics such as "run after all of these jobs finish".
- Fan-out orchestration beyond multiple independent jobs depending on the same source job.
- Payload transformation, templating, or mapping between jobs.
- Exactly-once workflow guarantees.
- Cross-job transaction boundaries.
- Built-in compensation, saga, or rollback behavior.
- Visual workflow design tools.
- External event bus or message queue integration.

## Success Criteria

- A dependent job can be registered with `afterJob("source")` and runs after the source job succeeds.
- A dependent job configured for `DEAD_LETTERED` runs only when the source execution reaches that status.
- A source job that runs 10 times can create 10 traceable dependent executions, one per matching source execution.
- A multi-node test proves two scheduler instances do not create duplicate dependent executions for the same source execution and dependent job.
- A restart test proves durable stores do not lose pending dependent work or replay already-created dependent executions.
- Job history exposes the relationship between a dependent execution and the source execution that triggered it.
- Documentation keeps the feature explicitly outside workflow-engine scope.

## Open Questions

- Should dependent executions receive `payload = null` by default, or should the source payload be opt-in copyable only for JSON-compatible payloads?
- Should dependent triggers run when a manually stopped execution becomes `CANCELLED`, or should `CANCELLED` be excluded unless explicitly requested?
- Should cycle detection be limited to locally registered definitions, or should it inspect persisted job definitions as well?
- Should dependent enqueue happen synchronously in the source scheduler's completion path, or through a store-level scan for newly terminal executions?
