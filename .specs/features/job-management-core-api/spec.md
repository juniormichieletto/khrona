# Job Management Core API

## Goal

Expose reusable core functions for applications to inspect and control Khrona jobs. Khrona itself must not ship built-in REST routes, admin UI, dashboards, or route security policy. Host applications can wrap these functions in their own endpoints, screens, CLIs, or internal health tooling when they choose to expose them.

## Requirements

- **REQ-JM1: Job Listing:** Applications can list registered and persisted jobs through `khrona-core`.
- **REQ-JM2: Job Overview:** Applications can retrieve a single job overview by job ID, including definition metadata and derived runtime state.
- **REQ-JM3: Status-Derived Progress:** Applications can inspect job progress using persisted execution state: latest execution, active executions, next pending execution, status counts, timestamps, attempt, worker, and error fields.
- **REQ-JM4: History Listing:** Applications can list execution history for a job using bounded query options.
- **REQ-JM5: Manual Start:** Applications can manually start a registered job with an optional payload.
- **REQ-JM6: Pause And Resume:** Applications can pause and resume a job without deleting its definition or execution history.
- **REQ-JM7: Paused Scheduling:** Paused jobs must not claim or execute pending work. Pending executions remain persisted while paused.
- **REQ-JM8: Resume Behavior:** Resumed jobs process existing pending work through the normal scheduler and misfire rules.
- **REQ-JM9: Local Stop:** Applications can stop executions that are active in the current scheduler process.
- **REQ-JM10: Stop Status:** A successful manual stop must persist an explicit terminal execution status.
- **REQ-JM11: Store Portability:** Management queries must work across Memory, JDBC, Redis, and test stores.
- **REQ-JM12: Shutdown Checkpoint:** Job handlers can call a Khrona-provided checkpoint function at safe points. When scheduler shutdown has started, the checkpoint prevents the handler from continuing past that point.
- **REQ-JM13: Checkpoint Persistence Semantics:** A handler stopped by a shutdown checkpoint must not be marked `SUCCESS` or terminal `CANCELLED`. Khrona must persist or release the execution so durable stores can run it again after restart according to the existing at-least-once model.
- **REQ-JM14: Core-Only Boundary:** The implementation is core-only. Ktor routes, REST endpoints, dashboards, admin UI, and route security policy are owned by host applications, not Khrona.

## Non-Goals

- Built-in REST routes, Ktor route helpers, dashboards, or admin UI.
- RBAC, authentication, authorization, tenant policy, or route security guidance beyond stating that host applications own it.
- Cross-node stop commands.
- Handler-reported percent, checkpoint, or message progress.
- Deleting, compacting, replaying, or cleaning up old history.
- Database-specific notification or wake-up integrations.

## Success Criteria

- A host app can call Khrona core APIs to list jobs, inspect status, start a job, pause/resume a job, stop local active work, and list history.
- Pause state is persisted in the shared job definition so all scheduler instances using the same store observe it.
- Stop behavior is explicit about being local to the current scheduler instance.
- Long-running handlers can opt into graceful-shutdown safe points without depending directly on Kotlin coroutine cancellation timing.
- The shared store contract tests cover the execution query behavior for every built-in store.
