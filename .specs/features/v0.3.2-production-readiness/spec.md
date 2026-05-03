# Spec: v0.3.2 - Production Readiness Hardening

## Overview
Khrona has the core scheduler, durable JDBC store, distributed claiming, and v0.3.1 reliability fixes in place. This phase closes the remaining production-readiness gaps found during review: blocking JDBC work inside suspend APIs, fixed lease settings, unbounded polling, weak retry validation, silent payload fallback behavior, and stale project state around completed hardening work.

## Objectives
- Keep scheduler and Ktor coroutine dispatchers from being blocked by JDBC calls.
- Make lease and heartbeat behavior configurable, validated, and safe by default.
- Process large JDBC backlogs in bounded batches instead of loading every eligible execution.
- Validate retry policies before jobs are accepted.
- Preserve payload semantics by rejecting unsupported JDBC payloads unless the caller explicitly provides supported JSON-compatible values.
- Align roadmap and state docs with the implemented shutdown and Ktor quick-start fixes.

## Requirements
- **REQ-P1: JDBC Dispatcher Isolation:** All blocking JDBC operations must execute on a dedicated dispatcher, with an injectable dispatcher for tests and advanced integrations.
- **REQ-P2: Lease Configuration:** Scheduler configuration must expose execution lease duration and heartbeat interval or heartbeat ratio settings.
- **REQ-P3: Lease Validation:** Scheduler startup and job registration paths must reject invalid lease and heartbeat settings, including non-positive durations and heartbeat intervals that cannot refresh the lease safely.
- **REQ-P4: Bounded Polling:** Scheduler polling must claim/process eligible executions in bounded batches. JDBC queries must enforce the configured batch size across H2, PostgreSQL, MySQL, and Oracle.
- **REQ-P5: Retry Validation:** Retry policies must reject invalid `maxAttempts`, negative delays, `maxDelay < initialDelay`, non-positive factors, and invalid jitter ranges.
- **REQ-P6: Fractional Backoff:** Retry delay calculation must preserve fractional factors such as `1.5` instead of truncating the multiplier before applying it.
- **REQ-P7: Payload Fail-Fast:** JDBC payload serialization must fail fast for unsupported payload types and unsupported nested values instead of silently storing `toString()`.
- **REQ-P8: Documentation Consistency:** `.specs/project/ROADMAP.md` and `.specs/project/STATE.md` must accurately reflect which production-readiness blockers are complete and which remain open.
- **REQ-P9: Verification:** Each implementation task must follow TDD and finish with `./gradlew clean test`.
