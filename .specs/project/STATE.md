# STATE — Khrona

## Decisions
- **Structure:** Transitioned from `.spec/SPEC.md` to `.specs/` structure for TLC spec-driven workflow.
- **Guarantee:** Explicitly settled on **at-least-once** execution; removed "exactly-once" to avoid distributed systems fallacies.
- **Storage:** `MemoryJobStore` is a first-class citizen for single-instance/dev/test use cases.
- **v0.1 Completion:** Implemented core engine, in-memory store, and Ktor integration.
- **v0.2 Completion:** Implemented JDBC/Postgres store, retries, and durable scheduling.
- **v0.3 Completion:** Implemented lease-based claiming, distributed locking (FORBID), and stale worker recovery.
- **v0.3.1 Hardening:** Implemented handler registry, resilient recurring schedules, enforced timeouts, suspendable APIs, structured JDBC payloads, fail-fast migrations, and safer REPLACE ordering.

## Blockers
- Production readiness review found runtime hardening blockers before the project should be described as production-ready:
  - Blocking JDBC work currently runs inside suspend APIs without dispatcher isolation.
  - Scheduler shutdown cancels active jobs but relies on lease expiry for recovery.
  - Lease duration is fixed instead of configurable.
  - Eligible execution polling is unbounded.
  - Ktor quick-start documentation does not compile as written.

## Todos
- [x] Map existing codebase (if any) to `.specs/codebase/`.
- [x] Initialize the Gradle/Kotlin project structure.
- [x] Implement JDBC/Postgres Store (v0.2)
- [x] Implement Multi-node Claiming (v0.3)
- [x] Implement Reliability Hardening (v0.3.1)
- [ ] Implement Production Readiness Hardening (v0.3.2)
- [ ] Implement Admin API & Visibility (v0.4)

## Next Tasks
- [x] Fix Ktor quick-start/API mismatch.
- [ ] Move blocking JDBC work off scheduler/application dispatchers.
- [x] Add graceful scheduler shutdown with bounded wait and explicit execution release/marking.
- [ ] Make lease duration and heartbeat behavior configurable and validated.
- [ ] Add bounded JDBC polling/claiming.
- [ ] Validate retry policy settings and correct fractional backoff factors.
- [ ] Remove silent unsupported JDBC payload fallback behavior.

## Deferred Ideas
- Payload versioning/evolution (tracked in Future Considerations).
- Advanced RBAC for Admin API.
- Dynamic Lock Keys.
- Cross-database adaptive scheduler delay with bounded fallback polling.
- Optional database-native wake-up mechanisms after the portable adaptive loop exists.
