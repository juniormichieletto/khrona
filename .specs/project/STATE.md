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
- None. v0.3.2 hardening addresses previously identified production-readiness blockers, including validation and lifecycle isolation gaps found in review.

## Todos
- [x] Map existing codebase (if any) to `.specs/codebase/`.
- [x] Initialize the Gradle/Kotlin project structure.
- [x] Implement JDBC/Postgres Store (v0.2)
- [x] Implement Multi-node Claiming (v0.3)
- [x] Implement Reliability Hardening (v0.3.1)
- [x] Add Production Readiness Hardening implementation plan (v0.3.2)
- [x] Implement Production Readiness Hardening (v0.3.2)
- [x] Add Android SQLite Store implementation plan (v0.6)
- [ ] Implement Android SQLite Store (v0.6)
- [ ] Implement Admin API & Visibility (v0.4)

## Next Tasks
- [ ] Implement Android SQLite Store (v0.6)
- [ ] Implement Admin API & Dashboard (v0.4)
- [ ] Implement Metrics (Micrometer) (v0.4)
- [ ] Add lock inspection capabilities (v0.4)
- [ ] Add shutdown/cancellation contract hardening to a future reliability release
- [ ] Add timezone support, DLQ tooling enhancements, and job pause/disable to future production features

## Deferred Ideas
- Payload versioning/evolution (tracked in Future Considerations).
- Advanced RBAC for Admin API.
- Dynamic Lock Keys.
- Shutdown/cancellation contract tests and claimed-before-active edge hardening.
- Timezone-aware cron scheduling.
- Dead-letter queue inspection, replay, and cleanup tooling.
- Job pause/disable support.
- Cross-database adaptive scheduler delay with bounded fallback polling.
- Optional database-native wake-up mechanisms after the portable adaptive loop exists.
- Android OS wake-up integration beyond storage, such as exact alarms or first-class WorkManager orchestration.
