# ROADMAP — Khrona

## Next Tasks
- [ ] Fix the Ktor quick-start/API mismatch so users can copy the documented setup into a real Ktor module.
- [ ] Isolate JDBC store calls onto an IO dispatcher, with an injectable dispatcher if the API shape stays clean.
- [x] Implement graceful scheduler shutdown: cancel active handlers, wait up to a configured timeout, then release or mark owned executions.
- [ ] Expose configurable execution lease duration and heartbeat settings with validation.
- [ ] Bound JDBC polling/claiming so large backlogs are processed in batches.
- [ ] Validate retry policy configuration and fix fractional backoff factor handling.
- [ ] Replace silent unsupported payload `toString()` fallback with fail-fast or explicit serializer behavior.

## v0.1: Core & In-Memory
- [x] Project Setup & Module Structure
- [x] Core Domain Models (JobDefinition, JobExecution, etc.)
- [x] Kotlin DSL for Job Definition
- [x] In-Memory Job Store (`MemoryJobStore`)
- [x] Basic Scheduler Runtime (Coroutine-native)
- [x] Interval & Cron Triggers (In-Memory)
- [x] Startup Trigger Support (Basic implementation via initial scheduling)
- [x] Ktor Plugin Integration (Lifecycle)

## v0.2: Persistence & Reliability
- [x] JDBC / Postgres Persistence
- [x] One-time Jobs (Persistent)
- [x] Transactional Enqueue
- [x] Retry Policies & Backoff
- [x] Dead-Letter Support

## v0.3: Distributed & Coordination
- [x] Multi-node Claiming (Lease Model)
- [x] Distributed Locking (`FORBID` policy)
- [x] Stale Worker Recovery

## v0.3.1: Reliability Hardening
- [x] Fix JDBC Handler Registry (JobDefinition.handler loss)
- [x] Resilient Recurring Schedules (Next run after terminal failure)
- [x] Execution Timeouts Enforcement
- [x] Observable API (Suspendable register/trigger)
- [x] Structured JSON Payloads
- [x] JDBC Schema Optimization (Indexing)
- [x] Fail-fast JDBC Migration
- [x] Safe REPLACE Ordering (claim before supersede)

## v0.3.2: Production Readiness Hardening
- [x] Fix Ktor quick-start/API mismatch so documented setup compiles as written
- [ ] Isolate blocking JDBC operations from coroutine scheduler/application dispatchers
- [ ] Add graceful scheduler shutdown with bounded wait for active executions
- [ ] Mark or release owned `CLAIMED`/`RUNNING` executions during shutdown instead of relying only on lease expiry
- [ ] Make execution lease duration configurable
- [ ] Make heartbeat interval configurable or derive it from validated lease settings
- [ ] Add bounded polling/claiming so large backlogs do not load every eligible execution each poll
- [ ] Validate retry policy inputs (`maxAttempts`, delays, factor, jitter)
- [ ] Fix fractional retry backoff factor handling
- [ ] Fail fast or require explicit serialization for unsupported JDBC payload types instead of silently using `toString()`

## v0.4: Ops & Visibility
- [ ] Admin API & Routes
- [ ] Metrics & Tracing (Micrometer/OpenTelemetry)
- [x] Misfire Policies
- [ ] Lock Inspection

## v0.5: Hardening
- [ ] Testkit (Virtual Time)
- [ ] Documentation & Samples
- [ ] Performance Tuning
- [ ] Cross-Database Adaptive Delay Scheduler
- [ ] Adaptive scheduler configuration docs

## Future Considerations
- **Cross-Database Adaptive Delay Scheduler:** Replace fixed idle polling with a hybrid adaptive loop that sleeps until the next known execution, stale recovery deadline, or a configurable max polling interval. The baseline must work across Memory, H2, PostgreSQL, MySQL, and Oracle without database-specific notifications. Native database wake-up mechanisms may be added later as optional optimizations.
- **Dynamic Worker Sizing:** Automatically adjust the number of concurrent execution coroutines based on load.
