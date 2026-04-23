# ROADMAP — Khrona

## v0.1: Core & In-Memory
- [ ] Project Setup & Module Structure
- [ ] Core Domain Models (JobDefinition, JobExecution, etc.)
- [ ] Kotlin DSL for Job Definition
- [ ] In-Memory Job Store (`MemoryJobStore`)
- [ ] Basic Scheduler Runtime (Coroutine-native)
- [ ] Interval & Cron Triggers (In-Memory)
- [ ] Startup Trigger Support
- [ ] Ktor Plugin Integration (Lifecycle)

## v0.2: Persistence & Reliability
- [ ] JDBC / Postgres Persistence
- [ ] One-time Jobs (Persistent)
- [ ] Transactional Enqueue
- [ ] Retry Policies & Backoff
- [ ] Dead-Letter Support

## v0.3: Distributed & Coordination
- [ ] Multi-node Claiming (Lease Model)
- [ ] Distributed Locking (`FORBID` policy)
- [ ] Stale Worker Recovery

## v0.4: Ops & Visibility
- [ ] Admin API & Routes
- [ ] Metrics & Tracing (Micrometer/OpenTelemetry)
- [ ] Misfire Policies
- [ ] Lock Inspection

## v0.5: Hardening
- [ ] Testkit (Virtual Time)
- [ ] Documentation & Samples
- [ ] Performance Tuning
