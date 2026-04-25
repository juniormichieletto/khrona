# ROADMAP — Khrona

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

## v0.4: Ops & Visibility
- [ ] Admin API & Routes
- [ ] Metrics & Tracing (Micrometer/OpenTelemetry)
- [ ] Misfire Policies
- [ ] Lock Inspection

## v0.5: Hardening
- [ ] Testkit (Virtual Time)
- [ ] Documentation & Samples
- [ ] Performance Tuning

## Future Considerations
- **Adaptive Delay Scheduler:** Investigate moving from fixed polling to an adaptive sleep model (sleeping until the next known job execution). This would require a distributed signaling mechanism (e.g., Postgres `NOTIFY`) to wake up workers when new work is scheduled by other nodes.
- **Dynamic Worker Sizing:** Automatically adjust the number of concurrent execution coroutines based on load.
