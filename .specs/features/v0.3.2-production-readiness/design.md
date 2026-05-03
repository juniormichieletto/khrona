# Design: v0.3.2 - Production Readiness Hardening

## 1. JDBC Dispatcher Isolation
**Problem:** `JdbcJobStore` exposes suspend functions but performs blocking JDBC work directly in the caller coroutine.

**Solution:**
- Add a dispatcher parameter to `JdbcJobStore`, defaulting to `Dispatchers.IO`.
- Wrap every JDBC operation, including `migrate()` if it remains synchronous or a new suspend migration variant is added, in a shared helper such as `withJdbcContext { ... }`.
- Keep JDBC transaction boundaries inside the dispatcher block so connection usage does not escape across coroutine contexts.
- Tests should inject a limited or test dispatcher to prove store operations leave the caller dispatcher.

## 2. Lease And Heartbeat Configuration
**Problem:** Scheduler claims executions with a hard-coded five minute lease and heartbeats at `leaseDuration / 2`.

**Solution:**
- Add `executionLeaseDuration` and `heartbeatInterval` to `KhronaConfig`.
- Default to the current behavior: five minute lease and half-lease heartbeat.
- Provide Kotlin duration DSL helpers for both settings.
- Validate that lease duration and heartbeat interval are positive and that heartbeat interval is strictly less than lease duration.
- Prefer explicit heartbeat interval in config. If the project chooses derived heartbeat behavior, derive it once during validation and keep the runtime path simple.

## 3. Bounded Polling And Claiming
**Problem:** `listEligibleExecutions(now)` returns every eligible execution, which can load large backlogs and launch too much claim work in a single poll.

**Solution:**
- Add a configurable `pollBatchSize` to `KhronaConfig`.
- Change the store contract to support bounded eligible execution listing, either by adding a `limit` parameter or introducing a new method while preserving source compatibility where practical.
- Implement dialect-specific limiting:
  - PostgreSQL and MySQL: `LIMIT ?`
  - H2: `LIMIT ?`
  - Oracle: `FETCH FIRST ? ROWS ONLY` or an equivalent supported query shape
- Keep ordering by `scheduled_at ASC` for fairness.
- Add tests that create more eligible rows than the batch size and prove a single poll only processes the bounded amount.

## 4. Retry Policy Validation And Fractional Backoff
**Problem:** Invalid retry settings can be accepted, and `calculateDelay` truncates fractional backoff multipliers before applying them.

**Solution:**
- Add validation to `RetryPolicy` or to the job validation path so invalid policy values are rejected before scheduling.
- Use millisecond or nanosecond arithmetic with a `Double` multiplier, then cap at `maxDelay`.
- Keep jitter application after capping and ensure final delay is never negative.
- Add deterministic tests for fractional factors with jitter disabled.

## 5. Payload Serialization Fail-Fast
**Problem:** Unsupported payloads and nested values currently fall back to `toString()`, which can corrupt data silently.

**Solution:**
- Restrict automatic JDBC payload support to JSON-compatible types: `String`, finite `Number`, `Boolean`, `Map<String, *>`, `Iterable<*>`, and `null`.
- Reject unsupported top-level and nested values with an `IllegalArgumentException` that names the unsupported type and path.
- Reject map keys that are not strings instead of silently converting keys.
- Keep existing successful round-trip behavior for supported structured payloads.
- Document that custom domain payloads should be converted to supported JSON-compatible structures before triggering jobs, unless a future explicit serializer extension is added.

## 6. Project State Cleanup
**Problem:** The project docs currently disagree about the Ktor quick-start and shutdown fixes.

**Solution:**
- Update `ROADMAP.md` and `STATE.md` when implementation tasks complete.
- Keep the immediate `Next Tasks` section as the release readiness source of truth.
- Remove resolved blockers from `STATE.md` once code and tests prove they are complete.
