# PR #11 follow-up correctness fixes

Status: Done

## Scope and invariants

1. Lease safety: claim and renewal response latency must not extend a worker's local ownership deadline. Local elapsed time must keep advancing across wall-clock stalls; heartbeat and watchdog share deadlines safely.
2. Store ownership: a claimed execution uses the store that granted its claim for heartbeat, completion, retries, and shutdown release, even when configuration selects a new store for future work.
3. Legacy JDBC filtering: unrelated older rows cannot exhaust the result limit before locally registered jobs are considered. Fallback queries expand only as needed and return at most the requested number of matching executions.

## Verification

- Reproduce each defect with failing deterministic regressions before implementation.
- Run focused regressions, the full clean test suite and coverage gates.
- Run scheduler/JDBC/Redis performance suites, Ktor E2E, and mutation gates.
- Inspect staged scope; commit and push only the reviewed fixes and tests.

No version bump or public SPI change is required.
