# PR #11 follow-up results

Completed 2026-11-15 on `fix/p0-production-readiness-defects`.

## Changes

- Claim and heartbeat budgets start before storage I/O. A shared atomic deadline combines wall-clock bounds and monotonic elapsed time; delayed responses cannot extend the budget.
- Each claim carries its original store through status transitions, renewal, retries, follow-ups, and shutdown release. New polls and registrations can use a newly configured store without redirecting existing claims.
- Legacy JDBC queries expand their eligible prefix only as needed to find locally registered jobs. Returned matching batches stay capped by the requested limit; query-side filtering remains the preferred path for large shared backlogs.

## TDD evidence

Eight regression tests failed before the implementation, covering delayed renewal/claim responses, stalled wall time, original-store completion/renewal/retry/release, and foreign-row starvation. Two additional mutation-guided tests assert transient heartbeat error tolerance and cancellation exactly at expiry. Existing assertions and quality thresholds were preserved.

## Verification

- `./gradlew clean test jacocoTestReport jacocoRootReport jacocoTestCoverageVerification jacocoRootCoverageVerification`: passed with 401 tests before the final two boundary tests.
- Full tests and coverage rerun with `--rerun-tasks` after adding those tests: 403 tests, zero failures/errors/skips.
- Core, JDBC, Redis performance suites: 13 tests, zero failures/errors/skips.
- Dockerized Ktor generator E2E: 3 tests, zero failures/errors/skips.
- Mutation suites and checker: aggregate 91.61%; core 90.48%, Ktor 100.00%, JDBC 92.00%, Memory 94.12%.
- Coverage checker: root line 97.50%, instruction 95.05%, branch 93.17%; all module thresholds pass.
- `git diff --check`: passed.

## Limits

Changing stores does not migrate persisted definitions or pending work. Keep the original store open for its in-flight claims. Handler cancellation is cooperative and external side effects still require idempotency. This task closes the three scoped review defects; broader management and observability work remains on the project roadmap.
