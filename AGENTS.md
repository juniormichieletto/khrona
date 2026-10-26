# Agent Instructions

## Engineering Standards

- **Test-Driven Development (TDD):** You MUST follow TDD for most implementation tasks.
    - Write a failing test case that reproduces the issue or defines the new feature.
    - Run the test to confirm it fails.
    - Implement the minimal code changes required to make the test pass.
    - Refactor as necessary while keeping the test suite green.
- **Verification:** Always execute the full test suite after making changes to ensure the project remains functional and no regressions were introduced.
- **Version Bumping:** Whenever you bump the project version (e.g., in `build.gradle.kts`), you MUST also update all version references in `README.md` to ensure documentation remains synchronized with the release.
- **Source Control:** NEVER commit changes automatically. Always wait for explicit user confirmation or a "commit" directive after you have presented your changes for review.

```bash
./gradlew clean test
```

- **End-to-End (E2E) Dockerized Ktor Generator Suite:** Tests tagged with `@Tag("e2e")` are excluded from default `./gradlew test` runs because they scaffold a fresh Ktor project inside Docker containers (`ktor new`), publish local artifacts to `mavenLocal()`, and boot real Ktor servers against Memory, PostgreSQL (`JdbcJobStore`), and Redis (`RedisJobStore`). Run them explicitly when validating end-to-end Ktor integration or before releases:

```bash
./gradlew :khrona-ktor:e2eTest
```

- **Performance Benchmark Suite:** Tests tagged with `@Tag("perf")` in `khrona-core`, `khrona-store-jdbc`, and `khrona-store-redis` are excluded from default `./gradlew test` runs. Run them explicitly when modifying the scheduler polling loop or storage implementations, or before major releases:

```bash
# Run all performance benchmark suites
./gradlew perfTest

# Or run a specific module's performance suite
./gradlew :khrona-core:perfTest
./gradlew :khrona-store-jdbc:perfTest
./gradlew :khrona-store-redis:perfTest
```
