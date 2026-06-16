# Contributing to Khrona

First off, thank you for considering contributing to **Khrona**! It's people like you that make the open-source community such an amazing place.

## Code of Conduct

By participating in this project, you are expected to uphold our standards for a professional and welcoming community.

## How Can I Contribute?

### Reporting Bugs
- Use the **GitHub Issues** tab.
- Check if the bug has already been reported.
- Include a clear title and description, as many relevant details as possible, and a code snippet or a test case demonstrating the issue.

### Suggesting Enhancements
- Open a **GitHub Issue** with the "enhancement" label.
- Provide a clear and detailed explanation of the proposed feature and why it would be useful.

### Pull Requests
1. **Fork** the repository and create your branch from `master`.
2. **Write tests** for your changes. We follow a strict **TDD** (Test-Driven Development) approach.
3. Ensure the full test suite passes: `./gradlew test`.
4. Follow the existing **Kotlin coding style** and project conventions.
5. Use descriptive commit messages.
6. Submit your pull request and wait for review.

## Project Structure

- `khrona-core`: The core scheduling engine and DSL.
- `khrona-ktor`: Ktor plugin and integration.
- `khrona-store-jdbc`: JDBC-based job storage for persistence.
- `khrona-store-memory`: In-memory job storage (for testing).

## Development Setup

Khrona uses **Gradle**. To run the full test suite locally, you will need **Docker** installed (for Testcontainers-based integration tests).

```bash
./gradlew test
```

## Release Workflow

When preparing a new release, follow these steps:

1. **Version Bump:** Update the `version` in the `allprojects` block of the root `build.gradle.kts` file.
2. **Changelog:** Update `CHANGELOG.md` with a new version section following the [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format. Document all `Added`, `Changed`, `Fixed`, and `Removed` items.
3. **Roadmap:** Update `.specs/project/ROADMAP.md` and the roadmap section in `README.md` to mark completed versions.
4. **Commit:** Commit all documentation and version changes with a message like `release: vX.Y.Z`.
5. **Tag:** Create an annotated git tag: `git tag -a vX.Y.Z -m "Release vX.Y.Z - Brief summary"`.
6. **Push:** Push both the branch and the tags: `git push origin main --tags`. The tag workflow publishes the modules to Maven Central and uses JReleaser to create a GitHub release without attaching JAR files.

### Backfilling GitHub Releases

Historical tags can be backfilled with JReleaser. See `docs/release-process.md` for the rationale, safety checks, and verification steps.

## Performance Standards

- **CPU Efficiency:** Polling overhead should remain below 1% CPU for a moderate number of jobs (e.g., 10-100) across all storage engines.
- **Memory Footprint:** Memory usage should stay under 50MB for the scheduler loop.
- **Verification:** Use the `@Disabled` performance tests in `khrona-core`, `khrona-store-jdbc`, and `khrona-store-redis` to verify these standards before major releases.

Happy coding!
