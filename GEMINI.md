# Khrona Project Instructions

## Release Workflow
When preparing a new release, follow these steps:

1. **Version Bump:** Update the `version` in the `allprojects` block of the root `build.gradle.kts` file.
2. **Changelog:** Update `CHANGELOG.md` with a new version section following the [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format. Document all `Added`, `Changed`, `Fixed`, and `Removed` items.
3. **Roadmap:** Update `.specs/project/ROADMAP.md` and the roadmap section in `README.md` to mark completed versions.
4. **Commit:** Commit all documentation and version changes with a message like `release: vX.Y.Z`.
5. **Tag:** Create an annotated git tag: `git tag -a vX.Y.Z -m "Release vX.Y.Z - Brief summary"`.
6. **Push:** Push both the branch and the tags: `git push origin main --tags`.

## Performance Standards
- Polling overhead should remain below 1% CPU for a moderate number of jobs (e.g., 10-100) across all storage engines.
- Memory footprint should stay under 50MB for the scheduler loop.
- Use the `@Disabled` performance tests in `khrona-core`, `khrona-store-jdbc`, and `khrona-store-redis` to verify these standards before major releases.
