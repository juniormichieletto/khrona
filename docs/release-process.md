# Release Process Notes

This document records the JReleaser release setup and the one-time historical GitHub release backfill performed for Khrona.

## What changed

Khrona now uses JReleaser to create GitHub releases for existing Git tags.

The release setup is intentionally split:

- Gradle still publishes signed module artifacts to Maven Central through the existing `publishAggregationToCentralPortal` task.
- JReleaser creates the GitHub release and release notes.
- JReleaser does not upload JARs or other release assets to GitHub releases.

The relevant files are:

- `jreleaser.yml`: normal GitHub release configuration.
- `.github/workflows/publish.yml`: tag workflow that publishes Maven artifacts, then creates the GitHub release.
- `scripts/backfill-releases.sh`: one-time helper for historical GitHub release backfills.
- `build.gradle.kts`: JReleaser plugin configuration and release-version/config-file overrides used by the backfill helper.

## Why releases do not include JAR assets

Khrona is a multi-module library published through Maven Central. GitHub release JAR uploads would duplicate Maven Central artifacts and create another distribution surface to keep in sync.

The JReleaser configuration disables release assets explicitly:

```yaml
uploadAssets: NEVER
files: false
artifacts: false
checksums: false
signatures: false
catalogs: false
```

The GitHub release is therefore used for release notes and tag visibility only. Consumers should resolve artifacts from Maven Central.

## Normal release flow

For a new release:

1. Update the project version in `build.gradle.kts`.
2. Update all matching README dependency examples when the version changes.
3. Update `CHANGELOG.md`.
4. Run the full test suite:

   ```bash
   ./gradlew clean test
   ```

5. Commit the release changes.
6. Create and push the release tag.

   ```bash
   git tag -a vX.Y.Z -m "Release vX.Y.Z"
   git push origin main --tags
   ```

The GitHub Actions tag workflow then:

1. Publishes signed Maven artifacts with `./gradlew publishAggregationToCentralPortal`.
2. Runs `./gradlew jreleaserRelease`.
3. Creates a GitHub release with generated notes and no attached assets.

The workflow uses `GITHUB_TOKEN` as `JRELEASER_GITHUB_TOKEN`.

## Historical backfill

The repository had tags created before JReleaser-managed GitHub releases were configured. These were backfilled with:

```bash
JRELEASER_GITHUB_TOKEN=... ./scripts/backfill-releases.sh --execute
```

The helper exists because JReleaser creates one release per run. It loops through the historical tag list and creates one GitHub release for each tag.

The backfilled tags were:

```text
v0.1.0
v0.1.1
v0.3.0
0.3.1
v0.3.2
v0.3.3
v0.4.0
```

`0.3.1` intentionally has no `v` prefix because that is the tag that exists in the repository.

## Backfill safety model

The helper defaults to dry-run mode:

```bash
./scripts/backfill-releases.sh
```

Dry-run mode:

- creates temporary JReleaser config files under `build/jreleaser/backfill/`;
- creates per-tag release-note files under `build/jreleaser/backfill/`;
- runs JReleaser with `--dryrun`;
- does not create or update GitHub releases.

Execution mode:

```bash
JRELEASER_GITHUB_TOKEN=... ./scripts/backfill-releases.sh --execute
```

Execution mode refuses to run unless:

- `JRELEASER_GITHUB_TOKEN` is set;
- all listed tags exist locally;
- the working tree is clean.

The clean-tree guard prevents accidental release creation from uncommitted script/config edits.

## Why the backfill uses external changelog files

For the current release flow, JReleaser can generate release notes from Git history using the conventional-commits preset.

For historical backfill, the helper writes exact per-tag changelog files and passes them to JReleaser as external changelogs. This avoids ambiguity around the first release and around the nonstandard `0.3.1` tag name.

Each backfilled release uses the intended commit range:

- first tag: full history up to the tag;
- later tags: previous tag through current tag.

## Verification commands

Check the evaluated current JReleaser config:

```bash
./gradlew jreleaserConfig -Djreleaser.github.token=dummy
```

Dry-run all historical release backfills:

```bash
./scripts/backfill-releases.sh --dry-run
```

Verify GitHub releases and asset counts:

```bash
curl -sS \
  -H "Authorization: Bearer $JRELEASER_GITHUB_TOKEN" \
  -H "Accept: application/vnd.github+json" \
  'https://api.github.com/repos/juniormichieletto/khrona/releases?per_page=100' |
  jq -r '.[] | [.tag_name, .name, (.assets | length)] | @tsv'
```

Expected backfill result:

```text
v0.4.0   Release v0.4.0   0
v0.3.3   Release v0.3.3   0
v0.3.2   Release v0.3.2   0
0.3.1    Release 0.3.1    0
v0.3.0   Release v0.3.0   0
v0.1.1   Release v0.1.1   0
v0.1.0   Release v0.1.0   0
```

The third column is the number of attached GitHub release assets. It should remain `0`.

## Temporary files

JReleaser and Gradle generate local files under:

- `build/jreleaser/`
- `build/gradle-user-home/`
- module `build/` directories
- `.gradle/`

These paths are ignored and can be deleted when not needed for local inspection.
