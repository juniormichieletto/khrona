#!/usr/bin/env bash
set -euo pipefail

export GRADLE_USER_HOME="${GRADLE_USER_HOME:-${PWD}/build/gradle-user-home}"

mode="dry-run"
if [[ "${1:-}" == "--execute" ]]; then
  mode="execute"
elif [[ "${1:-}" == "--dry-run" || -z "${1:-}" ]]; then
  mode="dry-run"
else
  echo "Usage: $0 [--dry-run|--execute]" >&2
  exit 1
fi

tags=(
  "v0.1.0"
  "v0.1.1"
  "v0.3.0"
  "0.3.1"
  "v0.3.2"
  "v0.3.3"
  "v0.4.0"
)

require_tag() {
  local tag="$1"
  if ! git rev-parse --verify --quiet "refs/tags/${tag}" >/dev/null; then
    echo "Missing required tag: ${tag}" >&2
    exit 1
  fi
}

version_from_tag() {
  local tag="$1"
  echo "${tag#v}"
}

write_config() {
  local tag="$1"
  local previous_tag="$2"
  local config_file="$3"
  local changelog_file="$4"

  cat > "${config_file}" <<EOF_CONFIG
project:
  name: khrona
  description: Coroutine-native job scheduling for Kotlin and Ktor
  authors:
    - AJ
  license: Apache-2.0
  links:
    homepage: https://github.com/juniormichieletto/khrona
  inceptionYear: "2026"

release:
  github:
    owner: juniormichieletto
    name: khrona
    tagName: ${tag}
    releaseName: Release ${tag}
EOF_CONFIG

  if [[ -n "${previous_tag}" ]]; then
    cat >> "${config_file}" <<EOF_CONFIG
    previousTagName: ${previous_tag}
EOF_CONFIG
  fi

  cat >> "${config_file}" <<EOF_CONFIG
    skipTag: true
    uploadAssets: NEVER
    files: false
    artifacts: false
    checksums: false
    signatures: false
    catalogs: false
    changelog:
      enabled: true
      external: ${changelog_file}
EOF_CONFIG
}

write_changelog() {
  local tag="$1"
  local previous_tag="$2"
  local changelog_file="$3"
  local range=("${tag}")

  if [[ -n "${previous_tag}" ]]; then
    range=("${previous_tag}..${tag}")
  fi

  {
    echo "## Changelog"
    echo
    echo "## Changes"

    if ! git log --reverse --format='- %h %s' "${range[@]}"; then
      echo "Failed to generate changelog for ${tag}" >&2
      exit 1
    fi

    echo
    echo "## Contributors"
    git log --format='%an' "${range[@]}" | sort -u | sed 's/^/- /'
  } > "${changelog_file}"
}

if [[ "${mode}" == "execute" ]]; then
  if [[ -z "${JRELEASER_GITHUB_TOKEN:-}" ]]; then
    echo "JRELEASER_GITHUB_TOKEN is required for --execute." >&2
    exit 1
  fi

  if [[ -n "$(git status --porcelain)" ]]; then
    echo "Refusing --execute with a dirty working tree." >&2
    echo "Commit, stash, or discard local changes first." >&2
    exit 1
  fi
fi

for tag in "${tags[@]}"; do
  require_tag "${tag}"
done

mkdir -p build/jreleaser/backfill

previous_tag=""
for tag in "${tags[@]}"; do
  version="$(version_from_tag "${tag}")"
  config_file="build/jreleaser/backfill/${tag}.yml"
  changelog_file="build/jreleaser/backfill/${tag}-CHANGELOG.md"

  write_changelog "${tag}" "${previous_tag}" "${changelog_file}"
  write_config "${tag}" "${previous_tag}" "${config_file}" "${changelog_file}"

  echo
  echo "Preparing GitHub release for ${tag} (project version ${version})"
  if [[ -n "${previous_tag}" ]]; then
    echo "Changelog range: ${previous_tag}..${tag}"
  else
    echo "Changelog range: initial history through ${tag}"
  fi

  args=(
    "./gradlew"
    "jreleaserRelease"
    "-PreleaseVersion=${version}"
    "-PjreleaserConfigFile=${config_file}"
  )

  if [[ "${mode}" == "dry-run" ]]; then
    args+=("--dryrun")
    if [[ -z "${JRELEASER_GITHUB_TOKEN:-}" ]]; then
      args+=("-Djreleaser.github.token=dummy")
    fi
  fi

  "${args[@]}"

  previous_tag="${tag}"
done

if [[ "${mode}" == "dry-run" ]]; then
  echo
  echo "Dry run complete. No GitHub releases were created."
  echo "Review build/jreleaser/backfill/*-CHANGELOG.md, then rerun with --execute to create releases."
fi
