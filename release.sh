#!/bin/bash
set -e

# Ensure we are on main branch
BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ "$BRANCH" != "main" ] && [ "$BRANCH" != "master" ]; then
    echo "Error: Releases must be created from the main branch (currently on $BRANCH)"
    exit 1
fi

# Ensure working tree is clean
if [ -n "$(git status --porcelain)" ]; then
    echo "Error: Working tree has uncommitted changes or untracked files. Commit or stash them first."
    git status -s
    exit 1
fi

# Extract the default release version from build.gradle.kts
VERSION=$(sed -n 's/.*releaseVersion = providers.gradleProperty("releaseVersion").orElse("\([^"]*\)").*/\1/p' build.gradle.kts | head -n 1)

if [ -z "$VERSION" ]; then
    echo "Error: Could not find version in build.gradle.kts"
    exit 1
fi

TAG="v$VERSION"

echo "Releasing version $VERSION (Tag: $TAG) from branch $BRANCH"

# Check if tag already exists
if git rev-parse "$TAG" >/dev/null 2>&1; then
    echo "Error: Tag $TAG already exists"
    exit 1
fi

# Ensure all tests and coverage verification pass before tagging
echo "Running full test suite and coverage verification..."
./gradlew clean test jacocoTestReport jacocoRootReport jacocoTestCoverageVerification jacocoRootCoverageVerification

# Create and push the commit and tag
echo "Creating tag $TAG..."
git tag -a "$TAG" -m "Release $TAG"

echo "Pushing branch $BRANCH and tag $TAG to origin..."
git push origin "$BRANCH"
git push origin "$TAG"

echo "Release $TAG pushed successfully. The GitHub Action will publish to Maven Central and create the GitHub release."
