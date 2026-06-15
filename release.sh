#!/bin/bash
set -e

# Extract version from build.gradle.kts
VERSION=$(grep "version =" build.gradle.kts | head -n 1 | awk -F'"' '{print $2}')

if [ -z "$VERSION" ]; then
    echo "Error: Could not find version in build.gradle.kts"
    exit 1
fi

TAG="v$VERSION"

echo "Releasing version $VERSION (Tag: $TAG)"

# Check if tag already exists
if git rev-parse "$TAG" >/dev/null 2>&1; then
    echo "Error: Tag $TAG already exists"
    exit 1
fi

# Ensure all tests pass before tagging
echo "Running tests..."
./gradlew clean test

# Create and push the tag
echo "Creating tag $TAG..."
git tag -a "$TAG" -m "Release $TAG"

echo "Pushing tag $TAG..."
git push origin "$TAG"

echo "Tag $TAG pushed successfully. The GitHub Action will publish to Maven Central and create the GitHub release."
