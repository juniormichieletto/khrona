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

Happy coding!
