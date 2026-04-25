# Agent Instructions

## Engineering Standards

- **Test-Driven Development (TDD):** You MUST follow TDD for most implementation tasks.
    - Write a failing test case that reproduces the issue or defines the new feature.
    - Run the test to confirm it fails.
    - Implement the minimal code changes required to make the test pass.
    - Refactor as necessary while keeping the test suite green.
- **Verification:** Always execute the full test suite after making changes to ensure the project remains functional and no regressions were introduced.

```bash
./gradlew clean test
```
