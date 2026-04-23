# CONVENTIONS — Khrona

- **Style:** Standard Kotlin coding conventions.
- **Naming:** 
    - Jobs: kebab-case IDs.
    - Classes: PascalCase.
    - Methods/Properties: camelCase.
- **Error Handling:** Use custom exception hierarchy; prefer returning results for domain logic where possible.
- **Concurrency:** Prefer structured concurrency; all public execution APIs must be `suspend`.
- **Testing:** Unit tests for logic; Integration tests with Testcontainers (for JDBC).
