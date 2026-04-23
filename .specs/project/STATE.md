# STATE — Khrona

## Decisions
- **Structure:** Transitioned from `.spec/SPEC.md` to `.specs/` structure for TLC spec-driven workflow.
- **Guarantee:** Explicitly settled on **at-least-once** execution; removed "exactly-once" to avoid distributed systems fallacies.
- **Storage:** `MemoryJobStore` is a first-class citizen for single-instance/dev/test use cases.
- **v0.1 Completion:** Implemented core engine, in-memory store, and Ktor integration.

## Blockers
- None.

## Todos
- [ ] Map existing codebase (if any) to `.specs/codebase/`.
- [ ] Initialize the Gradle/Kotlin project structure.

## Deferred Ideas
- Payload versioning/evolution (tracked in Future Considerations).
- Advanced RBAC for Admin API.
- Dynamic Lock Keys.
