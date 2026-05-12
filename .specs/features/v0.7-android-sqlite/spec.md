# Spec: v0.7 - Android SQLite Store

## Overview

Khrona currently supports in-memory storage and JDBC-backed storage for JVM/server deployments. Android applications need a local durable store that does not require JDBC, a server database, or network access.

This feature will add an Android-focused SQLite implementation of the `JobStore` SPI so Khrona can persist scheduled work inside an Android app process while keeping the core scheduler contract portable.

## Objectives

- Provide durable local scheduling storage for Android apps.
- Avoid JDBC on Android.
- Keep Android-specific dependencies out of `khrona-core`.
- Preserve Khrona's at-least-once execution model.
- Reuse the existing store contract behavior wherever possible.
- Make Android lifecycle and background execution boundaries explicit.

## Requirements

- **REQ-AS1: Android Store Module:** Add a separate `khrona-store-android-sqlite` module for Android-specific SQLite storage.
- **REQ-AS2: Core Isolation:** `khrona-core` must not depend on Android SDK, AndroidX, Room, or SQLite implementation classes.
- **REQ-AS3: JobStore Compatibility:** The Android SQLite store must implement the same `JobStore` behavior required by the scheduler, including scheduling, claiming, status updates, retries, dead-lettering, locking, lease recovery, payload persistence, and recurring execution persistence.
- **REQ-AS4: SQLite Schema:** The store must define an Android-compatible SQLite schema for job definitions, executions, locks, payload metadata, heartbeats, retry state, and dead-letter state.
- **REQ-AS5: Migration Path:** Schema creation and upgrades must be explicit and versioned so app upgrades do not silently discard scheduled work.
- **REQ-AS6: Transaction Safety:** Claiming, status transitions, lock acquisition, and enqueue operations must use SQLite transactions to preserve scheduler invariants.
- **REQ-AS7: Android Lifecycle Contract:** Documentation must state that the store persists work, but Android OS wake-up and process scheduling remain the host app's responsibility.
- **REQ-AS8: WorkManager Interop Guidance:** Provide documentation or sample guidance showing how an app can use Android WorkManager or another app-level mechanism to start Khrona when background execution is allowed.
- **REQ-AS9: Clock and Device State:** The design must account for process death, device sleep, reboot, and wall-clock changes without promising exact execution time.
- **REQ-AS10: Test Coverage:** Add store contract tests that exercise Android SQLite behavior, using the most practical Android test strategy available in the project setup.

## Non-Goals

- Replacing JDBC storage.
- Making Android the baseline runtime for Khrona.
- Implementing OS-level alarms, exact alarms, foreground services, or WorkManager scheduling inside `khrona-core`.
- Guaranteeing execution while the Android app process is not allowed to run.
- Adding a Room-based public API unless implementation research shows it is the best fit.
- Cross-device synchronization.

## Success Criteria

- Android apps can use Khrona with durable local SQLite storage.
- The Android store passes the shared `JobStore` behavior tests.
- Scheduler behavior remains unchanged for Memory and JDBC stores.
- The Android module can be excluded from non-Android consumers.
- Documentation clearly explains what Khrona handles and what Android background execution APIs must handle.
- App restarts do not lose pending, running, retry, or dead-letter execution state.
