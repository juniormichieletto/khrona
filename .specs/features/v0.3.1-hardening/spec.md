# Spec: v0.3.1 - Reliability Hardening

## Overview
Based on the initial project review, several critical reliability issues have been identified in the current implementation of Khrona. This phase focuses on hardening the core scheduler and JDBC storage to ensure production readiness.

## Objectives
- Ensure job handlers are correctly associated with jobs when using persistent storage.
- Guarantee that recurring jobs continue to run even after individual execution failures or dead-lettering.
- Implement missing API features like `timeout` and proper concurrency policies.
- Improve API observability by making key methods suspendable.
- Define a robust payload serialization strategy.
- Optimize database schema for production workloads.

## Requirements
- **REQ-H1: Runtime Handler Registry:** The scheduler must maintain a registry of job handlers in-memory, as they cannot be serialized.
- **REQ-H2: Resilient Recurring Schedules:** Terminal failure (DEAD_LETTERED) of one occurrence must not prevent the scheduling of the next occurrence.
- **REQ-H3: Execution Timeouts:** Job handlers must be executed with a timeout enforced by the scheduler.
- **REQ-H4: Concurrency Policy Support:** The `REPLACE` policy must be implemented or explicitly handled.
- **REQ-H5: Observable API:** `registerJob` and `trigger` must allow callers to await completion and handle errors.
- **REQ-H6: Structured Payloads:** Payloads must be stored as JSON to preserve structure and types.
- **REQ-H7: Database Performance:** The JDBC schema must include necessary indexes for efficient polling and recovery.
