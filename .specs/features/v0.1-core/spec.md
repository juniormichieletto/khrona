# Spec: v0.1 - Core & In-Memory

## 1. Overview
Initialize the Khrona project with its core domain models, an in-memory storage implementation, a basic coroutine-native scheduler, and Ktor plugin integration.

## 2. Requirements

### 2.1 Project Infrastructure
- **REQ-1.1:** Setup multi-module Gradle project (Kotlin DSL).
- **REQ-1.2:** Define core modules: `khrona-core`, `khrona-ktor`, `khrona-store-memory`.

### 2.2 Core Domain Models
- **REQ-2.1:** `JobDefinition`: ID, description, handler, trigger spec, policies, lock key.
- **REQ-2.2:** `JobExecution`: Execution ID (UUID), Job ID, timestamps (scheduled, started, completed), status, attempt, error, payload.
- **REQ-2.3:** `ExecutionStatus`: PENDING, CLAIMED, RUNNING, SUCCESS, FAILED, CANCELLED, DEAD_LETTERED.

### 2.3 Kotlin DSL
- **REQ-3.1:** `job("id") { ... }` builder for `JobDefinition`.
- **REQ-3.2:** Explicit trigger functions: `every(...)`, `cron(...)`, `onStartup(...)`, `onceEphemeral(...)`.

### 2.4 In-Memory Store
- **REQ-4.1:** `MemoryJobStore` implementation of `JobStore` interface.
- **REQ-4.2:** Support for basic CRUD on jobs and executions in-memory.

### 2.5 Scheduler Runtime
- **REQ-5.1:** Coroutine-native execution loop.
- **REQ-5.2:** Basic polling mechanism for eligible jobs.
- **REQ-5.3:** Support for Interval and Cron triggers.

### 2.6 Ktor Integration
- **REQ-6.1:** Ktor `ApplicationPlugin` for `Scheduler`.
- **REQ-6.2:** Lifecycle hooks: Start on `ApplicationStarted`, Stop gracefully on `ApplicationStopping`.

## 3. Constraints
- Must use Kotlin 2.x and Coroutines 1.8+.
- Ktor 3.x support.
- No external dependencies for `khrona-core` other than Coroutines and standard library.
