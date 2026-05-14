# ARCHITECTURE — Khrona

## Component Diagram

```mermaid
graph TD
    subgraph "Client Layer"
        DSL[Kotlin DSL] --> Ktor[Ktor Plugin]
    end

    subgraph "Core Engine"
        Sch[Scheduler] --> Triggers[Interval / Cron Triggers]
        Sch --> Workers[Execution Coroutines]
        Workers --> Heartbeat[Heartbeat Manager]
    end

    subgraph "Storage Layer (SPI)"
        Store[JobStore Interface]
        Memory[MemoryJobStore] --- Store
        JDBC[JdbcJobStore] --- Store
        Redis[RedisJobStore] --- Store
        
        subgraph "JDBC Dialects"
            JDBC --- Postgres[PostgreSQL]
            JDBC --- MySql[MySQL 8]
            JDBC --- Oracle[Oracle]
            JDBC --- H2[H2]
        end
    end

    DSL --> Sch
    Sch <--> Store
    Workers <--> Store
```

## Component Overview
- **Core:** Job definitions, triggers, execution engine.
- **Store SPI:** Pluggable storage layer (`MemoryJobStore`, `JdbcJobStore`, `RedisJobStore`).
- **JdbcDialect:** Abstraction for database-specific SQL (PostgreSQL, MySQL, H2, etc.).
- **RedisJobStore:** Redis-backed scheduler state using namespaced hashes, sorted-set lease indexes, lock indexes, and Lua scripts for atomic claim/supersede transitions.
- **Worker:** Coroutine-based executor polling and heartbeating.
- **Ktor Plugin:** Integration bridge for Ktor lifecycle and routing.

## Execution Flow

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant ST as JobStore
    participant W as Worker Coroutine
    
    loop Every pollingInterval
        S->>ST: listEligibleExecutions(now)
        ST-->>S: List<JobExecution>
        loop For each execution
            S->>ST: isLockHeld(lockKey)
            ST-->>S: Boolean
            alt Lock Free
                S->>ST: claimExecution(execId, workerId)
                ST-->>S: Success
                S->>W: launch execution
                activate W
                W->>ST: updateStatus(RUNNING)
                loop Heartbeat
                    W->>ST: heartbeat(execId)
                end
                W->>W: execute handler()
                W->>ST: updateStatus(SUCCESS)
                W->>ST: saveExecution(next)
                deactivate W
            end
        end
    end
```

1. **Definition:** User defines jobs via DSL.
2. **Scheduling:** Triggers calculate next run times.
3. **Queueing:** Eligible jobs are moved to the queue (store-specific).
4. **Claiming:** Workers claim jobs from the store.
5. **Execution:** Jobs run in a managed CoroutineScope.
6. **Completion:** Results/Errors are persisted; retries scheduled if needed.
