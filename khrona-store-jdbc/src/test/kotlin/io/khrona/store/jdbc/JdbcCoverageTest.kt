package io.khrona.store.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.khrona.core.ExecutionStatus
import io.khrona.core.JobExecution
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.Statement
import java.sql.SQLException
import java.time.Duration
import java.time.Instant
import javax.sql.DataSource

class JdbcCoverageTest : AbstractJdbcJobStoreTest() {
    override fun createDataSource(): HikariDataSource {
        val config = HikariConfig().apply {
            jdbcUrl = "jdbc:h2:mem:khrona_coverage_${System.currentTimeMillis()};MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1"
            username = "sa"
            password = ""
            driverClassName = "org.h2.Driver"
        }
        return HikariDataSource(config)
    }

    @Test
    fun `H2 store covers payload conversion branches`() = runBlocking {
        val payload = mapOf(
            "string" to "value",
            "long" to 42L,
            "double" to 1.5,
            "boolean" to true,
            "list" to listOf("nested", 7L, false),
            "null" to null
        )
        val execution = JobExecution(jobId = "jdbc-payload", scheduledAt = Instant.now(), payload = payload)

        store.saveExecution(execution)

        assertEquals(payload, store.getExecution(execution.id)?.payload)
        assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                store.saveExecution(JobExecution(jobId = "jdbc-nan", scheduledAt = Instant.now(), payload = Double.NaN))
            }
        }
        assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                store.saveExecution(JobExecution(jobId = "jdbc-object", scheduledAt = Instant.now(), payload = Any()))
            }
        }
        assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                store.saveExecution(JobExecution(jobId = "jdbc-null-key", scheduledAt = Instant.now(), payload = mapOf(null to "value")))
            }
        }
        assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                store.saveExecution(JobExecution(jobId = "jdbc-int-key", scheduledAt = Instant.now(), payload = mapOf(123 to "value")))
            }
        }
        assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                store.saveExecution(JobExecution(jobId = "jdbc-inf", scheduledAt = Instant.now(), payload = Double.POSITIVE_INFINITY))
            }
        }

        // Test root payloads of all types
        listOf(
            true,
            false,
            "plain-string",
            12345L,
            99.5,
            listOf(true, false, 10L, 20.5, "item", null),
            mapOf("k" to true, "k2" to false, "k3" to 1L, "k4" to 2.5, "k5" to "s", "k6" to null)
        ).forEachIndexed { i, p ->
            val exec = JobExecution(jobId = "jdbc-root-$i", scheduledAt = Instant.now(), payload = p)
            store.saveExecution(exec)
            assertEquals(p, store.getExecution(exec.id)?.payload)
        }
    }

    @Test
    fun `H2 store covers every terminal status branch`() = runBlocking {
        ExecutionStatus.entries.forEach { status ->
            val execution = JobExecution(jobId = "jdbc-status-$status", scheduledAt = Instant.now())
            store.saveExecution(execution)

            store.updateExecutionStatus(execution.id, status, status.name)

            val updated = store.getExecution(execution.id)
            assertEquals(status, updated?.status)
            if (status in listOf(
                    ExecutionStatus.SUCCESS,
                    ExecutionStatus.FAILED,
                    ExecutionStatus.DEAD_LETTERED,
                    ExecutionStatus.MISFIRED,
                    ExecutionStatus.SUPERSEDED,
                    ExecutionStatus.CANCELLED
                )
            ) {
                assertTrue(updated?.completedAt != null)
            } else {
                assertFalse(updated?.completedAt != null)
            }
        }
    }

    @Test
    fun `dialects cover lock exclusion and listExecutions sql branches`() {
        val legacy = object : JdbcDialect {
            override fun upsertJobSql(): String = ""
            override fun upsertExecutionSql(): String = ""
            override fun claimExecutionSql(): String = ""
            override fun listEligibleExecutionsSql(): String = ""
            override fun heartbeatSql(): String = ""
            override fun isLockHeldSql(excludeId: Boolean): String = ""
            override fun resetExpiredExecutionsSql(): String = ""
            override fun cleanupCompletedExecutionsSql(statusCount: Int): String = ""
        }
        listOf(PostgresDialect(), H2Dialect(), MySqlDialect(), OracleDialect(), legacy).forEach { dialect ->
            val unfiltered = dialect.listExecutionsSql(
                hasJobId = false,
                statusCount = 0,
                hasScheduledFrom = false,
                hasScheduledUntil = false
            )
            assertFalse(unfiltered.contains("WHERE"))
            assertTrue(unfiltered.contains("ORDER BY scheduled_at DESC, id DESC"))

            val filtered = dialect.listExecutionsSql(
                hasJobId = true,
                statusCount = 2,
                hasScheduledFrom = true,
                hasScheduledUntil = true
            )
            assertTrue(filtered.contains("WHERE job_id = ? AND status IN (?,?) AND scheduled_at >= ? AND scheduled_at <= ?"))
            if (dialect is OracleDialect) {
                assertTrue(filtered.contains("FETCH FIRST ? ROWS ONLY"))
            } else {
                assertTrue(filtered.contains("LIMIT ?"))
            }
        }
        listOf(PostgresDialect(), H2Dialect(), MySqlDialect(), OracleDialect()).forEach { dialect ->
            assertFalse(dialect.isLockHeldSql(false).contains("id !="))
            assertTrue(dialect.isLockHeldSql(true).contains("id !="))
        }
        assertEquals("SELECT definition_json FROM khrona_jobs WHERE id = ? FOR SHARE", PostgresDialect().selectJobForClaimSql())
        assertEquals("SELECT definition_json FROM khrona_jobs WHERE id = ? FOR SHARE", MySqlDialect().selectJobForClaimSql())
        assertEquals("SELECT definition_json FROM khrona_jobs WHERE id = ? FOR UPDATE", H2Dialect().selectJobForClaimSql())
        assertEquals("SELECT definition_json FROM khrona_jobs WHERE id = ? FOR UPDATE", OracleDialect().selectJobForClaimSql())
        assertEquals("SELECT definition_json FROM khrona_jobs WHERE id = ? FOR UPDATE", legacy.selectJobForClaimSql())
    }

    @Test
    fun `dialects cover all core sql statements`() {
        listOf(PostgresDialect(), H2Dialect(), MySqlDialect(), OracleDialect()).forEach { dialect ->
            assertTrue(dialect.upsertJobSql().contains("khrona_jobs"))
            assertTrue(dialect.upsertExecutionSql().contains("khrona_executions"))
            assertTrue(dialect.claimExecutionSql().contains("CLAIMED"))
            assertTrue(dialect.listEligibleExecutionsSql().contains("SELECT") && dialect.listEligibleExecutionsSql().contains("khrona_executions"))
            assertFalse(dialect.listEligibleExecutionsSql(0).contains("job_id IN"))
            assertTrue(dialect.listEligibleExecutionsSql(2).contains("AND job_id IN (?,?)"))
            assertTrue(dialect.heartbeatSql().contains("UPDATE khrona_executions"))
            assertTrue(dialect.resetExpiredExecutionsSql().contains("PENDING"))
        }
        val legacyCustomDialect = object : JdbcDialect {
            override fun upsertJobSql(): String = "INSERT INTO custom_jobs"
            override fun upsertExecutionSql(): String = "INSERT INTO custom_executions"
            override fun claimExecutionSql(): String = "UPDATE custom_executions"
            override fun listEligibleExecutionsSql(): String = "SELECT * FROM custom_executions"
            override fun heartbeatSql(): String = "UPDATE custom_executions"
            override fun isLockHeldSql(excludeId: Boolean): String = "SELECT COUNT(*)"
            override fun resetExpiredExecutionsSql(): String = "UPDATE custom_executions"
            override fun cleanupCompletedExecutionsSql(statusCount: Int): String = "DELETE FROM custom_executions"
        }
        assertEquals("SELECT * FROM custom_executions", legacyCustomDialect.listEligibleExecutionsSql(3))
        assertEquals("SELECT COUNT(*)", legacyCustomDialect.isLockHeldSql())
    }

    @Test
    fun `dialects cover cleanup completed executions sql`() {
        listOf(PostgresDialect(), H2Dialect(), MySqlDialect(), OracleDialect()).forEach { dialect ->
            val sql = dialect.cleanupCompletedExecutionsSql(3)
            assertTrue(sql.contains("DELETE") && sql.contains("FROM khrona_executions"))
            assertTrue(sql.contains("completed_at < ?"))
        }
        assertTrue(PostgresDialect().cleanupCompletedExecutionsSql(3).contains("FOR UPDATE SKIP LOCKED"))
        assertTrue(MySqlDialect().cleanupCompletedExecutionsSql(3).contains("FOR UPDATE SKIP LOCKED"))
        assertFalse(H2Dialect().cleanupCompletedExecutionsSql(3).contains("FOR UPDATE SKIP LOCKED"))
        assertFalse(OracleDialect().cleanupCompletedExecutionsSql(3).contains("FOR UPDATE SKIP LOCKED"))
        val oracleSkipLockedSql = OracleDialect().selectCompletedExecutionsForCleanupSql(3)
        assertTrue(oracleSkipLockedSql.contains("FOR UPDATE SKIP LOCKED"))
        assertFalse(oracleSkipLockedSql.contains("ROWNUM"))
    }

    @Test
    fun `resolveDialect covers known and fallback database products`() {
        assertInstanceOf(PostgresDialect::class.java, JdbcJobStore.resolveDialect(dataSource("PostgreSQL")))
        assertInstanceOf(MySqlDialect::class.java, JdbcJobStore.resolveDialect(dataSource("MySQL")))
        assertInstanceOf(MySqlDialect::class.java, JdbcJobStore.resolveDialect(dataSource("MariaDB")))
        assertInstanceOf(OracleDialect::class.java, JdbcJobStore.resolveDialect(dataSource("Oracle")))
        assertInstanceOf(H2Dialect::class.java, JdbcJobStore.resolveDialect(dataSource("H2")))
        assertInstanceOf(H2Dialect::class.java, JdbcJobStore.resolveDialect(dataSource("SQLite")))
    }

    @Test
    fun `H2 store covers active lock exclusion false branch`() = runBlocking {
        val execution = JobExecution(jobId = "jdbc-lock-excluded", scheduledAt = Instant.now(), lockKey = "only-lock")
        store.saveExecution(execution)
        store.claimExecution(execution.id, "worker", Duration.ofMinutes(1))

        assertFalse(store.isLockHeld("only-lock", excludeExecutionId = execution.id))
    }

    @Test
    fun `migration ignorable error detection covers duplicate and oracle branches`() {
        val method = JdbcJobStore::class.java.getDeclaredMethod(
            "isIgnorableMigrationError",
            String::class.java,
            Exception::class.java
        )
        method.isAccessible = true
        val store = JdbcJobStore(dataSource, H2Dialect())

        assertEquals(true, method.invoke(store, "CREATE INDEX idx ON table_name(id)", SQLException("already exists")))
        assertEquals(true, method.invoke(store, "CREATE INDEX idx ON table_name(id)", SQLException("Duplicate key name 'idx'")))
        assertEquals(true, method.invoke(store, "CREATE TABLE sample(id INT)", SQLException("ORA-00955: name is already used")))
        assertEquals(false, method.invoke(store, "CREATE TABLE sample(id INT)", SQLException("already exists")))
        assertEquals(false, method.invoke(store, "ALTER TABLE sample ADD id INT", SQLException("ORA-00955")))
        assertEquals(false, method.invoke(store, "CREATE INDEX idx ON table_name(id)", SQLException(null as String?)))
    }

    @OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)
    @Test
    fun `toAny covers primitive non string fallback and json elements`() {
        val method = JdbcJobStore::class.java.getDeclaredMethod(
            "toAny",
            kotlinx.serialization.json.JsonElement::class.java
        )
        method.isAccessible = true
        val store = JdbcJobStore(dataSource, H2Dialect())

        val unquoted = kotlinx.serialization.json.JsonUnquotedLiteral("custom_unquoted_literal")
        val nullElement = kotlinx.serialization.json.JsonNull
        val jsonArray = kotlinx.serialization.json.JsonArray(listOf(nullElement, kotlinx.serialization.json.JsonPrimitive(42L)))
        val jsonObject = kotlinx.serialization.json.JsonObject(mapOf("key" to nullElement))

        assertEquals("custom_unquoted_literal", method.invoke(store, unquoted))
        assertEquals(null, method.invoke(store, nullElement))
        assertEquals(listOf(null, 42L), method.invoke(store, jsonArray))
        assertEquals(mapOf("key" to null), method.invoke(store, jsonObject))
    }

    @Test
    fun `getDialect resolves dialect dynamically when none is provided`() = runBlocking {
        val dynamicStore = JdbcJobStore(dataSource)
        val job = io.khrona.core.JobDefinition("dynamic-job", trigger = io.khrona.core.IntervalTrigger(Duration.ofMinutes(1)))
        dynamicStore.saveJob(job)
        assertEquals("dynamic-job", dynamicStore.getJob("dynamic-job")?.id)
    }

    @Test
    fun `migration skips ignorable statement errors and restores autocommit`() = runBlocking {
        val connection = migrationConnection { statement ->
            if (statement.startsWith("CREATE INDEX", ignoreCase = true)) {
                throw SQLException("already exists")
            }
            true
        }
        val store = JdbcJobStore(singleConnectionDataSource(connection), H2Dialect())

        store.migrate()

        assertEquals(true, connection.autoCommit)
        assertEquals(true, connection.committed)
        assertEquals(false, connection.rolledBack)
    }

    @Test
    fun `migration rolls back and restores autocommit on non ignorable statement errors`() {
        val connection = migrationConnection { statement ->
            if (statement.startsWith("CREATE TABLE", ignoreCase = true)) {
                throw SQLException("not ignorable")
            }
            true
        }
        val store = JdbcJobStore(singleConnectionDataSource(connection), H2Dialect())

        assertThrows(SQLException::class.java) {
            runBlocking {
                store.migrate()
            }
        }

        assertEquals(true, connection.autoCommit)
        assertEquals(true, connection.rolledBack)
    }

    private fun dataSource(productName: String): DataSource {
        val metadata = Proxy.newProxyInstance(
            DatabaseMetaData::class.java.classLoader,
            arrayOf(DatabaseMetaData::class.java)
        ) { _, method, _ ->
            when (method.name) {
                "getDatabaseProductName" -> productName
                else -> defaultValue(method.returnType)
            }
        } as DatabaseMetaData
        val connection = Proxy.newProxyInstance(
            Connection::class.java.classLoader,
            arrayOf(Connection::class.java)
        ) { _, method, _ ->
            when (method.name) {
                "getMetaData" -> metadata
                "close" -> null
                else -> defaultValue(method.returnType)
            }
        } as Connection
        return Proxy.newProxyInstance(
            DataSource::class.java.classLoader,
            arrayOf(DataSource::class.java)
        ) { _, method, _ ->
            when (method.name) {
                "getConnection" -> connection
                else -> defaultValue(method.returnType)
            }
        } as DataSource
    }

    private fun defaultValue(type: Class<*>): Any? {
        return when (type) {
            java.lang.Boolean.TYPE -> false
            java.lang.Byte.TYPE -> 0.toByte()
            java.lang.Short.TYPE -> 0.toShort()
            java.lang.Integer.TYPE -> 0
            java.lang.Long.TYPE -> 0L
            java.lang.Float.TYPE -> 0f
            java.lang.Double.TYPE -> 0.0
            java.lang.Character.TYPE -> 0.toChar()
            else -> null
        }
    }

    private fun migrationConnection(execute: (String) -> Boolean): MigrationConnection {
        val state = MigrationConnection()
        val statement = Proxy.newProxyInstance(
            Statement::class.java.classLoader,
            arrayOf(Statement::class.java)
        ) { _, method, args ->
            when (method.name) {
                "execute" -> execute(args?.get(0) as String)
                "close" -> null
                else -> defaultValue(method.returnType)
            }
        } as Statement
        state.proxy = Proxy.newProxyInstance(
            Connection::class.java.classLoader,
            arrayOf(Connection::class.java)
        ) { _, method, args ->
            when (method.name) {
                "getAutoCommit" -> state.autoCommit
                "setAutoCommit" -> {
                    state.autoCommit = args?.get(0) as Boolean
                    null
                }
                "createStatement" -> statement
                "commit" -> {
                    state.committed = true
                    null
                }
                "rollback" -> {
                    state.rolledBack = true
                    null
                }
                "close" -> null
                else -> defaultValue(method.returnType)
            }
        } as Connection
        return state
    }

    private fun singleConnectionDataSource(connection: MigrationConnection): DataSource {
        return Proxy.newProxyInstance(
            DataSource::class.java.classLoader,
            arrayOf(DataSource::class.java)
        ) { _, method, _ ->
            when (method.name) {
                "getConnection" -> connection.proxy
                else -> defaultValue(method.returnType)
            }
        } as DataSource
    }

    private class MigrationConnection {
        lateinit var proxy: Connection
        var autoCommit: Boolean = true
        var committed: Boolean = false
        var rolledBack: Boolean = false
    }

    @Test
    fun `should apply queryTimeoutSeconds to PreparedStatements`() = runBlocking {
        val observedTimeouts = java.util.concurrent.CopyOnWriteArrayList<Int>()

        // Wrap the real dataSource to intercept setQueryTimeout calls
        val spyDataSource = Proxy.newProxyInstance(
            DataSource::class.java.classLoader,
            arrayOf(DataSource::class.java)
        ) { _, method, _ ->
            when (method.name) {
                "getConnection" -> {
                    val realConn = dataSource.connection
                    Proxy.newProxyInstance(
                        Connection::class.java.classLoader,
                        arrayOf(Connection::class.java)
                    ) { _, connMethod, connArgs ->
                        when (connMethod.name) {
                            "prepareStatement" -> {
                                val realStmt = realConn.prepareStatement(connArgs!![0] as String)
                                Proxy.newProxyInstance(
                                    java.sql.PreparedStatement::class.java.classLoader,
                                    arrayOf(java.sql.PreparedStatement::class.java)
                                ) { _, stmtMethod, stmtArgs ->
                                    if (stmtMethod.name == "setQueryTimeout") {
                                        observedTimeouts.add(stmtArgs!![0] as Int)
                                    }
                                    if (stmtArgs != null) stmtMethod.invoke(realStmt, *stmtArgs)
                                    else stmtMethod.invoke(realStmt)
                                }
                            }
                            "close" -> realConn.close()
                            else -> {
                                if (connArgs != null) connMethod.invoke(realConn, *connArgs)
                                else connMethod.invoke(realConn)
                            }
                        }
                    }
                }
                else -> null
            }
        } as DataSource

        val timeoutStore = JdbcJobStore(spyDataSource, H2Dialect(), queryTimeoutSeconds = 7)

        val job = io.khrona.core.JobDefinition(
            id = "timeout-test-job",
            handler = {},
            trigger = io.khrona.core.IntervalTrigger(java.time.Duration.ofMinutes(1))
        )
        timeoutStore.saveJob(job)

        assertTrue(observedTimeouts.isNotEmpty(), "queryTimeout should have been set on at least one PreparedStatement")
        assertTrue(observedTimeouts.all { it == 7 }, "All query timeouts should be 7, but got: $observedTimeouts")
    }

    @Test
    fun `should default queryTimeoutSeconds to 30`() = runBlocking {
        val observedTimeouts = java.util.concurrent.CopyOnWriteArrayList<Int>()

        val spyDataSource = Proxy.newProxyInstance(
            DataSource::class.java.classLoader,
            arrayOf(DataSource::class.java)
        ) { _, method, _ ->
            when (method.name) {
                "getConnection" -> {
                    val realConn = dataSource.connection
                    Proxy.newProxyInstance(
                        Connection::class.java.classLoader,
                        arrayOf(Connection::class.java)
                    ) { _, connMethod, connArgs ->
                        when (connMethod.name) {
                            "prepareStatement" -> {
                                val realStmt = realConn.prepareStatement(connArgs!![0] as String)
                                Proxy.newProxyInstance(
                                    java.sql.PreparedStatement::class.java.classLoader,
                                    arrayOf(java.sql.PreparedStatement::class.java)
                                ) { _, stmtMethod, stmtArgs ->
                                    if (stmtMethod.name == "setQueryTimeout") {
                                        observedTimeouts.add(stmtArgs!![0] as Int)
                                    }
                                    if (stmtArgs != null) stmtMethod.invoke(realStmt, *stmtArgs)
                                    else stmtMethod.invoke(realStmt)
                                }
                            }
                            "close" -> realConn.close()
                            else -> {
                                if (connArgs != null) connMethod.invoke(realConn, *connArgs)
                                else connMethod.invoke(realConn)
                            }
                        }
                    }
                }
                else -> null
            }
        } as DataSource

        val defaultStore = JdbcJobStore(spyDataSource, H2Dialect())
        val job = io.khrona.core.JobDefinition(
            id = "default-timeout-job",
            handler = {},
            trigger = io.khrona.core.IntervalTrigger(java.time.Duration.ofMinutes(1))
        )
        defaultStore.saveJob(job)

        assertTrue(observedTimeouts.isNotEmpty(), "queryTimeout should have been set")
        assertTrue(observedTimeouts.all { it == JdbcJobStore.DEFAULT_QUERY_TIMEOUT_SECONDS })
        assertEquals(30, JdbcJobStore.DEFAULT_QUERY_TIMEOUT_SECONDS)
    }

    @Test
    fun `should reject non-positive queryTimeoutSeconds`() {
        assertThrows(IllegalArgumentException::class.java) {
            JdbcJobStore(dataSource, queryTimeoutSeconds = 0)
        }
        assertThrows(IllegalArgumentException::class.java) {
            JdbcJobStore(dataSource, queryTimeoutSeconds = -5)
        }
    }

    @Test
    fun `completeExecution rolls back status update when follow-up persistence fails`() = runBlocking<Unit> {
        val current = JobExecution(jobId = "rollback-complete", scheduledAt = Instant.now())
        store.saveExecution(current)
        assertTrue(store.claimExecution(current.id, "worker-1", Duration.ofMinutes(5)))

        val invalidFollowUp = JobExecution(
            jobId = "rollback-complete",
            scheduledAt = Instant.now().plusSeconds(60),
            payload = Any()
        )
        assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                store.completeExecution(
                    id = current.id,
                    status = ExecutionStatus.SUCCESS,
                    workerId = "worker-1",
                    nextExecution = invalidFollowUp
                )
            }
        }

        assertEquals(ExecutionStatus.CLAIMED, store.getExecution(current.id)?.status)
        org.junit.jupiter.api.Assertions.assertNull(store.getExecution(invalidFollowUp.id))
    }

    @Test
    fun `store persists all write operations when connection pool has autoCommit disabled`() = runBlocking<Unit> {
        val hikari = com.zaxxer.hikari.HikariDataSource(
            com.zaxxer.hikari.HikariConfig().apply {
                jdbcUrl = "jdbc:h2:mem:khrona_autocommit_false_${java.util.UUID.randomUUID()};MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1"
                username = "sa"
                password = ""
                driverClassName = "org.h2.Driver"
                isAutoCommit = false
                maximumPoolSize = 2
            }
        )
        try {
            val nonAutoCommitStore = JdbcJobStore(hikari)
            nonAutoCommitStore.migrate()

            val job = io.khrona.core.JobDefinition(
                id = "autocommit-false-job",
                handler = {},
                trigger = io.khrona.core.IntervalTrigger(Duration.ofMinutes(5))
            )
            nonAutoCommitStore.saveJob(job)
            assertEquals(job.id, nonAutoCommitStore.getJob(job.id)?.id)

            val exec = JobExecution(
                jobId = job.id,
                scheduledAt = Instant.now().minusSeconds(10)
            )
            nonAutoCommitStore.saveExecution(exec)
            assertEquals(ExecutionStatus.PENDING, nonAutoCommitStore.getExecution(exec.id)?.status)

            assertTrue(nonAutoCommitStore.claimExecution(exec.id, "worker-1", Duration.ofSeconds(10)))
            assertEquals(ExecutionStatus.CLAIMED, nonAutoCommitStore.getExecution(exec.id)?.status)

            assertTrue(nonAutoCommitStore.heartbeat(exec.id, "worker-1", Duration.ofMinutes(5)))
            assertTrue(nonAutoCommitStore.updateExecutionStatus(exec.id, ExecutionStatus.SUCCESS, workerId = "worker-1"))
            assertEquals(ExecutionStatus.SUCCESS, nonAutoCommitStore.getExecution(exec.id)?.status)

            val expiredExec = JobExecution(
                jobId = job.id,
                scheduledAt = Instant.now().minusSeconds(20)
            )
            nonAutoCommitStore.saveExecution(expiredExec)
            assertTrue(nonAutoCommitStore.claimExecution(expiredExec.id, "worker-1", Duration.ofSeconds(-5)))
            assertEquals(1, nonAutoCommitStore.resetExpiredExecutions(Instant.now()))
            assertEquals(ExecutionStatus.PENDING, nonAutoCommitStore.getExecution(expiredExec.id)?.status)

            val deleted = nonAutoCommitStore.cleanupCompletedExecutions(Instant.now().plusSeconds(60))
            assertEquals(1, deleted)
            org.junit.jupiter.api.Assertions.assertNull(nonAutoCommitStore.getExecution(exec.id))
        } finally {
            hikari.close()
        }
    }

    @Test
    fun `store binds and reads all JDBC timestamps using explicit UTC Calendar`() = runBlocking<Unit> {
        val nonUtcBindings = java.util.concurrent.atomic.AtomicInteger(0)
        val utcBindings = java.util.concurrent.atomic.AtomicInteger(0)
        val utcReads = java.util.concurrent.atomic.AtomicInteger(0)

        fun wrapResultSet(rs: java.sql.ResultSet): java.sql.ResultSet {
            return java.lang.reflect.Proxy.newProxyInstance(
                java.sql.ResultSet::class.java.classLoader,
                arrayOf(java.sql.ResultSet::class.java)
            ) { _, method, args ->
                if (method.name == "getTimestamp") {
                    if (args != null && args.size == 2 && args[1] is java.util.Calendar) {
                        val cal = args[1] as java.util.Calendar
                        if (cal.timeZone.rawOffset == 0) {
                            utcReads.incrementAndGet()
                        } else {
                            nonUtcBindings.incrementAndGet()
                        }
                    } else {
                        nonUtcBindings.incrementAndGet()
                    }
                }
                try {
                    method.invoke(rs, *(args ?: emptyArray()))
                } catch (e: java.lang.reflect.InvocationTargetException) {
                    throw e.targetException
                }
            } as java.sql.ResultSet
        }

        fun wrapStatement(stmt: java.sql.PreparedStatement): java.sql.PreparedStatement {
            return java.lang.reflect.Proxy.newProxyInstance(
                java.sql.PreparedStatement::class.java.classLoader,
                arrayOf(java.sql.PreparedStatement::class.java)
            ) { _, method, args ->
                if (method.name == "setTimestamp") {
                    if (args != null && args.size == 3 && args[2] is java.util.Calendar) {
                        val cal = args[2] as java.util.Calendar
                        if (cal.timeZone.rawOffset == 0) {
                            utcBindings.incrementAndGet()
                        } else {
                            nonUtcBindings.incrementAndGet()
                        }
                    } else {
                        nonUtcBindings.incrementAndGet()
                    }
                }
                val result = try {
                    method.invoke(stmt, *(args ?: emptyArray()))
                } catch (e: java.lang.reflect.InvocationTargetException) {
                    throw e.targetException
                }
                if (method.name == "executeQuery" && result is java.sql.ResultSet) {
                    wrapResultSet(result)
                } else {
                    result
                }
            } as java.sql.PreparedStatement
        }

        fun wrapConnection(conn: Connection): Connection {
            return java.lang.reflect.Proxy.newProxyInstance(
                Connection::class.java.classLoader,
                arrayOf(Connection::class.java)
            ) { _, method, args ->
                val result = try {
                    method.invoke(conn, *(args ?: emptyArray()))
                } catch (e: java.lang.reflect.InvocationTargetException) {
                    throw e.targetException
                }
                if (method.name == "prepareStatement" && result is java.sql.PreparedStatement) {
                    wrapStatement(result)
                } else {
                    result
                }
            } as Connection
        }

        val trackingDs = java.lang.reflect.Proxy.newProxyInstance(
            DataSource::class.java.classLoader,
            arrayOf(DataSource::class.java)
        ) { _, method, args ->
            val result = try {
                method.invoke(dataSource, *(args ?: emptyArray()))
            } catch (e: java.lang.reflect.InvocationTargetException) {
                throw e.targetException
            }
            if (method.name == "getConnection" && result is Connection) {
                wrapConnection(result)
            } else {
                result
            }
        } as DataSource

        val utcStore = JdbcJobStore(trackingDs, H2Dialect())
        val now = Instant.parse("2026-04-20T12:00:00Z")
        val exec = JobExecution(jobId = "utc-cal-job", scheduledAt = now, lockKey = "utc-lock")
        utcStore.saveExecution(exec)
        assertEquals(now, utcStore.getExecution(exec.id)?.scheduledAt)
        assertEquals(1, utcStore.listEligibleExecutions(now.plusSeconds(1), 10, setOf("utc-cal-job")).size)
        assertTrue(utcStore.claimExecution(exec.id, "worker-1", Duration.ofSeconds(30)))
        assertTrue(utcStore.heartbeat(exec.id, "worker-1", Duration.ofSeconds(60)))
        assertTrue(utcStore.isLockHeld("utc-lock", null))
        assertEquals(1, utcStore.supersedeExecutionsByLockKey("utc-lock", null).size)
        assertEquals(0, utcStore.resetExpiredExecutions(now.plusSeconds(120)))
        assertTrue(utcStore.updateExecutionStatus(exec.id, ExecutionStatus.SUCCESS))
        assertEquals(1, utcStore.cleanupCompletedExecutions(Instant.now().plusSeconds(60)))

        assertEquals(0, nonUtcBindings.get(), "All setTimestamp and getTimestamp calls must pass an explicit UTC Calendar")
        assertTrue(utcBindings.get() > 0)
        assertTrue(utcReads.get() > 0)
    }

    @Test
    fun `legacy custom dialect must execute filtered polling`() = runBlocking {
        HikariDataSource(HikariConfig().apply {
            jdbcUrl = "jdbc:h2:mem:custom_${java.util.UUID.randomUUID()};MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1"
            username = "sa"
            password = ""
        }).use { ds ->
            val h2 = H2Dialect()
            val legacy = object : JdbcDialect {
                override fun upsertJobSql() = h2.upsertJobSql()
                override fun upsertExecutionSql() = h2.upsertExecutionSql()
                override fun claimExecutionSql() = h2.claimExecutionSql()
                override fun listEligibleExecutionsSql() = h2.listEligibleExecutionsSql()
                override fun heartbeatSql() = h2.heartbeatSql()
                override fun isLockHeldSql(excludeId: Boolean) = h2.isLockHeldSql(excludeId)
                override fun resetExpiredExecutionsSql() = h2.resetExpiredExecutionsSql()
                override fun cleanupCompletedExecutionsSql(statusCount: Int) = h2.cleanupCompletedExecutionsSql(statusCount)
            }
            val store = JdbcJobStore(ds, legacy)
            store.migrate()
            val job = io.khrona.core.JobDefinition(id = "custom-job", trigger = io.khrona.core.OneTimeTrigger(Instant.EPOCH))
            store.saveJob(job)
            store.saveExecution(JobExecution(jobId = job.id, scheduledAt = Instant.EPOCH))
            assertEquals(1, store.listEligibleExecutions(Instant.now(), 10, setOf(job.id)).size)
        }
    }

    @Test
    fun `claimExecution handles null and corrupt job definition json safely`() = runBlocking {
        val nullDefExec = JobExecution(jobId = "null-def-job", scheduledAt = Instant.EPOCH)
        val corruptDefExec = JobExecution(jobId = "corrupt-def-job", scheduledAt = Instant.EPOCH)
        dataSource.connection.use { conn ->
            conn.prepareStatement("INSERT INTO khrona_jobs (id, definition_json) VALUES (?, ?)").use { stmt ->
                stmt.setString(1, "null-def-job")
                stmt.setNull(2, java.sql.Types.VARCHAR)
                stmt.executeUpdate()
                stmt.setString(1, "corrupt-def-job")
                stmt.setString(2, "{corrupt-json")
                stmt.executeUpdate()
            }
        }
        store.saveExecution(nullDefExec)
        store.saveExecution(corruptDefExec)

        assertTrue(store.claimExecution(nullDefExec.id, "worker-1", Duration.ofMinutes(1)))
        assertEquals(ExecutionStatus.CLAIMED, store.getExecution(nullDefExec.id)?.status)

        assertFalse(store.claimExecution(corruptDefExec.id, "worker-1", Duration.ofMinutes(1)))
        assertEquals(ExecutionStatus.PENDING, store.getExecution(corruptDefExec.id)?.status)
    }
}

