package io.khrona.store.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.khrona.core.ExecutionStatus
import io.khrona.core.JobExecution
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
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
                    ExecutionStatus.SUPERSEDED
                )
            ) {
                assertTrue(updated?.completedAt != null)
            } else {
                assertFalse(updated?.completedAt != null)
            }
        }
    }

    @Test
    fun `dialects cover lock exclusion sql branches`() {
        listOf(PostgresDialect(), H2Dialect(), MySqlDialect(), OracleDialect()).forEach { dialect ->
            assertFalse(dialect.isLockHeldSql(false).contains("id !="))
            assertTrue(dialect.isLockHeldSql(true).contains("id !="))
        }
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
}
