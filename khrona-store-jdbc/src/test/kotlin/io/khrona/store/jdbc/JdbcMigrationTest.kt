package io.khrona.store.jdbc

import kotlinx.coroutines.runBlocking
import org.h2.jdbcx.JdbcDataSource
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class JdbcMigrationTest {
    @Test
    fun `migration should throw on non-idempotency failures`() = runBlocking {
        val dataSource = JdbcDataSource().apply {
            setURL("jdbc:h2:mem:khrona_migration_failure;DB_CLOSE_DELAY=-1")
            user = "sa"
            password = ""
        }

        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("CREATE TABLE khrona_jobs (id VARCHAR(255) PRIMARY KEY)")
                stmt.execute("CREATE TABLE khrona_executions (id VARCHAR(36) PRIMARY KEY)")
            }
        }

        assertThrows(Exception::class.java) {
            runBlocking {
                val store = JdbcJobStore(dataSource, H2Dialect())
                store.migrate()
            }
        }
    }
}
