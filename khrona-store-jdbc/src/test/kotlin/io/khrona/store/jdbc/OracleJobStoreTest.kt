package io.khrona.store.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.testcontainers.oracle.OracleContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration

@Testcontainers
class OracleJobStoreTest : AbstractJdbcJobStoreTest() {

    companion object {
        @Container
        val oracle = OracleContainer("gvenzl/oracle-free:23-slim-faststart")
            .withDatabaseName("khrona")
            .withUsername("test")
            .withPassword("test")
            .withStartupTimeout(Duration.ofMinutes(2))
    }

    override fun createDataSource(): HikariDataSource {
        val config = HikariConfig().apply {
            jdbcUrl = oracle.jdbcUrl
            username = oracle.username
            password = oracle.password
            driverClassName = oracle.driverClassName
        }
        return HikariDataSource(config)
    }
}
