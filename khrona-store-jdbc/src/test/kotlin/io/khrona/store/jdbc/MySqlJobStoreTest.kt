package io.khrona.store.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
class MySqlJobStoreTest : AbstractJdbcJobStoreTest() {

    companion object {
        @Container
        val mysql = MySQLContainer("mysql:8.0").apply {
            withDatabaseName("khrona")
            withUsername("test")
            withPassword("test")
        }
    }

    override fun createDataSource(): HikariDataSource {
        val config = HikariConfig().apply {
            jdbcUrl = mysql.jdbcUrl
            username = mysql.username
            password = mysql.password
            driverClassName = mysql.driverClassName
        }
        return HikariDataSource(config)
    }
}
