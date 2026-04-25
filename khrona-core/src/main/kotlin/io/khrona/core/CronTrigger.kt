package io.khrona.core

import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime

@Serializable
@SerialName("cron")
class CronTrigger(val expression: String) : Trigger {
    @Transient
    private val cronDefinition = lazy {
        val parts = expression.trim().split(Regex("\\s+")).filter { it.isNotEmpty() }
        when (parts.size) {
            5 -> CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX)
            6, 7 -> CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ)
            else -> CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ)
        }
    }
    @Transient
    private val parser = lazy { CronParser(cronDefinition.value) }
    @Transient
    private val cron = lazy { parser.value.parse(expression) }
    @Transient
    private val executionTime = lazy { ExecutionTime.forCron(cron.value) }

    override fun nextExecutionTime(after: Instant): Instant? {
        val zdt = ZonedDateTime.ofInstant(after, ZoneId.of("UTC"))
        return try {
            executionTime.value.nextExecution(zdt)
                .map { it.toInstant() }
                .orElse(null)
        } catch (e: Exception) {
            null
        }
    }
}
