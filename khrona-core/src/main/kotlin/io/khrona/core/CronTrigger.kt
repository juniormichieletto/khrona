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
class CronTrigger(val expression: String, val context: String? = null) : Trigger {
    init {
        // Fast-fail: Validate the expression immediately
        try {
            CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX)).parse(expression)
        } catch (e: Exception) {
            val prefix = if (context != null) "Job '$context' has an invalid" else "Invalid"
            throw IllegalArgumentException("$prefix Unix cron expression '$expression': ${e.message}", e)
        }
    }

    @Transient
    private val cronDefinition = lazy { CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX) }
    @Transient
    private val parser = lazy { CronParser(cronDefinition.value) }
    @Transient
    private val cron = lazy { parser.value.parse(expression) }
    @Transient
    private val executionTime = lazy { ExecutionTime.forCron(cron.value) }

    override fun nextExecutionTime(after: Instant): Instant? {
        val zdt = ZonedDateTime.ofInstant(after, ZoneId.of("UTC"))
        return executionTime.value.nextExecution(zdt)
            .map { it.toInstant() }
            .orElse(null)
    }
}
