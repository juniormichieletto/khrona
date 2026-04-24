package io.khrona.core
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import java.time.Clock
import java.time.Duration
import java.time.Instant

object DurationSerializer : KSerializer<Duration> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("Duration", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: Duration) = encoder.encodeString(value.toString())
    override fun deserialize(decoder: Decoder): Duration = Duration.parse(decoder.decodeString())
}

object InstantSerializer : KSerializer<Instant> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("Instant", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: Instant) = encoder.encodeString(value.toString())
    override fun deserialize(decoder: Decoder): Instant = Instant.parse(decoder.decodeString())
}

typealias JobHandler = suspend (payload: Any?) -> Unit

data class JobDefinition(
    val id: String,
    val description: String? = null,
    val handler: JobHandler,
    val trigger: Trigger,
    val retryPolicy: RetryPolicy = RetryPolicy.DEFAULT,
    val lockKey: String? = null,
    val timeout: Duration? = null
)

interface Trigger {
    fun nextExecutionTime(after: Instant): Instant?
}

@Serializable
data class RetryPolicy(
    val maxAttempts: Int = 3,
    @Serializable(with = DurationSerializer::class)
    val initialDelay: Duration = Duration.ofSeconds(1),
    @Serializable(with = DurationSerializer::class)
    val maxDelay: Duration = Duration.ofMinutes(5),
    val factor: Double = 2.0,
    val jitter: Double = 0.1
) {

    fun calculateDelay(attempt: Int): Duration {
        if (attempt <= 0) return Duration.ZERO
        val backoff = initialDelay.multipliedBy(Math.pow(factor, (attempt - 1).toDouble()).toLong())
        val cappedBackoff = if (backoff > maxDelay) maxDelay else backoff
        
        val jitterAmount = cappedBackoff.toMillis() * jitter * (Math.random() * 2 - 1)
        return cappedBackoff.plusMillis(jitterAmount.toLong())
    }

    companion object {
        val DEFAULT = RetryPolicy()
    }
}
