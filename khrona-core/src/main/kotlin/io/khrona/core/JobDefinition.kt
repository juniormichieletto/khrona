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
import java.util.UUID

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

object UUIDSerializer : KSerializer<UUID> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("UUID", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: UUID) = encoder.encodeString(value.toString())
    override fun deserialize(decoder: Decoder): UUID = UUID.fromString(decoder.decodeString())
}

typealias JobHandler = suspend (payload: Any?) -> Unit

enum class ConcurrencyPolicy {
    ALLOW,
    FORBID,
    REPLACE
}

@Serializable
enum class MisfirePolicy {
    FIRE_NOW,
    IGNORE
}

@Serializable
data class JobDefinition(
    val id: String,
    val description: String? = null,
    @kotlinx.serialization.Transient
    val handler: JobHandler = { },
    val trigger: Trigger,
    val retryPolicy: RetryPolicy = RetryPolicy.DEFAULT,
    val concurrencyPolicy: ConcurrencyPolicy = ConcurrencyPolicy.FORBID,
    val misfirePolicy: MisfirePolicy = MisfirePolicy.FIRE_NOW,
    val lockKey: String? = null,
    @Serializable(with = DurationSerializer::class)
    val timeout: Duration? = null
)

@Serializable
sealed interface Trigger {
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
    init {
        require(maxAttempts > 0) { "maxAttempts must be at least 1" }
        require(!initialDelay.isNegative) { "initialDelay must not be negative" }
        require(!maxDelay.isNegative) { "maxDelay must not be negative" }
        require(maxDelay >= initialDelay) { "maxDelay must be greater than or equal to initialDelay" }
        require(factor >= 1.0) { "factor must be at least 1.0" }
        require(jitter in 0.0..1.0) { "jitter must be between 0.0 and 1.0" }
    }

    fun calculateDelay(attempt: Int): Duration {
        if (attempt <= 0) return Duration.ZERO
        // Use Double arithmetic to preserve fractional factors before converting to long millis
        val backoffMillis = (initialDelay.toMillis() * Math.pow(factor, (attempt - 1).toDouble())).toLong()
        val backoff = Duration.ofMillis(backoffMillis)
        val cappedBackoff = if (backoff > maxDelay) maxDelay else backoff
        
        if (jitter <= 0.0) return cappedBackoff
        
        val jitterAmount = cappedBackoff.toMillis() * jitter * (Math.random() * 2 - 1)
        val finalDelay = cappedBackoff.plusMillis(jitterAmount.toLong())
        return if (finalDelay.isNegative) Duration.ZERO else finalDelay
    }

    companion object {
        val DEFAULT = RetryPolicy()
    }
}
