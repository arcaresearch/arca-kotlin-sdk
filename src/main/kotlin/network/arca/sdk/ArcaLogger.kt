package network.arca.sdk

import java.util.concurrent.Executors
import java.util.logging.Level
import java.util.logging.Logger as JulLogger

/**
 * Severity levels for [ArcaLogRecord].
 *
 * Ordering is `DEBUG < INFO < NOTICE < WARNING < ERROR`. Set [ArcaLogger.minLevel]
 * to control which records are emitted.
 */
public enum class ArcaLogLevel(internal val severity: Int) {
    DEBUG(0),
    INFO(1),
    NOTICE(2),
    WARNING(3),
    ERROR(4),
}

/**
 * A single diagnostic record emitted by the SDK.
 *
 * [metadata] contains structured context such as `errorId`, `path`, `market`,
 * `operationId`, `statusCode`, `httpMethod`, or `url`. Values are always
 * strings so they can be forwarded to arbitrary backends without further
 * encoding.
 */
public data class ArcaLogRecord(
    public val level: ArcaLogLevel,
    public val category: String,
    public val message: String,
    public val error: Throwable? = null,
    public val metadata: Map<String, String> = emptyMap(),
    public val timestampMillis: Long = System.currentTimeMillis(),
)

/**
 * A handler that receives diagnostic records from the SDK. Implement this to
 * forward records to Datadog, Sentry, Crashlytics, or a custom backend.
 * Handlers are invoked on a single background thread, so implementations do
 * not need to be internally thread-safe.
 */
public fun interface ArcaLogHandler {
    public fun handle(record: ArcaLogRecord)
}

/**
 * Internal logger used by every SDK subsystem. Writes to [java.util.logging]
 * (logger name `io.arcaos.sdk.<category>`) and, if configured, forwards a
 * structured [ArcaLogRecord] to a host-provided [ArcaLogHandler].
 *
 * Message strings are captured as a lambda so formatting work is skipped when
 * the record is below [minLevel].
 */
public class ArcaLogger(
    minLevel: ArcaLogLevel = ArcaLogLevel.WARNING,
    private val handler: ArcaLogHandler? = null,
) {
    /** Minimum level at which records are emitted. Records below are dropped. */
    @Volatile
    public var minLevel: ArcaLogLevel = minLevel

    private val handlerExecutor = if (handler != null) {
        Executors.newSingleThreadExecutor { r -> Thread(r, "arca-sdk-logger").apply { isDaemon = true } }
    } else {
        null
    }

    public fun log(
        level: ArcaLogLevel,
        category: String,
        error: Throwable? = null,
        metadata: Map<String, String> = emptyMap(),
        message: () -> String,
    ) {
        if (level.severity < minLevel.severity) return

        val rendered = message()
        val line = rendered + renderMetadata(metadata) + (error?.let { " error=$it" } ?: "")
        JulLogger.getLogger("$SUBSYSTEM.$category").log(julLevel(level), line)

        val h = handler ?: return
        val record = ArcaLogRecord(level, category, rendered, error, metadata)
        handlerExecutor?.execute { runCatching { h.handle(record) } }
    }

    public fun debug(category: String, error: Throwable? = null, metadata: Map<String, String> = emptyMap(), message: () -> String): Unit =
        log(ArcaLogLevel.DEBUG, category, error, metadata, message)

    public fun info(category: String, error: Throwable? = null, metadata: Map<String, String> = emptyMap(), message: () -> String): Unit =
        log(ArcaLogLevel.INFO, category, error, metadata, message)

    public fun notice(category: String, error: Throwable? = null, metadata: Map<String, String> = emptyMap(), message: () -> String): Unit =
        log(ArcaLogLevel.NOTICE, category, error, metadata, message)

    public fun warning(category: String, error: Throwable? = null, metadata: Map<String, String> = emptyMap(), message: () -> String): Unit =
        log(ArcaLogLevel.WARNING, category, error, metadata, message)

    public fun error(category: String, error: Throwable? = null, metadata: Map<String, String> = emptyMap(), message: () -> String): Unit =
        log(ArcaLogLevel.ERROR, category, error, metadata, message)

    private fun julLevel(level: ArcaLogLevel): Level = when (level) {
        ArcaLogLevel.DEBUG -> Level.FINE
        ArcaLogLevel.INFO -> Level.INFO
        ArcaLogLevel.NOTICE -> Level.CONFIG
        ArcaLogLevel.WARNING -> Level.WARNING
        ArcaLogLevel.ERROR -> Level.SEVERE
    }

    private fun renderMetadata(metadata: Map<String, String>): String {
        if (metadata.isEmpty()) return ""
        return " " + metadata.entries.sortedBy { it.key }.joinToString(" ") { "${it.key}=${it.value}" }
    }

    public companion object {
        public const val SUBSYSTEM: String = "io.arcaos.sdk"

        /** A singleton no-op logger used before an [Arca] instance is configured. */
        public val disabled: ArcaLogger = ArcaLogger(ArcaLogLevel.ERROR, null)
    }
}
