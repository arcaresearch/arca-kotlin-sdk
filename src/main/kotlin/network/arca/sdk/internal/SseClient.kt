package network.arca.sdk.internal

import kotlin.math.min
import kotlin.math.pow

/**
 * One server-sent event: the `id:`, `event:` and joined `data:` lines of a
 * block ended by a blank line. Comment lines (`: heartbeat`) never produce a
 * frame; a block with no `data:` line is dropped, as the spec requires.
 */
internal data class SseFrame(val id: String?, val event: String?, val data: String)

/**
 * Incremental server-sent-event parser. Feed it bytes (or whole lines
 * without their terminator); it returns a frame whenever a block completes.
 */
internal class SseParser {
    private var id: String? = null
    private var event: String? = null
    private val data = ArrayList<String>()
    private val pending = java.io.ByteArrayOutputStream()

    /** Consume one byte. Returns a frame when it completed a block's blank line. */
    fun consume(byte: Int): SseFrame? {
        if (byte != '\n'.code) {
            pending.write(byte)
            return null
        }
        var bytes = pending.toByteArray()
        pending.reset()
        if (bytes.isNotEmpty() && bytes.last() == '\r'.code.toByte()) bytes = bytes.copyOf(bytes.size - 1)
        return consume(String(bytes, Charsets.UTF_8))
    }

    /** Consume one line. Returns a frame when the line was a block's blank line. */
    fun consume(line: String): SseFrame? {
        if (line.isEmpty()) {
            val frame = if (data.isEmpty()) null else SseFrame(id, event, data.joinToString("\n"))
            id = null
            event = null
            data.clear()
            return frame
        }
        if (line.startsWith(":")) return null // comment / heartbeat
        val colon = line.indexOf(':')
        val field: String
        var value: String
        if (colon >= 0) {
            field = line.substring(0, colon)
            value = line.substring(colon + 1)
            if (value.startsWith(" ")) value = value.substring(1)
        } else {
            field = line
            value = ""
        }
        when (field) {
            "id" -> id = value
            "event" -> event = value
            "data" -> data.add(value)
            else -> {} // `retry` and unknown fields are ignored
        }
        return null
    }
}

/**
 * Reconnect schedule for streams: 1 s, 2 s, 4 s, … capped at 30 s — the
 * schedule the SDK's WebSocket uses.
 */
internal object SseBackoff {
    fun delayMillis(attempt: Int): Long = (min(2.0.pow(attempt.coerceAtLeast(0)), 30.0) * 1000).toLong()
}
