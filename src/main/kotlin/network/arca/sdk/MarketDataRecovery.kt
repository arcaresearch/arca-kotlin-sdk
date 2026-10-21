package network.arca.sdk

/** Hint that usable connectivity returned. Advances only an already-pending public Hyperliquid
 * retry, with 250–499 ms jitter and a 30 s cooldown. Never creates an owner, interrupts a live
 * connection, or resets failure backoff. Call on a connectivity restoration event, not a timer.
 * This does not reconnect the authenticated Arca transport or establish price freshness. */
public fun Arca.notifyMarketDataNetworkAvailable() { ws.notifyMarketDataNetworkAvailable() }

/** Serialized by the source. A token fences sleepers that resume after cancellation. */
internal class MarketRetryWindow {
    data class Plan(val token: Long, val delayMs: Long)
    private var token = 0L
    private var deadline: Long? = null
    private var lastAdvance: Long? = null
    fun schedule(now: Long, delayMs: Long): Plan {
        deadline = now + delayMs * 1_000_000
        return Plan(++token, delayMs)
    }
    fun advance(now: Long, jitterMs: Long): Plan? {
        val due = deadline ?: return null
        if (lastAdvance?.let { now - it < 30_000_000_000L } == true) return null
        val delayMs = 250 + jitterMs.coerceIn(0, 249)
        if (due - now <= delayMs * 1_000_000) return null // never postpone an imminent retry
        lastAdvance = now
        return schedule(now, delayMs)
    }
    fun take(expected: Long): Boolean {
        if (deadline == null || token != expected) return false
        deadline = null
        return true
    }
    fun cancel() { deadline = null; ++token }
}
