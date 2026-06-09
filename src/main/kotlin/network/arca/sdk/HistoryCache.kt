package network.arca.sdk

import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/** Configuration for the SDK's in-memory history cache. */
public class CacheConfig(
    /** Maximum number of cached responses. Default 50. Set to 0 to disable caching. */
    public val maxEntries: Int = 50,
) {
    public companion object {
        /** A disabled cache that stores nothing. */
        public val disabled: CacheConfig = CacheConfig(maxEntries = 0)
    }
}

/**
 * Thread-safe LRU cache for historical data responses (equity history, PnL
 * history, candles). Backed by an access-ordered [LinkedHashMap] guarded by a
 * lock — synchronous, so parallel callers don't serialize through an actor.
 */
public class HistoryCache(config: CacheConfig = CacheConfig()) {
    private val maxEntries: Int = config.maxEntries
    private val lock = ReentrantLock()

    private val map = object : LinkedHashMap<String, Any>(16, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, Any>?): Boolean =
            size > maxEntries
    }

    @Suppress("UNCHECKED_CAST")
    public fun <T> get(key: String): T? = lock.withLock {
        if (maxEntries <= 0) null else map[key] as T?
    }

    public fun set(key: String, value: Any): Unit = lock.withLock {
        if (maxEntries > 0) map[key] = value
    }

    public fun delete(key: String): Unit = lock.withLock {
        map.remove(key)
        Unit
    }

    public fun clear(): Unit = lock.withLock { map.clear() }

    public val size: Int
        get() = lock.withLock { map.size }
}

/** Build a deterministic cache key from a method name and sorted parameters. */
public fun buildCacheKey(method: String, params: Map<String, String?>): String {
    val parts = params.keys.sorted().mapNotNull { key ->
        params[key]?.let { "$key=$it" }
    }
    return "$method:${parts.joinToString("&")}"
}
