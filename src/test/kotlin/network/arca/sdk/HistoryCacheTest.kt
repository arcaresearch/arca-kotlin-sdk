package network.arca.sdk

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class HistoryCacheTest {

    @Test
    fun cacheMissReturnsNull() {
        val cache = HistoryCache()
        assertNull(cache.get<String>("missing"))
    }

    @Test
    fun storeAndRetrieve() {
        val cache = HistoryCache()
        cache.set("key1", "hello")
        assertEquals("hello", cache.get<String>("key1"))
    }

    @Test
    fun evictsLeastRecentlyUsed() {
        val cache = HistoryCache(CacheConfig(maxEntries = 3))
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)
        assertEquals(3, cache.size)

        cache.set("d", 4)
        assertEquals(3, cache.size)

        assertNull(cache.get<Int>("a"))
        assertEquals(2, cache.get<Int>("b"))
        assertEquals(3, cache.get<Int>("c"))
        assertEquals(4, cache.get<Int>("d"))
    }

    @Test
    fun accessPromotesEntry() {
        val cache = HistoryCache(CacheConfig(maxEntries = 3))
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)

        cache.get<Int>("a")

        cache.set("d", 4)
        assertEquals(1, cache.get<Int>("a"), "Accessed entry should not be evicted")
        assertNull(cache.get<Int>("b"), "Oldest untouched entry should be evicted")
    }

    @Test
    fun updateExistingKeyDoesNotGrow() {
        val cache = HistoryCache(CacheConfig(maxEntries = 2))
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("a", 10)

        assertEquals(2, cache.size)
        assertEquals(10, cache.get<Int>("a"))
    }

    @Test
    fun clear() {
        val cache = HistoryCache()
        cache.set("a", 1)
        cache.set("b", 2)
        assertEquals(2, cache.size)

        cache.clear()
        assertEquals(0, cache.size)
        assertNull(cache.get<Int>("a"))
    }

    @Test
    fun disabledCacheIsNoop() {
        val cache = HistoryCache(CacheConfig.disabled)
        cache.set("a", 1)
        assertEquals(0, cache.size)
        assertNull(cache.get<Int>("a"))
    }

    @Test
    fun zeroMaxEntriesIsNoop() {
        val cache = HistoryCache(CacheConfig(maxEntries = 0))
        cache.set("a", 1)
        assertEquals(0, cache.size)
    }

    @Test
    fun defaultMaxEntries() {
        val cache = HistoryCache()
        for (i in 0 until 55) {
            cache.set("key-$i", i)
        }
        assertEquals(50, cache.size)
        assertNull(cache.get<Int>("key-0"))
        assertEquals(54, cache.get<Int>("key-54"))
    }

    @Test
    fun concurrentAccess() = runBlocking {
        val cache = HistoryCache(CacheConfig(maxEntries = 100))
        val jobs = (0 until 50).map { i ->
            launch(Dispatchers.Default) {
                cache.set("key-$i", i)
                cache.get<Int>("key-$i")
            }
        }
        jobs.forEach { it.join() }
        assertEquals(50, cache.size)
    }
}

class BuildCacheKeyTest {

    @Test
    fun deterministicWithSortedParams() {
        val key = buildCacheKey(
            "equityHistory",
            mapOf("to" to "2026-03-24", "from" to "2026-01-01", "prefix" to "/accounts", "points" to "200"),
        )
        assertEquals("equityHistory:from=2026-01-01&points=200&prefix=/accounts&to=2026-03-24", key)
    }

    @Test
    fun omitsNullValues() {
        val key = buildCacheKey(
            "candles",
            mapOf("market" to "BTC", "interval" to "1h", "startTime" to null, "endTime" to null),
        )
        assertEquals("candles:interval=1h&market=BTC", key)
    }

    @Test
    fun differentParamsProduceDifferentKeys() {
        val k1 = buildCacheKey("candles", mapOf("market" to "BTC", "interval" to "1h"))
        val k2 = buildCacheKey("candles", mapOf("market" to "BTC", "interval" to "4h"))
        assertNotEquals(k1, k2)
    }
}
