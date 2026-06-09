package network.arca.sdk

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** Captures records emitted by [ArcaLogger] for test assertions. */
private class CapturingLogHandler : ArcaLogHandler {
    private val lock = Any()
    private val backing = mutableListOf<ArcaLogRecord>()

    val records: List<ArcaLogRecord>
        get() = synchronized(lock) { backing.toList() }

    override fun handle(record: ArcaLogRecord) {
        synchronized(lock) { backing.add(record) }
    }
}

/**
 * Await the handler queue so records emitted asynchronously by [ArcaLogger] are
 * visible before the test asserts on them.
 */
private suspend fun drainLogHandler(handler: CapturingLogHandler, expected: Int, timeoutMs: Long = 1000) {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (handler.records.size < expected && System.currentTimeMillis() < deadline) {
        delay(5)
    }
}

class ArcaLoggerTest {

    @Test
    fun levelComparisonOrdering() {
        assertTrue(ArcaLogLevel.DEBUG < ArcaLogLevel.INFO)
        assertTrue(ArcaLogLevel.INFO < ArcaLogLevel.NOTICE)
        assertTrue(ArcaLogLevel.NOTICE < ArcaLogLevel.WARNING)
        assertTrue(ArcaLogLevel.WARNING < ArcaLogLevel.ERROR)
    }

    @Test
    fun warningLevelDropsDebugAndInfo() = runBlocking {
        val handler = CapturingLogHandler()
        val log = ArcaLogger(minLevel = ArcaLogLevel.WARNING, handler = handler)

        log.debug("network") { "d" }
        log.info("network") { "i" }
        log.notice("network") { "n" }
        log.warning("network") { "w" }
        log.error("network") { "e" }

        drainLogHandler(handler, expected = 2)
        assertEquals(listOf(ArcaLogLevel.WARNING, ArcaLogLevel.ERROR), handler.records.map { it.level })
    }

    @Test
    fun debugLevelReceivesAll() = runBlocking {
        val handler = CapturingLogHandler()
        val log = ArcaLogger(minLevel = ArcaLogLevel.DEBUG, handler = handler)

        log.debug("c") { "d" }
        log.info("c") { "i" }
        log.notice("c") { "n" }
        log.warning("c") { "w" }
        log.error("c") { "e" }

        drainLogHandler(handler, expected = 5)
        assertEquals(
            listOf(ArcaLogLevel.DEBUG, ArcaLogLevel.INFO, ArcaLogLevel.NOTICE, ArcaLogLevel.WARNING, ArcaLogLevel.ERROR),
            handler.records.map { it.level },
        )
    }

    @Test
    fun messageClosureNotEvaluatedWhenBelowLevel() {
        val handler = CapturingLogHandler()
        val log = ArcaLogger(minLevel = ArcaLogLevel.WARNING, handler = handler)

        var evaluationCount = 0
        repeat(3) {
            log.debug("c") {
                evaluationCount += 1
                "should not be built"
            }
        }
        assertEquals(0, evaluationCount, "Debug-level message lambdas must not be evaluated when minLevel is WARNING")

        log.warning("c") {
            evaluationCount += 1
            "should be built"
        }
        assertEquals(1, evaluationCount, "Warning-level message lambda must be evaluated when minLevel is WARNING")
    }

    @Test
    fun metadataAndErrorPreserved() = runBlocking {
        val handler = CapturingLogHandler()
        val log = ArcaLogger(minLevel = ArcaLogLevel.DEBUG, handler = handler)
        val err = IllegalStateException("sample")

        log.error("network", error = err, metadata = mapOf("path" to "/objects", "httpMethod" to "GET")) {
            "request failed"
        }

        drainLogHandler(handler, expected = 1)
        assertEquals(1, handler.records.size)
        val record = handler.records[0]
        assertEquals(ArcaLogLevel.ERROR, record.level)
        assertEquals("network", record.category)
        assertEquals("request failed", record.message)
        assertEquals("/objects", record.metadata["path"])
        assertEquals("GET", record.metadata["httpMethod"])
        assertEquals(err, record.error)
    }

    @Test
    fun concurrentEmissionDeliversAllRecords() = runBlocking {
        val handler = CapturingLogHandler()
        val log = ArcaLogger(minLevel = ArcaLogLevel.DEBUG, handler = handler)

        val emitCount = 200
        val jobs = (0 until emitCount).map { i ->
            launch(Dispatchers.Default) {
                log.warning("stress", metadata = mapOf("i" to i.toString())) { "msg" }
            }
        }
        jobs.forEach { it.join() }

        drainLogHandler(handler, expected = emitCount)
        assertEquals(emitCount, handler.records.size, "All concurrent records must be delivered without loss")
        val seen = handler.records.mapNotNull { it.metadata["i"] }.toSet()
        assertEquals(emitCount, seen.size, "Every concurrent index must appear exactly once")
    }

    @Test
    fun disabledLoggerDoesNotCrashWithoutHandler() {
        val log = ArcaLogger.disabled
        log.debug("c") { "ignored" }
        log.warning("c") { "still ignored at min=ERROR" }
        log.error("c") { "goes to JUL but no handler to deliver to" }
    }

    @Test
    fun minLevelCanBeLowered() = runBlocking {
        val handler = CapturingLogHandler()
        val log = ArcaLogger(minLevel = ArcaLogLevel.ERROR, handler = handler)

        log.warning("c") { "dropped" }
        drainLogHandler(handler, expected = 0, timeoutMs = 50)
        assertEquals(0, handler.records.size)

        log.minLevel = ArcaLogLevel.DEBUG
        log.warning("c") { "kept" }
        drainLogHandler(handler, expected = 1)
        assertEquals(1, handler.records.size)
    }
}
