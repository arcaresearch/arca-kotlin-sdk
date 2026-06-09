package network.arca.sdk

import kotlinx.coroutines.delay
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import network.arca.sdk.models.ConnectionStatus
import network.arca.sdk.models.EventType
import okhttp3.OkHttpClient
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.Collections
import kotlinx.coroutines.flow.Flow

/**
 * Ports the behaviorally-meaningful subset of Swift's `WebSocketManagerTests`.
 *
 * The Swift suite also covers `OutboundMessage`/`InboundControlMessage` Codable
 * round-trips and the Swift-only `SendableBox`/`StoppedBox` helpers; those are
 * Swift-runtime concerns with no Kotlin analogue (outbound frames are built with
 * `buildJsonObject`, inbound frames are parsed inline in `handleMessage`). What
 * remains — message normalization, valuation fan-out, malformed-payload
 * hardening, logger instrumentation, and the resume/authenticated lifecycle — is
 * ported here against the real `WebSocketManager` via [WebSocketManager.injectMessage].
 */
class WebSocketManagerTest {

    private val managers = Collections.synchronizedList(mutableListOf<WebSocketManager>())

    @AfterEach
    fun tearDown() {
        synchronized(managers) { managers.toList() }.forEach { runCatching { it.shutdown() } }
        managers.clear()
    }

    private fun makeManager(logger: ArcaLogger = ArcaLogger.disabled): WebSocketManager {
        val manager = WebSocketManager(
            baseUrl = "http://localhost:3052",
            token = "test",
            realmId = "rlm_test",
            httpClient = OkHttpClient(),
            log = logger,
        )
        managers.add(manager)
        return manager
    }

    /**
     * Subscribe to [flow] (the bus has `replay = 0`, so the collector must be
     * live before the message is injected), inject, then drain for [durationMs].
     */
    private suspend fun <T> collect(
        flow: Flow<T>,
        durationMs: Long = 200,
        inject: () -> Unit,
    ): List<T> = coroutineScope {
        val out = Collections.synchronizedList(mutableListOf<T>())
        val job = launch { flow.collect { out.add(it) } }
        delay(60)
        inject()
        delay(durationMs)
        job.cancel()
        synchronized(out) { out.toList() }
    }

    // MARK: - Capabilities + initial state

    @Test
    fun advertisedCapabilitiesIncludeServerPricing() {
        assertTrue(ArcaClient.ADVERTISED_CAPABILITIES.contains("server-authoritative-pricing"))
    }

    @Test
    fun initialStatusIsDisconnected() {
        assertEquals(ConnectionStatus.DISCONNECTED, makeManager().status)
    }

    // MARK: - mids.snapshot normalization

    @Test
    fun midsSnapshotNormalizedToMidsUpdated() = runBlocking {
        val manager = makeManager()
        val received = collect(manager.midsEvents()) {
            manager.injectMessage(
                """{"type":"mids.snapshot","mids":{"hl:0:BTC":"97000.5","hl:0:ETH":"3500.25"}}""",
            )
        }
        assertEquals(1, received.size, "mids.snapshot should flow through midsEvents()")
        assertEquals("97000.5", received.first()["hl:0:BTC"])
        assertEquals("3500.25", received.first()["hl:0:ETH"])
    }

    @Test
    fun midsSnapshotEmptyMapStillDelivered() = runBlocking {
        val manager = makeManager()
        val received = collect(manager.events) {
            manager.injectMessage("""{"type":"mids.snapshot","mids":{}}""")
        }
        assertEquals(1, received.size, "Empty mids.snapshot should still be delivered")
        assertEquals(
            EventType.MIDS_UPDATED.wire,
            received.first().type,
            "mids.snapshot should be rewritten to mids.updated type",
        )
        assertEquals(emptyMap<String, String>(), received.first().mids)
    }

    @Test
    fun midsUpdatedStillPassesThrough() = runBlocking {
        val manager = makeManager()
        val received = collect(manager.midsEvents()) {
            manager.injectMessage("""{"type":"mids.updated","mids":{"hl:0:BTC":"97100"},"deliverySeq":1}""")
        }
        assertEquals(1, received.size, "mids.updated should still pass through midsEvents()")
        assertEquals("97100", received.first()["hl:0:BTC"])
    }

    @Test
    fun exchangeNotificationsDeliverBareExchangeUpdated() = runBlocking {
        val manager = makeManager()
        val received = collect(manager.exchangeNotifications()) {
            manager.injectMessage(
                """{"type":"exchange.updated","entityId":"obj_1","entityPath":"/exchanges/main"}""",
            )
        }
        assertEquals(1, received.size)
        assertEquals(EventType.EXCHANGE_UPDATED.wire, received.first().type)
        assertEquals("obj_1", received.first().entityId)
        assertNull(received.first().exchangeState)
    }

    // MARK: - watch_snapshot normalization

    @Test
    fun watchSnapshotValuationNormalizedToObjectValuation() = runBlocking {
        val manager = makeManager()
        val received = collect(manager.objectValuationEvents()) {
            manager.injectMessage(
                """{"type":"watch_snapshot","path":"/exchanges/strategy-1","watchId":"req_abc123","valuation":{"objectId":"obj_001","path":"/exchanges/strategy-1","type":"exchange","denomination":"USD","valueUsd":"200.00","balances":[{"denomination":"USD","amount":"200.00","valueUsd":"200.00"}]}}""",
            )
        }
        assertEquals(1, received.size, "watch_snapshot should flow through objectValuationEvents()")
        val item = received.first()
        assertEquals("/exchanges/strategy-1", item.path)
        assertEquals("req_abc123", item.watchId)
        assertEquals("200.00", item.valuation.valueUsd)
        assertEquals("obj_001", item.valuation.objectId.value)
        assertEquals("exchange", item.valuation.type)
    }

    @Test
    fun watchSnapshotWithoutValuationDoesNotEmit() = runBlocking {
        val manager = makeManager()
        val received = collect(manager.objectValuationEvents()) {
            manager.injectMessage("""{"type":"watch_snapshot","path":"/wallets/main","watchId":"req_xyz"}""")
        }
        assertTrue(received.isEmpty(), "watch_snapshot without valuation should not emit object.valuation")
    }

    @Test
    fun watchSnapshotMultiObjectValuationsEmitPerPath() = runBlocking {
        val manager = makeManager()
        val received = collect(manager.objectValuationEvents(), durationMs = 250) {
            manager.injectMessage(
                """{"type":"watch_snapshot","path":"/","watchId":"req_multi","valuations":{"/exchanges/s1":{"objectId":"obj_1","path":"/exchanges/s1","type":"exchange","denomination":"USD","valueUsd":"500.00","balances":[{"denomination":"USD","amount":"500.00","valueUsd":"500.00"}]},"/exchanges/s2":{"objectId":"obj_2","path":"/exchanges/s2","type":"exchange","denomination":"USD","valueUsd":"300.00","balances":[{"denomination":"USD","amount":"300.00","valueUsd":"300.00"}]}}}""",
            )
        }
        assertEquals(2, received.size, "should emit one object.valuation per entry in valuations map")
        val paths = received.map { it.path }.toSet()
        assertTrue(paths.contains("/exchanges/s1"))
        assertTrue(paths.contains("/exchanges/s2"))
        assertTrue(received.all { it.watchId == "req_multi" })
    }

    // MARK: - Logger instrumentation

    @Test
    fun serverErrorMessageEmitsErrorLogRecord() = runBlocking {
        val handler = CapturingLogHandler()
        val manager = makeManager(ArcaLogger(ArcaLogLevel.DEBUG, handler))

        manager.injectMessage("""{"type":"error","message":"Invalid realm"}""")

        val records = awaitRecords(handler) { recs ->
            recs.any { it.level == ArcaLogLevel.ERROR && it.category == "websocket" }
        }
        val errorRecords = records.filter { it.level == ArcaLogLevel.ERROR && it.category == "websocket" }
        assertFalse(errorRecords.isEmpty(), "Server error message should emit an error-level websocket record")
        assertEquals("Invalid realm", errorRecords.first().metadata["message"])
    }

    @Test
    fun deliveryGapEmitsWarningLogRecord() = runBlocking {
        val handler = CapturingLogHandler()
        val manager = makeManager(ArcaLogger(ArcaLogLevel.DEBUG, handler))

        manager.injectMessage("""{"type":"mids.updated","mids":{"hl:0:BTC":"1"},"deliverySeq":1}""")
        manager.injectMessage("""{"type":"mids.updated","mids":{"hl:0:BTC":"2"},"deliverySeq":5}""")

        val records = awaitRecords(handler) { recs -> recs.any { it.message.contains("delivery gap") } }
        val gapRecords = records.filter { it.message.contains("delivery gap") }
        assertFalse(gapRecords.isEmpty(), "Delivery gap should emit a warning record")
        assertEquals(ArcaLogLevel.WARNING, gapRecords.first().level)
        assertEquals("3", gapRecords.first().metadata["missed"])
    }

    // MARK: - Resume / Authenticated lifecycle

    @Test
    fun triggerResumeFiresOnResumeHandlersWithDuration() {
        val manager = makeManager()
        val captured = Collections.synchronizedList(mutableListOf<Double>())
        manager.onResume { captured.add(it) }

        manager.triggerResume(30.0)

        assertEquals(listOf(30.0), captured.toList())
    }

    @Test
    fun removeResumeHandlerStopsFurtherCalls() {
        val manager = makeManager()
        val captured = Collections.synchronizedList(mutableListOf<Double>())
        val id = manager.onResume { captured.add(it) }

        manager.triggerResume(10.0)
        manager.removeResumeHandler(id)
        manager.triggerResume(20.0)

        assertEquals(listOf(10.0), captured.toList())
    }

    @Test
    fun authenticatedHandlerFiresOnEveryAuthMessage() = runBlocking {
        val manager = makeManager()
        val count = java.util.concurrent.atomic.AtomicInteger(0)
        manager.onAuthenticated { count.incrementAndGet() }

        // Two synthetic re-auths (e.g. token refresh path) — both fire.
        manager.injectMessage("""{"type":"authenticated","message":null}""")
        manager.injectMessage("""{"type":"authenticated","message":null}""")

        assertEquals(2, count.get())
    }

    @Test
    fun authenticatedHandlerDoesNotFireForAuthBeforeRegistration() = runBlocking {
        val manager = makeManager()

        manager.injectMessage("""{"type":"authenticated","message":null}""")

        val count = java.util.concurrent.atomic.AtomicInteger(0)
        manager.onAuthenticated { count.incrementAndGet() }
        delay(50)
        assertEquals(0, count.get(), "Registering after auth should not retroactively fire")
    }

    @Test
    fun removeAuthenticatedHandlerStopsFurtherCalls() = runBlocking {
        val manager = makeManager()
        val count = java.util.concurrent.atomic.AtomicInteger(0)
        val id = manager.onAuthenticated { count.incrementAndGet() }

        manager.injectMessage("""{"type":"authenticated","message":null}""")
        manager.removeAuthenticatedHandler(id)
        manager.injectMessage("""{"type":"authenticated","message":null}""")

        assertEquals(1, count.get())
    }

    // MARK: - Malformed payload hardening

    @Test
    fun candlesUpdatedFragmentCandleIsSkippedWithoutCrashing() = runBlocking {
        val manager = makeManager()
        // `candle` is a bare number (fragment) — the exact off-the-wire shape
        // that triggered the production crash in the Swift SDK.
        val received = collect(manager.events) {
            manager.injectMessage(
                """{"type":"candles.updated","candles":[{"market":"hl:0:BTC","interval":"1m","candle":0}]}""",
            )
        }
        val candleEvents = received.filter { it.type == EventType.CANDLE_UPDATED.wire }
        assertTrue(candleEvents.isEmpty(), "A fragment `candle` value must be skipped, emitting no candle event")
    }

    @Test
    fun candlesUpdatedValidCandleStillEmits() = runBlocking {
        val manager = makeManager()
        val received = collect(manager.candleEvents()) {
            manager.injectMessage(
                """{"type":"candles.updated","candles":[{"market":"hl:0:BTC","interval":"1m","candle":{"t":1,"o":"100","h":"110","l":"90","c":"105","v":"12","n":3}}]}""",
            )
        }
        assertEquals(1, received.size, "A valid candle should emit exactly one candle event")
        assertEquals("hl:0:BTC", received.first().market)
        assertEquals("1m", received.first().interval.wire)
        assertEquals("105", received.first().candle.c)
    }

    @Test
    fun watchSnapshotFragmentValuationIsSkippedWithoutCrashing() = runBlocking {
        val manager = makeManager()
        // Both the single `valuation` and a `valuations` map entry are fragments.
        val received = collect(manager.events) {
            manager.injectMessage(
                """{"type":"watch_snapshot","watchId":"w1","path":"/a","valuation":42,"valuations":{"/b":"oops"}}""",
            )
        }
        val valuationEvents = received.filter { it.type == EventType.OBJECT_VALUATION.wire }
        assertTrue(valuationEvents.isEmpty(), "Fragment valuation values must be skipped, emitting no valuation event")
    }

    // MARK: - Helpers

    /** Poll the handler's captured records until [predicate] holds or the deadline passes. */
    private suspend fun awaitRecords(
        handler: CapturingLogHandler,
        timeoutMs: Long = 500,
        predicate: (List<ArcaLogRecord>) -> Boolean,
    ): List<ArcaLogRecord> {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            val recs = handler.records
            if (predicate(recs)) return recs
            delay(5)
        }
        return handler.records
    }

    /** Thread-safe [ArcaLogHandler] that accumulates every record for assertions. */
    private class CapturingLogHandler : ArcaLogHandler {
        private val store = Collections.synchronizedList(mutableListOf<ArcaLogRecord>())
        override fun handle(record: ArcaLogRecord) {
            store.add(record)
        }

        val records: List<ArcaLogRecord> get() = synchronized(store) { store.toList() }
    }
}
