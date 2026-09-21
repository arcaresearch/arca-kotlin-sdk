package network.arca.sdk

import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import network.arca.sdk.internal.SseBackoff
import network.arca.sdk.internal.SseFrame
import network.arca.sdk.internal.SseParser
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.AutoDepositState
import network.arca.sdk.models.WalletAccount
import network.arca.sdk.models.WalletFailureReason
import network.arca.sdk.models.WalletOperationState
import network.arca.sdk.models.WalletState
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * The Wallet Account read model and its stream (wallet-money-movement step
 * 08): every fixture the platform generates from its composition function
 * decodes and round-trips, the enums are byte-for-byte the shipped
 * vocabulary, and the SSE client parses frames, resumes with `Last-Event-ID`
 * on the 1 s → 30 s schedule, refreshes once on 401 and surfaces a refusal as
 * the typed exception.
 */
class WalletAccountTest {

    private lateinit var server: MockWebServer

    @BeforeEach
    fun setUp() {
        server = MockWebServer()
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    // MARK: - Fixtures

    /** Reached from the Gradle project directory inside the monorepo; skipped standalone. */
    private val fixturesDirectory = File("../../backend/libs/arca-go/cashv9/testdata/wallet-account")

    /** JSON with `null`s removed, so Go's `omitempty` and Kotlin's nulls compare equal. */
    private fun normalized(element: JsonElement): JsonElement? = when (element) {
        is JsonNull -> null
        is JsonObject -> JsonObject(element.mapNotNull { (k, v) -> normalized(v)?.let { k to it } }.toMap())
        is JsonArray -> JsonArray(element.mapNotNull { normalized(it) })
        else -> element
    }

    @Test
    fun everyFixtureDecodesAndRoundTripsAndEnumsMatchTheVocabulary() {
        assumeTrue(fixturesDirectory.isDirectory, "fixtures not present at ${fixturesDirectory.absolutePath}")
        val vocabulary = Json.parseToJsonElement(File(fixturesDirectory, "vocabulary.json").readText()).jsonObject
        fun set(key: String) = vocabulary.getValue(key).jsonArray.map { it.jsonPrimitive.content }
        assertEquals(1, vocabulary.getValue("schema").jsonPrimitive.content.toInt())
        // The enums ARE the vocabulary, in order.
        assertEquals(set("walletStates"), WalletState.entries.map { it.wire })
        assertEquals(set("operationStates"), WalletOperationState.entries.map { it.wire })
        assertEquals(set("reasons"), WalletFailureReason.entries.map { it.wire })
        assertEquals(set("autoDepositStates"), AutoDepositState.entries.map { it.wire })
        val attention = set("attention")
        val kinds = set("operationKinds")

        val files = fixturesDirectory.listFiles { f -> f.name.endsWith(".json") && f.name != "vocabulary.json" }!!.sortedBy { it.name }
        assertTrue(files.size >= 19, "fixture set shrank: ${files.size}")
        val statesSeen = mutableSetOf<String>()
        val autoDepositSeen = mutableSetOf<String>()
        for (file in files) {
            val raw = file.readText()
            val account = try {
                arcaJson.decodeFromString(WalletAccount.serializer(), raw)
            } catch (e: Exception) {
                fail<WalletAccount>("${file.name}: $e")
            }
            assertEquals(1, account.schema, file.name)
            assertNotNull(account.typedWalletState, "${file.name}: walletState ${account.walletState} unknown to the SDK")
            statesSeen += account.walletState
            account.attention.forEach { assertTrue(it in attention, "${file.name}: attention $it") }
            for (op in account.operations) {
                assertNotNull(op.typedState, "${file.name}: operation state ${op.state}")
                assertTrue(op.kind in kinds, "${file.name}: kind ${op.kind}")
                op.reason?.let { reason ->
                    assertNotNull(WalletFailureReason.fromWire(reason), "${file.name}: reason $reason")
                    assertEquals(WalletOperationState.FAILED.wire, op.state, "${file.name}: reason only when failed")
                }
                assertNotNull(op.amountMicro.toLongOrNull(), "${file.name}: amountMicro ${op.amountMicro}")
            }
            account.autoDeposit?.let { auto ->
                assertNotNull(auto.typedState, "${file.name}: autoDeposit state ${auto.state}")
                autoDepositSeen += auto.state
                if (auto.state == AutoDepositState.OFF.wire) assertNull(auto.routeId, file.name) else assertNotNull(auto.routeId, file.name)
            }
            listOf(account.balances.confirmedMicro, account.balances.availableMicro, account.balances.reservedMicro, account.balances.pendingInMicro, account.balances.pendingOutMicro)
                .forEach { assertNotNull(it.toLongOrNull(), "${file.name}: balance $it") }
            // Round trip: nothing lost or renamed.
            val again = arcaJson.encodeToJsonElement(WalletAccount.serializer(), account)
            assertEquals(normalized(Json.parseToJsonElement(raw)), normalized(again), "${file.name}: SDK type does not round-trip the fixture")
        }
        assertEquals(set("walletStates").toSet(), statesSeen, "fixtures cover every walletState")
        assertEquals(set("autoDepositStates").toSet(), autoDepositSeen, "fixtures cover every autoDeposit state")
    }

    // MARK: - SSE parsing and backoff

    @Test
    fun parserJoinsDataIgnoresCommentsAndDropsEmptyBlocks() {
        val parser = SseParser()
        assertNull(parser.consume(": connected"))
        assertNull(parser.consume(""), "a comment-only block is not a frame")
        assertNull(parser.consume("id: 42"))
        assertNull(parser.consume("event: snapshot"))
        assertNull(parser.consume("data: {\"a\":"))
        assertNull(parser.consume("data:1}"))
        assertEquals(SseFrame("42", "snapshot", "{\"a\":\n1}"), parser.consume(""))
        assertNull(parser.consume("id: 43"))
        assertNull(parser.consume(""), "id without data is not a frame")
        assertNull(parser.consume("data: x"))
        assertEquals(SseFrame(null, null, "x"), parser.consume(""), "state resets between blocks")

        val frames = ArrayList<SseFrame>()
        for (b in "id: 1\r\nevent: snapshot\r\ndata: a\r\n\r\n: heartbeat\n\ndata: b\n\n".toByteArray()) {
            parser.consume(b.toInt() and 0xff)?.let(frames::add)
        }
        assertEquals(listOf(SseFrame("1", "snapshot", "a"), SseFrame(null, null, "b")), frames)
    }

    @Test
    fun backoffIsOneToThirtySecondsDoubling() {
        assertEquals(listOf(1000L, 2000L, 4000L, 8000L, 16000L, 30000L, 30000L, 30000L), listOf(0, 1, 2, 3, 4, 5, 6, 9).map(SseBackoff::delayMillis))
    }

    // MARK: - Stream behaviour

    private fun snapshot(revision: Long, state: String): String =
        "id: $revision\nevent: snapshot\ndata: {\"schema\":1,\"revision\":$revision,\"realmId\":\"rlm_1\",\"boundaryId\":\"bnd_1\",\"ownerAddress\":\"0x4ae85840bdb73e220d646eb0c564eee18a790197\",\"source\":null,\"walletState\":\"$state\",\"attention\":[],\"balances\":{\"confirmedMicro\":\"3600000\",\"availableMicro\":\"3100000\",\"reservedMicro\":\"500000\",\"pendingInMicro\":\"0\",\"pendingOutMicro\":\"500000\",\"asOf\":{\"block\":46252318}},\"autoDeposit\":null,\"operations\":[]}\n\n"

    private fun sse(vararg frames: String): MockResponse = MockResponse()
        .setHeader("Content-Type", "text/event-stream")
        .setBody(frames.joinToString(""))

    private fun client(onUnauthorized: (suspend (AuthRefreshTrigger) -> String)? = null): ArcaClient =
        ArcaClient("fixture-token", server.url("/").toString(), OkHttpClient(), onUnauthorized)

    @Test
    fun streamParsesSnapshotsAndResumesWithLastEventIdAfterBackoff() = runBlocking {
        server.enqueue(sse(": connected\n\n", snapshot(41, "ready"), ": heartbeat\n\n", "event: control\ndata: {\"ignored\":true}\n\n", snapshot(42, "needs_attention")))
        server.enqueue(sse(snapshot(42, "needs_attention")))
        server.enqueue(sse(snapshot(43, "ready")))
        val runner = WalletAccountStreamRunner(client(), "rlm_1", "bnd_1", ArcaLogger.disabled)
        val started = System.nanoTime()
        val revisions = withTimeout(15_000) {
            kotlinx.coroutines.flow.channelFlow { runner.run(this) }.take(4).toList().map { it.revision }
        }
        assertEquals(listOf(41L, 42L, 42L, 43L), revisions, "one snapshot per connect after resume")
        val requests = (1..3).map { server.takeRequest(1, TimeUnit.SECONDS)!! }
        assertEquals(listOf(null, "42", "42"), requests.map { it.getHeader("Last-Event-ID") }, "Last-Event-ID carries the last delivered revision")
        requests.forEach {
            assertEquals("Bearer fixture-token", it.getHeader("Authorization"))
            assertEquals("text/event-stream", it.getHeader("Accept"))
            assertTrue(it.path!!.startsWith("/api/v1/custody/v9/cash/wallet-account/events?"), it.path!!)
            assertTrue(it.path!!.contains("realmId=rlm_1") && it.path!!.contains("boundaryId=bnd_1"), it.path!!)
        }
        // Two reconnects, each after the 1 s first step (a delivering
        // connection resets the schedule), so at least ~2 s elapsed.
        val elapsedMillis = (System.nanoTime() - started) / 1_000_000
        assertTrue(elapsedMillis >= 1900, "reconnected after ${elapsedMillis}ms; the first backoff step is 1 s")
    }

    @Test
    fun refusedConnectionThrowsTheTypedException() = runBlocking {
        server.enqueue(
            MockResponse().setResponseCode(404).setHeader("Content-Type", "application/json")
                .setBody("""{"success":false,"error":{"code":"NOT_FOUND","message":"V9 cash resource not found"}}"""),
        )
        val runner = WalletAccountStreamRunner(client(), "rlm_1", "bnd_missing", ArcaLogger.disabled)
        try {
            withTimeout(5_000) { kotlinx.coroutines.flow.channelFlow { runner.run(this) }.toList() }
            fail<Unit>("stream ended without throwing")
        } catch (e: ArcaException.NotFound) {
            assertEquals("NOT_FOUND", e.code)
        }
        assertEquals(1, server.requestCount, "a refusal is not retried")
    }

    @Test
    fun unauthorizedRefreshesOnceAndReconnects() = runBlocking {
        server.enqueue(
            MockResponse().setResponseCode(401).setHeader("Content-Type", "application/json")
                .setBody("""{"success":false,"error":{"code":"UNAUTHORIZED","message":"expired"}}"""),
        )
        server.enqueue(sse(snapshot(7, "ready")))
        val refreshes = AtomicInteger()
        val runner = WalletAccountStreamRunner(client { refreshes.incrementAndGet(); "fresh-token" }, "rlm_1", "bnd_1", ArcaLogger.disabled)
        val first = withTimeout(5_000) { kotlinx.coroutines.flow.channelFlow { runner.run(this) }.take(1).toList().single() }
        assertEquals(7L, first.revision)
        assertEquals(1, refreshes.get())
        assertEquals(listOf("Bearer fixture-token", "Bearer fresh-token"), (1..2).map { server.takeRequest(1, TimeUnit.SECONDS)!!.getHeader("Authorization") })
    }
}
