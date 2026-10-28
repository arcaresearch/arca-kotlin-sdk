package network.arca.sdk

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.Micro
import network.arca.sdk.models.MoneyChange
import network.arca.sdk.models.MoneyCounterparty
import network.arca.sdk.models.MoneyDiagnostic
import network.arca.sdk.models.MoneyDirection
import network.arca.sdk.models.MoneyLegStage
import network.arca.sdk.models.MoneyPrediction
import network.arca.sdk.models.MoneyRevaluation
import network.arca.sdk.models.WALLET_SNAPSHOT_SCHEMA
import network.arca.sdk.models.WalletEndpoint
import network.arca.sdk.models.WalletMoney
import network.arca.sdk.models.WalletSnapshot
import network.arca.sdk.models.WalletState
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.File
import java.time.Instant
import java.util.concurrent.TimeUnit

/**
 * Money figures (documents/contracts/sdk-money-figure.md): the schema-2
 * wallet snapshot decodes, every figure is Arca's figure verbatim, legs
 * render on the ends the frame lists, a prediction only adds legs, a venue
 * frame never outranks a booked settlement, and the per-wallet stream
 * resumes with `Last-Event-ID`. The fixture sequence is shared with the
 * Swift, Go and TypeScript SDKs.
 */
class WalletMoneyTest {

    @Test fun personSendDoesNotArriveInSendersCash() {
        var previous: WalletMoney? = null
        for (name in listOf("19-person-send-reserved", "20-person-send-in-transit", "21-person-send-paid")) {
            val money = WalletMoney.compose(fixture(name), previous = previous)
            assertTrue(money.cash.arriving.isEmpty() && money.cash.departing.isEmpty(), name)
            assertNull(money.cash.change, "A recipient credit never changes sender Cash")
            if (name != "21-person-send-paid") {
                assertEquals(1, money.total.departing.size, name)
                assertEquals(1, money.account(hl1)?.departing?.size, name)
                assertEquals(MoneyCounterparty.External("0xabcabcabcabcabcabcabcabcabcabcabcabcabca"), money.total.departing.first().counterparty)
            } else {
                assertTrue(money.total.departing.isEmpty())
                assertEquals("succeeded", money.total.change?.operationStatus)
            }
            previous = money
        }
    }

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

    /** Reached from the Gradle project directory inside the monorepo; skipped standalone. */
    private val fixturesDirectory = File("../../backend/libs/arca-go/fundingv9/testdata/wallet-money")

    private fun fixture(name: String): WalletSnapshot {
        val file = File(fixturesDirectory, "$name.json")
        assumeTrue(file.isFile, "fixtures not present at ${file.absolutePath}")
        return arcaJson.decodeFromString(WalletSnapshot.serializer(), file.readText())
    }

    private val hl1 = "obj_hl1"
    private val hl2 = "obj_hl2"

    @Test
    fun everyFrameDecodesAsSchemaTwoWithItsFigures() {
        assumeTrue(fixturesDirectory.isDirectory, "fixtures not present at ${fixturesDirectory.absolutePath}")
        val files = fixturesDirectory.listFiles { f -> f.name.endsWith(".json") }!!.sortedBy { it.name }
        assertTrue(files.size >= 19, "fixture set shrank: ${files.size}")
        for (file in files) {
            val snapshot = try {
                arcaJson.decodeFromString(WalletSnapshot.serializer(), file.readText())
            } catch (e: Exception) {
                fail<WalletSnapshot>("${file.name}: $e")
            }
            assertEquals(WALLET_SNAPSHOT_SCHEMA, snapshot.schema, file.name)
            assertNotNull(snapshot.totals.totalMicro.toLongOrNull(), "${file.name}: totalMicro")
            snapshot.moving.forEach { assertNotNull(it.typedLeg, "${file.name}: leg ${it.leg}") }
            snapshot.accounts.forEach { account ->
                assertNotNull(account.typedState, "${file.name}: state ${account.state}")
                account.balances?.let { assertNotNull(it.typedSource, "${file.name}: source ${it.source}") }
            }
            val money = WalletMoney.compose(snapshot)
            assertEquals(snapshot.watermark, money.watermark, file.name)
            assertEquals(snapshot.accounts.map { it.arcaId }, money.accountOrder, "${file.name}: accounts keep the snapshot's order")
        }
    }

    @Test
    fun figuresAreArcasFiguresVerbatim() {
        assumeTrue(fixturesDirectory.isDirectory)
        for (file in fixturesDirectory.listFiles { f -> f.name.endsWith(".json") }!!.sortedBy { it.name }) {
            val snapshot = arcaJson.decodeFromString(WalletSnapshot.serializer(), file.readText())
            val money = WalletMoney.compose(snapshot)
            assertEquals(snapshot.totals.totalMicro, money.total.settledMicro, "${file.name}: the total is totals.totalMicro")
            assertNull(money.total.availableMicro, file.name)
            assertEquals(snapshot.cash.balances.confirmedMicro, money.cash.settledMicro, "${file.name}: Cash is confirmedMicro")
            assertEquals(snapshot.cash.balances.availableMicro, money.cash.availableMicro, file.name)
            for (account in snapshot.accounts) {
                val figure = money.account(account.arcaId) ?: fail("${file.name}: ${account.arcaId}")
                assertEquals(account.balances?.totalMicro, figure.settledMicro, "${file.name}: ${account.arcaId} is balances.totalMicro")
                assertEquals(account.balances?.withdrawableMicro, figure.availableMicro, file.name)
                assertEquals(account.current, figure.current, file.name)
                assertEquals(account.asOf, figure.asOf, file.name)
            }
            assertEquals(snapshot.totals.complete, money.complete, file.name)
            assertTrue(money.diagnostics.isEmpty(), "${file.name}: ${money.diagnostics}")
        }
    }

    @Test
    fun everyDollarVisibleOnceAtEveryStage() {
        val rest = WalletMoney.compose(fixture("00-rest"))
        assertFalse(rest.cash.inMotion)
        assertEquals("31010000", rest.total.settledMicro)

        val reserved = WalletMoney.compose(fixture("01-move-reserved"), rest)
        assertEquals("28510000", reserved.cash.settledMicro)
        assertEquals("23500000", reserved.cash.availableMicro, "Arca's spendable figure already excludes the reservation")
        val leaving = reserved.cash.departing.single()
        assertEquals("5000000", leaving.amountMicro)
        assertEquals(MoneyLegStage.RESERVED, leaving.stage)
        assertEquals(MoneyCounterparty.Account(hl1, "Hyperliquid 1"), leaving.counterparty)
        assertEquals("req-move1", leaving.requestId)
        val arriving = reserved.account(hl1)!!.arriving.single()
        assertEquals(leaving.operationId, arriving.operationId, "one operation shown at both ends")
        assertEquals(MoneyCounterparty.Cash, arriving.counterparty)
        assertEquals("1500000", reserved.account(hl1)!!.settledMicro)
        assertTrue(reserved.total.arriving.isEmpty() && reserved.total.departing.isEmpty(), "an internal move never reaches the total")
        assertEquals("31010000", reserved.total.settledMicro)
        assertEquals(MoneyChange.Cause.Operation("opr_move1", "cash_to_trading"), reserved.cash.change?.cause)

        val inTransit = WalletMoney.compose(fixture("02-move-in-transit"), reserved)
        assertEquals("23500000", inTransit.cash.settledMicro)
        assertTrue(inTransit.cash.departing.isEmpty(), "a leg in transit is no longer the source's")
        assertEquals(MoneyLegStage.IN_TRANSIT, inTransit.account(hl1)!!.arriving.single().stage)
        assertEquals("5000000", inTransit.account(hl1)!!.arrivingMicro)
        assertEquals("31000000", inTransit.total.settledMicro, "the network fee is the only change to the total")
        assertEquals("-5010000", inTransit.cash.change?.deltaMicro)

        val completed = WalletMoney.compose(fixture("03-move-completed"), inTransit)
        assertEquals("6500000", completed.account(hl1)!!.settledMicro)
        assertEquals("arca_settlement", completed.account(hl1)!!.source)
        assertTrue(completed.account(hl1)!!.arriving.isEmpty())
        assertEquals(MoneyChange.Cause.Operation("opr_move1", "cash_to_trading"), completed.account(hl1)!!.change?.cause)
        assertEquals("5000000", completed.account(hl1)!!.change?.deltaMicro)

        val fill = WalletMoney.compose(fixture("04-venue-fill"), completed)
        assertEquals("6420000", fill.account(hl1)!!.settledMicro)
        assertEquals(MoneyChange.Cause.Venue, fill.account(hl1)!!.change?.cause)
        assertEquals("-80000", fill.account(hl1)!!.change?.deltaMicro)
        assertNull(fill.cash.change, "Cash did not move")
        assertEquals("30920000", fill.total.settledMicro)

        val failed = WalletMoney.compose(fixture("05-move-failed-before-pnr"), reserved)
        assertEquals("28510000", failed.cash.availableMicro)
        assertTrue(failed.cash.departing.isEmpty() && failed.account(hl1)!!.arriving.isEmpty())
        assertEquals("failed", failed.cash.change?.operationStatus, "a released reservation is not money sent")
        assertEquals(MoneyDirection.OUTGOING, failed.cash.change?.retired?.direction)

        val compensated = WalletMoney.compose(fixture("06-move-compensated"), inTransit)
        assertEquals("28500000", compensated.cash.settledMicro)
        assertTrue(compensated.account(hl1)!!.arriving.isEmpty())
        assertEquals("5000000", compensated.cash.change?.deltaMicro)
    }

    @Test
    fun onlyBoundaryCrossingLegsReachTheTotal() {
        val deposit = WalletMoney.compose(fixture("08-deposit-in-transit"))
        assertEquals(listOf("2000000"), deposit.total.arriving.map { it.amountMicro })
        assertEquals(MoneyCounterparty.External("0x3333333333333333333333333333333333333333"), deposit.total.arriving.single().counterparty)
        val send = WalletMoney.compose(fixture("11-send-in-transit"))
        assertEquals(listOf("1000000"), send.total.departing.map { it.amountMicro })
        val cash = WalletMoney.compose(fixture("14-cash-send-and-pending-in"))
        assertEquals(listOf("2000000:reserved"), cash.cash.departing.map { "${it.amountMicro}:${it.stage.wire}" })
        assertEquals(listOf("750000"), cash.cash.arriving.map { it.amountMicro })
        assertEquals(listOf("2000000"), cash.total.departing.map { it.amountMicro })
        assertEquals(listOf("750000"), cash.total.arriving.map { it.amountMicro })
    }

    @Test
    fun expectedIsListedNotSummed() {
        val money = WalletMoney.compose(fixture("07-deposit-expected"))
        val figure = money.account(hl2)!!
        assertEquals(MoneyLegStage.EXPECTED, figure.arriving.single().stage)
        assertEquals("2000000", figure.expectedMicro)
        assertEquals("0", figure.arrivingMicro, "expected money is listed, never counted")
        assertFalse(figure.inMotion)
        assertEquals("30920000", money.total.settledMicro)
    }

    @Test
    fun cashDepositIsExpectedThenCountedOnceAsPendingIn() {
        val expected = WalletMoney.compose(fixture("17-cash-deposit-expected"))
        val leg = expected.cash.arriving.single()
        assertEquals(MoneyLegStage.EXPECTED, leg.stage)
        assertEquals(MoneyCounterparty.External(null), leg.counterparty)
        assertEquals("3000000", expected.cash.expectedMicro)
        assertEquals("0", expected.cash.arrivingMicro, "expected money is listed, never counted")
        assertFalse(expected.cash.inMotion)
        assertEquals("30920000", expected.total.settledMicro)
        val onChain = WalletMoney.compose(fixture("18-cash-deposit-pending-in"), expected)
        assertEquals(listOf("3000000:in_transit"), onChain.cash.arriving.map { "${it.amountMicro}:${it.stage.wire}" }, "on chain, the deposit is Cash's pending figure alone")
        assertEquals("0", onChain.cash.expectedMicro)
        assertEquals(listOf("3000000"), onChain.total.arriving.map { it.amountMicro })
        assertEquals("30920000", onChain.total.settledMicro)
    }

    @Test
    fun externalDepositJoinsTheTotalWhenObservedAndLandsOnce() {
        val expected = WalletMoney.compose(fixture("07-deposit-expected"))
        val observed = WalletMoney.compose(fixture("08-deposit-in-transit"), expected)
        val landed = WalletMoney.compose(fixture("09-deposit-landed"), observed)
        assertEquals("30920000", expected.total.settledMicro)
        assertEquals("32920000", observed.total.settledMicro, "observed on chain: it joins the total as moving")
        assertEquals("1000000", observed.account(hl2)!!.settledMicro)
        assertEquals("32920000", landed.total.settledMicro, "landing moves it from moving to the account")
        assertEquals("3000000", landed.account(hl2)!!.settledMicro)
        assertTrue(landed.account(hl2)!!.arriving.isEmpty())
        assertEquals(MoneyChange.Cause.Operation("opr_dep1", "deposit"), landed.account(hl2)!!.change?.cause)
        assertEquals("0", landed.total.change?.deltaMicro, "the total was counted as moving already")
        assertEquals("2000000", landed.total.change?.retired?.amountMicro)
        assertEquals("succeeded", landed.total.change?.operationStatus)
    }

    @Test
    fun sendLeavesTheTotalOnlyWhenPaid() {
        val reserved = WalletMoney.compose(fixture("10-send-reserved"))
        val inTransit = WalletMoney.compose(fixture("11-send-in-transit"), reserved)
        val paid = WalletMoney.compose(fixture("12-send-paid"), inTransit)
        assertEquals("32920000", reserved.total.settledMicro)
        assertEquals(MoneyLegStage.RESERVED, reserved.account(hl1)!!.departing.single().stage)
        assertEquals("32920000", inTransit.total.settledMicro, "in transit, the money still belongs to the wallet")
        assertEquals(MoneyLegStage.IN_TRANSIT, inTransit.account(hl1)!!.departing.single().stage, "a send keeps its source line until paid")
        assertEquals("31920000", paid.total.settledMicro)
        assertTrue(paid.account(hl1)!!.departing.isEmpty() && paid.total.departing.isEmpty())
    }

    @Test
    fun legRendersOnTheVisibleEndAlone() {
        val money = WalletMoney.compose(fixture("13-off-page-counterparty"))
        val figure = money.account(hl1)!!
        assertEquals(MoneyCounterparty.Unknown("obj_hl9"), figure.departing.single().counterparty)
        assertNull(money.account("obj_hl9"), "the other end is not in this frame")
        assertTrue(money.total.arriving.isEmpty() && money.total.departing.isEmpty())
        assertTrue(money.diagnostics.isEmpty())
    }

    @Test
    fun partialCatalogNeverChangesTheTotal() {
        val unobserved = fixture("16-account-unobserved")
        val money = WalletMoney.compose(unobserved)
        assertNull(money.account(hl2)!!.settledMicro, "an account never observed has no figure, not zero")
        assertFalse(money.complete)
        assertEquals(unobserved.totals.totalMicro, money.total.settledMicro)
        assertEquals(WalletState.SETUP_REQUIRED, WalletMoney.compose(fixture("15-pre-cash-wallet")).snapshot.cash.typedWalletState)
    }

    @Test
    fun predictionAddsLegsOnlyAndNeverTouchesSettled() {
        val rest = fixture("00-rest")
        val prediction = MoneyPrediction("req-move1", null, "cash_to_trading", "5000000", WalletEndpoint(kind = "cash", boundaryId = rest.boundaryId), WalletEndpoint(kind = "hyperliquid", arcaId = hl1))
        val money = WalletMoney.compose(rest, predictions = listOf(prediction))
        assertEquals("28510000", money.cash.availableMicro, "a prediction never changes a figure")
        assertEquals(listOf(true), money.cash.departing.map { it.predicted })
        assertEquals(MoneyCounterparty.Account(hl1, "Hyperliquid 1"), money.cash.departing.single().counterparty)
        assertEquals(listOf(true), money.account(hl1)!!.arriving.map { it.predicted })
        assertTrue(money.total.departing.isEmpty())
        val framed = WalletMoney.compose(fixture("01-move-reserved"), predictions = listOf(prediction))
        assertEquals(listOf(false), framed.cash.departing.map { it.predicted }, "the framed leg replaces the predicted one")
    }

    @Test
    fun bookedSettlementOutranksStaleVenueFrame() {
        val booked = fixture("03-move-completed")
        val stale = MoneyRevaluation(hl1, "1500000", Instant.parse("2026-10-17T18:00:05Z"))
        val money = WalletMoney.compose(booked, revaluations = mapOf(hl1 to stale))
        assertEquals("6500000", money.account(hl1)!!.settledMicro)
        assertNull(money.account(hl1)!!.revaluedMicro, "a venue frame older than the booked figure is not used")
        assertEquals(listOf(MoneyDiagnostic.VenueFrameIgnored(hl1, "venue frame older than the figure")), money.diagnostics)
        val fresh = MoneyRevaluation(hl1, "6490000", Instant.parse("2026-10-17T18:00:11Z"))
        val revalued = WalletMoney.compose(booked, revaluations = mapOf(hl1 to fresh))
        assertEquals("6490000", revalued.account(hl1)!!.revaluedMicro)
        assertEquals("6500000", revalued.account(hl1)!!.settledMicro, "the figure itself never moves with a venue frame")
    }

    @Test
    fun revaluationWaitsForAPendingSettlement() {
        val money = WalletMoney.compose(fixture("02-move-in-transit"), revaluations = mapOf(hl1 to MoneyRevaluation(hl1, "6500000", Instant.parse("2026-10-17T18:00:30Z"))))
        assertNull(money.account(hl1)!!.revaluedMicro)
        assertEquals(1, money.diagnostics.size)
    }

    @Test
    fun fillIsCountedOnceAcrossSnapshotAndVenueFrames() {
        val completed = WalletMoney.compose(fixture("03-move-completed"))
        val venue = MoneyRevaluation(hl1, "6420000", Instant.parse("2026-10-17T18:00:11Z"))
        val ticking = WalletMoney.compose(completed.snapshot, completed, revaluations = mapOf(hl1 to venue))
        assertEquals("6420000", ticking.account(hl1)!!.revaluedMicro)
        assertEquals("6500000", ticking.account(hl1)!!.settledMicro)
        val folded = WalletMoney.compose(fixture("04-venue-fill"), ticking, revaluations = mapOf(hl1 to venue))
        assertEquals("6420000", folded.account(hl1)!!.settledMicro)
        assertNull(folded.account(hl1)!!.revaluedMicro)
        assertEquals("-80000", folded.account(hl1)!!.change?.deltaMicro, "one fill, one change")
    }

    @Test
    fun ambiguousCauseRollsDigitsWithoutACaption() {
        val landed = WalletMoney.compose(fixture("09-deposit-landed"))
        val raw = Json.parseToJsonElement(File(fixturesDirectory, "09-deposit-landed.json").readText()).jsonObject.toMutableMap()
        val accounts = raw.getValue("accounts").jsonArray.toMutableList()
        val second = accounts[1].jsonObject.toMutableMap()
        second["balances"] = JsonObject(second.getValue("balances").jsonObject.toMutableMap().apply {
            put("totalMicro", JsonPrimitive("2900000"))
            put("source", JsonPrimitive("arca_settlement"))
        })
        accounts[1] = JsonObject(second)
        raw["accounts"] = kotlinx.serialization.json.JsonArray(accounts)
        raw["totals"] = JsonObject(raw.getValue("totals").jsonObject.toMutableMap().apply {
            put("tradingMicro", JsonPrimitive("9320000"))
            put("totalMicro", JsonPrimitive("32820000"))
        })
        raw["watermark"] = JsonObject(raw.getValue("watermark").jsonObject.toMutableMap().apply { put("sequence", JsonPrimitive(110)) })
        val next = arcaJson.decodeFromJsonElement(WalletSnapshot.serializer(), JsonObject(raw))
        val money = WalletMoney.compose(next, landed)
        assertEquals("2900000", money.account(hl2)!!.settledMicro)
        assertNull(money.account(hl2)!!.change, "no operation and not a venue push: nothing is claimed")
    }

    @Test
    fun mismatchIsDiagnosedNeverGated() {
        val raw = Json.parseToJsonElement(File(fixturesDirectory, "00-rest.json").also { assumeTrue(it.isFile) }.readText()).jsonObject.toMutableMap()
        raw["totals"] = JsonObject(raw.getValue("totals").jsonObject.toMutableMap().apply { put("totalMicro", JsonPrimitive("99000000")) })
        val money = WalletMoney.compose(arcaJson.decodeFromJsonElement(WalletSnapshot.serializer(), JsonObject(raw)))
        assertEquals("99000000", money.total.settledMicro, "the frame is published as Arca composed it")
        assertEquals(listOf(MoneyDiagnostic.TotalsMismatch("99000000", "31010000")), money.diagnostics)
    }

    // MARK: - The watch

    @Test
    fun oneFrameOneValueAndOlderWatermarkIsIgnored() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val frames = MutableSharedFlow<WalletSnapshot>(replay = 16)
        val watch = WalletMoneyWatch(scope, ArcaLogger.disabled)
        watch.start(frames)
        val rest = fixture("00-rest")
        frames.emit(rest)
        frames.emit(fixture("01-move-reserved"))
        val reserved = withTimeout(5_000) { watch.money.first { it?.watermark?.sequence == 101L }!! }
        assertEquals(reserved.cash.departing.isEmpty(), reserved.account(hl1)!!.arriving.isEmpty(), "both ends of a leg change in the same frame")
        assertEquals(WatchStreamState.CONNECTED, watch.state.value)
        frames.emit(rest) // sequence 100 again: older than what is shown
        kotlinx.coroutines.delay(100)
        assertEquals(101L, watch.money.value?.watermark?.sequence)
        watch.stop()
        scope.cancel()
    }

    @Test
    fun watchPredictionIsRetiredByTheFrameThatCarriesIt() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val frames = MutableSharedFlow<WalletSnapshot>(replay = 16)
        val watch = WalletMoneyWatch(scope, ArcaLogger.disabled)
        watch.start(frames)
        frames.emit(fixture("00-rest"))
        withTimeout(5_000) { watch.money.first { it != null } }
        watch.predict(MoneyPrediction("req-move1", null, "cash_to_trading", "5000000", WalletEndpoint(kind = "cash"), WalletEndpoint(kind = "hyperliquid", arcaId = hl1)))
        withTimeout(5_000) { watch.money.first { it?.cash?.departing?.firstOrNull()?.predicted == true } }
        assertEquals("28510000", watch.money.value?.cash?.availableMicro, "no figure moved")
        frames.emit(fixture("01-move-reserved"))
        val framed = withTimeout(5_000) { watch.money.first { it?.watermark?.sequence == 101L }!! }
        assertEquals(listOf(false), framed.cash.departing.map { it.predicted }, "the framed leg replaced the prediction in the same step")
        watch.revalue(MoneyRevaluation(hl1, "6490000", Instant.parse("2026-10-17T18:00:11Z")))
        kotlinx.coroutines.delay(50)
        assertNull(watch.money.value?.account(hl1)?.revaluedMicro, "a settlement toward the account is pending")
        watch.stop()
        scope.cancel()
    }

    // MARK: - The stream

    private fun frame(name: String): String {
        val file = File(fixturesDirectory, "$name.json")
        assumeTrue(file.isFile)
        val text = file.readText()
        val snapshot = arcaJson.decodeFromString(WalletSnapshot.serializer(), text)
        val oneLine = Json.parseToJsonElement(text).toString()
        return "id: ${snapshot.watermark.sequence}\nevent: snapshot\ndata: $oneLine\n\n"
    }

    private fun sse(vararg frames: String): MockResponse = MockResponse().setHeader("Content-Type", "text/event-stream").setBody(frames.joinToString(""))

    private fun client(): ArcaClient = ArcaClient("fixture-token", server.url("/").toString(), OkHttpClient(), null)

    @Test
    fun streamParsesSnapshotsAndResumesWithLastEventId() = runBlocking {
        server.enqueue(sse(": connected\n\n", frame("00-rest"), "event: position\ndata: {\"sequence\":100}\n\n", frame("01-move-reserved")))
        server.enqueue(sse(frame("02-move-in-transit")))
        val runner = WalletSnapshotStreamRunner(client(), "rlm_1", "0x4ae85840bdb73e220d646eb0c564eee18a790197", "/users/alice", "", ArcaLogger.disabled)
        val sequences = withTimeout(15_000) {
            kotlinx.coroutines.flow.channelFlow { runner.run(this) }.take(3).toList().map { it.watermark.sequence }
        }
        assertEquals(listOf(100L, 101L, 102L), sequences)
        val requests = (1..2).map { server.takeRequest(1, TimeUnit.SECONDS)!! }
        assertEquals(listOf(null, "101"), requests.map { it.getHeader("Last-Event-ID") }, "Last-Event-ID carries the last delivered journal position")
        requests.forEach {
            assertTrue(it.path!!.startsWith("/api/v1/custody/v9/funding/wallet-stream?"), it.path!!)
            assertTrue(it.path!!.contains("ownerAddress=") && it.path!!.contains("arcaPath=") && !it.path!!.contains("boundaryId="), it.path!!)
        }
    }

    @Test
    fun refusedStreamThrowsTheTypedException() = runBlocking {
        server.enqueue(MockResponse().setResponseCode(404).setHeader("Content-Type", "application/json").setBody("""{"success":false,"error":{"code":"NOT_FOUND","message":"wallet not found"}}"""))
        val runner = WalletSnapshotStreamRunner(client(), "rlm_1", "0xnobody", "/users/nobody", "bnd", ArcaLogger.disabled)
        try {
            withTimeout(5_000) { kotlinx.coroutines.flow.channelFlow { runner.run(this) }.toList() }
            fail<Unit>("stream ended without throwing")
        } catch (e: ArcaException.NotFound) {
            assertEquals("NOT_FOUND", e.code)
        }
        assertEquals(1, server.requestCount, "a refusal is not retried")
    }

    @Test
    fun microHelpersAreExactIntegerArithmetic() {
        assertEquals("6", Micro.sum(listOf("1", "2", "3")))
        assertEquals("-10000", Micro.difference("5000000", "5010000"))
        assertNull(Micro.difference("x", "1"))
        assertEquals("6490000", Micro.fromDecimal("6.49"))
        assertEquals("-500000", Micro.fromDecimal("-0.5"))
        assertEquals("123456", Micro.fromDecimal("0.1234567"), "truncated toward zero past six decimals")
        assertNull(Micro.fromDecimal("abc"))
    }
}
