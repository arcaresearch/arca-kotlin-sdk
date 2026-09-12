package network.arca.sdk

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.runCurrent
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class PositionViewTest {
    companion object {
        fun snapshot(size: String, tick: Int = 1, market: String = "BTC"): ExchangeState {
            val positions = if (size == "0") "[]" else """[{"id":"p","market":"$market","side":"${if(size.startsWith("-")) "short" else "long"}","size":"${size.removePrefix("-")}","entryPrice":"100","leverage":5,"marginUsed":"90"}]"""
            return arcaJson.decodeFromString("""{"account":{"id":"venue","realmId":"r","name":"","createdAt":"","updatedAt":""},"marginSummary":{"equity":"999","initialMarginUsed":"90","maintenanceMarginRequired":"10","availableToWithdraw":"800","totalNtlPos":"100","totalUnrealizedPnl":"5"},"positions":$positions,"openOrders":[],"pendingIntents":[],"tradingAllocation":{"asOf":"2026-09-11T10:00:00.${tick.toString().padStart(9,'0')}Z","validUntil":"2099-01-01T00:00:00Z","revision":"1","preferences":{},"projectionUnavailable":false}}""")
        }
    }
    private fun operation(id: String, side: OrderSide) = Operation(OperationId(id), RealmId("r"), "/op/$id", OperationType.ORDER, OperationState.COMPLETED,
        input = """{"exchangeObjectId":"a","market":"BTC","side":"${side.wire}"}""", createdAt = "2026-09-11T10:00:01Z", updatedAt = "")
    private fun bind(view: PositionView, side: OrderSide, id: String = "op"): PositionUpdate = view.begin("BTC",side).also { view.bind(it,operation(id,side),"a") }
    private fun receipt(view: PositionView, token: PositionUpdate, size: String, op: String = "op", status: String = "FILLED") {
        view.receive(token,OrderExecutionReceipt(objectId="a",operationId=op,orderId="order-$op",status=status,filledSize=size,executionState="filled",fulfillmentState="full",remainingDisposition="filled"))
    }
    private fun size(view: PositionView) = view.current.value.positions.firstOrNull { it.market == "BTC" }?.signedSize ?: "0"

    @Test fun transitionsInBothAccountReceiptOrderingsAndLateSnapshots() = runTest {
        val cases = listOf(listOf("0","buy","2","2"),listOf("2","buy","1","3"),listOf("2","sell","1","1"),listOf("2","sell","2","0"),listOf("2","sell","3","-1"),listOf("-2","buy","3","1"),listOf("0.1","buy","0.2","0.3"))
        for ((base,side,executed,expected) in cases) for (accountFirst in listOf(false,true)) {
            val authority = snapshot(base)
            val view = PositionView("a",{snapshot(expected,3)},{ work -> launch { work() } }); view.observe(authority)
            val token = bind(view,OrderSide.fromWire(side)!!)
            if(accountFirst) view.observe(snapshot(expected,2)); receipt(view,token,executed)
            if(!accountFirst) view.observe(snapshot(base,2))
            assertEquals(expected,size(view)); assertEquals("execution",view.current.value.coverage.first().status)
            assertTrue(view.current.value.positions.all { it.source == "execution" && it.authoritativePosition == null }); assertEquals("999",authority.marginSummary.equity)
            view.accounted(token); assertEquals(expected,size(view)); assertTrue(view.current.value.pendingMarkets.isEmpty()); assertEquals("accounted",view.current.value.coverage.first().status)
            view.observe(snapshot(base)); receipt(view,token,executed); assertEquals(expected,size(view))
        }
    }
    @Test fun mixedAccountingRetainsOtherConfirmedFill() = runTest {
        var reads=0; val view=PositionView("a",{reads++; snapshot("1",4)},{ work -> launch { work() } }); view.observe(snapshot("2"))
        val a=bind(view,OrderSide.SELL,"a"); val b=bind(view,OrderSide.BUY,"b")
        receipt(view,a,"2","a"); receipt(view,b,"1","b"); view.accounted(a); view.observe(snapshot("0",2))
        assertEquals("1",size(view)); assertEquals(0,reads); view.accounted(b); assertEquals("1",size(view)); assertEquals(1,reads)
    }
    @Test fun oldReconciliationCannotDropNewScope() = runTest {
        val response=CompletableDeferred<ExchangeState>(); val started=CompletableDeferred<Unit>()
        val view=PositionView("a",{started.complete(Unit); response.await()},{ work -> launch { work() } }); view.observe(snapshot("2"))
        val a=bind(view,OrderSide.SELL,"a"); receipt(view,a,"2","a"); val accounting=async { view.accounted(a) }; started.await()
        val b=bind(view,OrderSide.BUY,"b"); receipt(view,b,"1","b"); response.complete(snapshot("0",2)); accounting.await()
        assertEquals("1",size(view)); assertEquals(2,view.current.value.coverage.size)
    }
    @Test fun cumulativePartialsDeduplicateAndRejectForeignIdentity() = runTest {
        val view=PositionView("a",{snapshot("0")},{ work -> launch { work() } }); view.observe(snapshot("2")); val token=bind(view,OrderSide.SELL)
        receipt(view,token,"0.5",status="CANCELLED"); receipt(view,token,"0.5",status="CANCELLED"); assertEquals("1.5",size(view))
        receipt(view,token,"1",status="CANCELLED"); receipt(view,token,"0.2"); receipt(view,token,"10","foreign"); assertEquals("1",size(view))
    }
    @Test fun unrelatedFillInvalidatesUntilFreshAccounting() = runTest {
        val view=PositionView("a",{snapshot("4",3)},{ work -> launch { work() } }); view.observe(snapshot("2")); val token=bind(view,OrderSide.BUY); receipt(view,token,"1")
        view.observeFill("BTC","external","external-order"); view.observe(snapshot("4",2))
        assertEquals(listOf("BTC"),view.current.value.unavailableMarkets); assertEquals("unavailable",view.current.value.coverage.first().status)
        assertThrows(IllegalStateException::class.java) { view.begin("BTC",OrderSide.BUY) }
        view.accounted(token); assertEquals("4",size(view)); assertTrue(view.current.value.unavailableMarkets.isEmpty())
    }
    @Test fun fillBeforeAttachmentAndReset() = runTest {
        val view=PositionView("a",{snapshot("0",3)},{ work -> launch { work() } })
        assertThrows(IllegalStateException::class.java) { view.begin("BTC",OrderSide.BUY) }; view.observe(snapshot("2"))
        val token=view.begin("BTC",OrderSide.SELL); view.observeFill("BTC","op","order-op"); assertEquals(listOf("BTC"),view.current.value.unavailableMarkets)
        view.bind(token,operation("op",OrderSide.SELL),"a"); receipt(view,token,"2"); assertTrue(view.current.value.unavailableMarkets.isEmpty()); assertEquals("0",size(view))
        view.invalidate(); assertEquals("unavailable",view.current.value.coverage.first().status)
        view.reset(); view.observe(snapshot("2",9)); receipt(view,token,"2"); view.accounted(token); assertEquals(PositionViewSnapshot(),view.current.value)
    }
    @Test fun boundedReservationsAndCancellation() = runTest {
        val view=PositionView("a",{snapshot("0")},{ work -> launch { work() } }); view.observe(snapshot("0"))
        view.begin("BTC",OrderSide.BUY).cancelBeforeSubmission(); assertTrue(view.current.value.pendingMarkets.isEmpty())
        val token=bind(view,OrderSide.BUY); token.cancelBeforeSubmission(); assertEquals(listOf("BTC"),view.current.value.pendingMarkets)
        repeat(127) { view.begin("BTC",OrderSide.BUY) }; assertThrows(IllegalStateException::class.java) { view.begin("BTC",OrderSide.BUY) }
    }
    @Test fun contradictorySnapshotAndAlreadyIncludedFill() = runTest {
        val view=PositionView("a",{snapshot("0")},{ work -> launch { work() } }); view.observe(snapshot("2"))
        val token=bind(view,OrderSide.BUY); receipt(view,token,"1")
        view.observeFill("BTC","older","older-order","2026-01-01T00:00:00Z"); assertTrue(view.current.value.unavailableMarkets.isEmpty())
        view.observe(snapshot("4",2)); assertEquals(listOf("BTC"),view.current.value.unavailableMarkets)
    }

}
