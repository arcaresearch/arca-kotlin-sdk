package network.arca.sdk

import network.arca.sdk.models.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class OrderReceiptRefinementTest {
    private fun receipt(size: String = "3", status: String = "FILLED", final: Boolean = false) = OrderExecutionReceipt(
        objectId = "account", operationId = "original", orderId = "order", status = status, filledSize = size,
        requestedSize = "10", remainingSize = "7", executionState = "partial", fulfillmentState = "partial",
        remainingDisposition = "cancelled", avgFillPrice = "100", averagePriceFinal = final)
    private fun fill(id: String = "a", size: String = "1", price: String = "100", order: String = "order",
                     original: String? = "original", recorded: String? = "fill-operation", stable: String? = null) = Fill(
        id = id, orderId = order, orderOperationId = original, operationId = recorded, fillId = stable,
        market = "gll:test:1", size = size, price = price, side = OrderSide.BUY)
    @Test fun completeRecordedFillsRefinePriceAndPreserveOriginalPartialIOC() {
        val original = receipt()
        val result = original.refined(listOf(fill(size = "1", price = "100"), fill("b", size = "2", price = "103")))
        assertEquals(original.copy(avgFillPrice = "102", averagePriceFinal = true, averagePriceSource = "ledger_vwap", fillsComplete = true), result)
    }
    @Test fun previewReplayAndOtherBracketLegsDoNotDoubleCount() {
        val fills = listOf(fill(size = "3", recorded = null), fill("row1", size = "3", price = "102", stable = "venue1"),
            fill("row2", size = "3.0", price = "102.0", stable = "venue1"), fill("child", size = "50", order = "other"))
        assertEquals("102", receipt().refined(fills).avgFillPrice)
        assertTrue(receipt().refined(fills).averagePriceFinal)
    }
    @Test fun incompleteOverfilledForeignConflictingAndUnrecordedEvidencePreserveProvisional() {
        val original = receipt()
        val cases = listOf(listOf(fill(size = "2")), listOf(fill(size = "4")), listOf(fill(size = "3", original = "foreign")),
            listOf(fill(size = "3", original = null)), listOf(fill(size = "3", recorded = null)),
            listOf(fill("a", size = "3", stable = "venue"), fill("b", size = "3", price = "101", stable = "venue")),
            listOf(fill(size = "invalid")), listOf(fill(size = "3", price = "0")), listOf(fill(size = "3", price = "9".repeat(39))))
        for (fills in cases) assertEquals(original, original.refined(fills))
    }
    @Test fun noFillOpenAndAlreadyFinalNeverChange() {
        for (original in listOf(receipt(size = "0"), receipt(status = "OPEN"), receipt(final = true))) {
            assertEquals(original, original.refined(listOf(fill(size = "3", price = "105"))))
        }
    }
    @Test fun repeatingAverageUsesEighteenFractionalDigits() {
        val result = receipt().refined(listOf(fill(size = "1", price = "1"), fill("b", size = "2", price = "2")))
        assertTrue(result.averagePriceFinal)
        assertEquals("1.666666666666666667", result.avgFillPrice)
    }
}
