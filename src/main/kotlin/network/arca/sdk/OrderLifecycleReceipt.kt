package network.arca.sdk

import kotlinx.serialization.Serializable
import network.arca.sdk.models.OrderExecutionReceipt

/** Shared Go owner computes every quantity, disposition and finality flag. */
@Serializable
public data class OrderLifecycleReceipt(
    public val objectId: String,
    public val operationId: String,
    public val leg: String,
    public val market: String,
    public val orderId: String,
    public val status: String,
    public val filledSize: String,
    public val requestedSize: String? = null,
    public val remainingSize: String? = null,
    public val executionState: String,
    public val fulfillmentState: String,
    public val remainingDisposition: String,
    public val avgFillPrice: String? = null,
    public val averagePriceFinal: Boolean,
    public val averagePriceSource: String,
    public val fillsComplete: Boolean,
) {
    internal fun receipt(): OrderExecutionReceipt = OrderExecutionReceipt(
        objectId = objectId, operationId = operationId, orderId = orderId, status = status,
        filledSize = filledSize, requestedSize = requestedSize, remainingSize = remainingSize,
        executionState = executionState, fulfillmentState = fulfillmentState, remainingDisposition = remainingDisposition,
        avgFillPrice = avgFillPrice, averagePriceFinal = averagePriceFinal, averagePriceSource = averagePriceSource,
        fillsComplete = fillsComplete,
    )
}
