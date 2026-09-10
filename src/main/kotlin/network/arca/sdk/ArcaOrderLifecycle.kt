package network.arca.sdk

import kotlinx.serialization.Serializable
import java.net.URLEncoder
import java.nio.charset.StandardCharsets

/** The retained operation identity. Neither form submits an order. */
public sealed interface OriginalOrderReference {
    public data class Id(val value: String) : OriginalOrderReference
    public data class Path(val value: String) : OriginalOrderReference
}

@Serializable
public data class OrderLifecycleIntent(
    public val realmId: String,
    public val objectId: String,
    public val operationId: String,
    public val leg: String,
    public val venue: String,
    public val venueAccountId: String,
    public val market: String,
    public val requestedSize: String,
    public val orderType: String,
    public val side: String,
    public val timeInForce: String,
    public val executionTimeInForce: String? = null,
    public val clientOrderId: String? = null,
    public val requestRef: String? = null,
    public val price: String? = null,
    public val triggerKind: String? = null,
    public val triggerPrice: String? = null,
    public val ocoGroupId: String? = null,
    public val isTrigger: Boolean,
    public val isMarketTrigger: Boolean,
    public val sizeToMax: Boolean,
    public val reduceOnly: Boolean,
)

@Serializable
public data class OrderLifecycle(
    public val intent: OrderLifecycleIntent,
    public val venueOrderId: String? = null,
    public val submission: String,
    public val working: Boolean,
    public val execution: String,
    public val terminal: Boolean,
    public val executedSize: String,
    public val executionQuantityFinal: Boolean,
    public val requestedSizeKnown: Boolean,
    public val remainingSize: String? = null,
    public val remainingDisposition: String,
    public val accountedSize: String,
    public val accountingComplete: Boolean,
    public val averagePrice: String? = null,
    public val averagePriceFinal: Boolean,
    public val recoveryRequired: Boolean,
    public val executionReceipt: OrderLifecycleReceipt? = null,
    public val committedFills: List<OrderLifecycleFill>? = null,
)

@Serializable
private data class OrderLifecycleResponse(val lifecycle: OrderLifecycle)

internal fun OrderLifecycle.validate(realm: String, objectId: String, operationId: String?, leg: Int) {
    require(intent.realmId == realm && intent.objectId == objectId && intent.operationId.isNotEmpty() && intent.operationId.length <= 128 &&
        (operationId == null || intent.operationId == operationId) && intent.leg == leg.toString() &&
        intent.venue.isNotEmpty() && intent.venueAccountId.isNotEmpty() && intent.market.isNotEmpty() &&
        intent.orderType in listOf("MARKET", "LIMIT") && intent.side in listOf("buy", "sell")) {
        "Original order evidence does not match the requested account and operation"
    }
    committedFills.orEmpty().forEach { fill ->
        require(fill.id.isNotEmpty() && fill.realmId == realm && fill.objectId == objectId && fill.operationId == intent.operationId &&
            fill.leg == intent.leg && fill.accountId == intent.venueAccountId && fill.orderId.isNotEmpty() && fill.orderId == venueOrderId &&
            fill.market == intent.market && fill.side == intent.side && listOf(fill.size,fill.price).all { Regex("""^[0-9]+(?:\.[0-9]+)?$""").matches(it) }) {
            "Committed fill does not match original order"
        }
    }
    executionReceipt?.let { receipt ->
        require(receipt.objectId == objectId && receipt.operationId == intent.operationId && receipt.leg == intent.leg &&
            receipt.market == intent.market && receipt.orderId == (venueOrderId ?: "") && receipt.filledSize == executedSize &&
            receipt.fillsComplete == accountingComplete && receipt.averagePriceFinal == averagePriceFinal) {
            "Original order receipt does not match its server view"
        }
    }
    val quantity = Regex("""^[0-9]+(?:\.[0-9]+)?$""")
    require((listOf(intent.requestedSize, executedSize, accountedSize) + listOfNotNull(remainingSize, averagePrice)).all { quantity.matches(it) }) {
        "Original order quantity is invalid"
    }
}

/** Read the server projection after placement or reconnect; never repeats placement. */
public suspend fun Arca.getOrderLifecycle(objectId: String, operation: OriginalOrderReference, leg: Int = 0): OrderLifecycle {
    require(objectId.isNotEmpty() && objectId.length <= 128 && leg >= 0) { "An account and a nonnegative original order leg are required" }
    val query = mutableMapOf("leg" to leg.toString())
    val expectedId: String? = when (operation) {
        is OriginalOrderReference.Id -> {
            require(operation.value.isNotEmpty() && operation.value.length <= 128) { "Original operation ID is required" }
            query["operationId"] = operation.value
            operation.value
        }
        is OriginalOrderReference.Path -> {
            require(operation.value.startsWith("/") && operation.value.length <= 1024) { "Original absolute operation path is required" }
            query["operationPath"] = operation.value
            null
        }
    }
    val escapedId = URLEncoder.encode(objectId, StandardCharsets.UTF_8.name()).replace("+", "%20")
    val response: OrderLifecycleResponse = client.get("/objects/$escapedId/exchange/order-lifecycle", query = query)
    response.lifecycle.validate(realm, objectId, expectedId, leg)
    return response.lifecycle
}
