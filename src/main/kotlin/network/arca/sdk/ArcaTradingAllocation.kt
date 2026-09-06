package network.arca.sdk

import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.TradingAllocationRead
import network.arca.sdk.models.TradingAllocationQuote
import network.arca.sdk.models.TradingAllocationQuoteRequest

/** Read capability, intent and allocation at one mirror observation. */
public suspend fun Arca.getTradingAllocation(objectId: String, market: String? = null): TradingAllocationRead {
    val query = if (market == null) emptyMap() else mapOf("market" to market)
    return client.get("/objects/$objectId/exchange/allocation", query = query)
}

/** Estimate only. Reserves nothing and never submits or resizes an order. */
public suspend fun Arca.quoteTradingAllocation(objectId: String, request: TradingAllocationQuoteRequest): TradingAllocationQuote {
    val body = arcaJson.encodeToJsonElement(TradingAllocationQuoteRequest.serializer(), request)
    return client.post("/objects/$objectId/exchange/allocation/quote", body = body)
}
