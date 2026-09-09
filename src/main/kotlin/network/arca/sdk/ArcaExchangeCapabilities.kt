package network.arca.sdk

import kotlinx.serialization.Serializable

@Serializable
public data class ExchangeCapabilities(
    public val objectId: String,
    public val orderTypes: List<String>,
    public val timeInForce: List<String>,
    public val marginModes: List<String>,
    public val leverageSelection: Boolean,
    public val positionTriggers: Boolean,
    public val brackets: Boolean,
    public val orderKeyRetirement: Boolean,
)

/** Account-adapter authority for optional controls; never a market-prefix table. */
public suspend fun Arca.getExchangeCapabilities(objectId: String): ExchangeCapabilities {
    val result: ExchangeCapabilities = client.get("/objects/$objectId/exchange/capabilities")
    require(result.objectId == objectId) { "Account identity mismatch" }
    return result
}
