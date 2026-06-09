package network.arca.sdk.models

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import network.arca.sdk.internal.arcaJson

/**
 * An event received from the Arca WebSocket stream. All fields except [type]
 * are optional — their presence depends on the event type.
 *
 * The wire `fill` key carries two different shapes: a venue-level [SimFill] on
 * `fill.previewed`, or the platform-level [Fill] on `fill.recorded`. It is
 * captured raw as [fillRaw] and decoded on demand by the [fill] / [recordedFill]
 * accessors based on [type].
 */
@Serializable
public data class RealmEvent(
    public val realmId: String? = null,
    public val type: String,
    public val entityId: String? = null,
    public val entityPath: String? = null,
    public val summary: ExplorerSummary? = null,
    public val operation: Operation? = null,
    public val event: ArcaEvent? = null,
    public val `object`: ArcaObject? = null,
    public val mids: Map<String, String>? = null,
    public val exchangeState: ExchangeState? = null,
    public val valuation: ObjectValuation? = null,
    public val path: String? = null,
    public val watchId: String? = null,
    public val aggregation: PathAggregation? = null,
    public val market: String? = null,
    public val interval: String? = null,
    public val candle: Candle? = null,
    @SerialName("fill") internal val fillRaw: JsonElement? = null,
    public val funding: FundingPayment? = null,
    public val trade: MarketTrade? = null,
    public val realm: Realm? = null,
    public val twap: Twap? = null,
    /** Present and true when the server detected and corrected a cache drift. */
    public val driftCorrected: Boolean? = null,
    // Envelope fields (Convergent Event Spine)
    public val eventId: String? = null,
    public val correlationId: String? = null,
    public val sequence: Int? = null,
    public val timestamp: String? = null,
    public val deliverySeq: Int? = null,
) {
    /** Venue-level fill, present on `fill.previewed` events. */
    public val fill: SimFill?
        get() {
            val el = fillRaw ?: return null
            if (type == EventType.FILL_RECORDED.wire) return null
            return runCatching { arcaJson.decodeFromJsonElement(SimFill.serializer(), el) }.getOrNull()
        }

    /** Platform-level fill, present on `fill.recorded` events. */
    public val recordedFill: Fill?
        get() {
            val el = fillRaw ?: return null
            if (type != EventType.FILL_RECORDED.wire) return null
            return runCatching { arcaJson.decodeFromJsonElement(Fill.serializer(), el) }.getOrNull()
        }
}
