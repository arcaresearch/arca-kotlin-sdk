package network.arca.sdk.models

/**
 * Routing and correlation metadata common to all WebSocket events. Separates
 * the "spine" (who, what, when, correlation chain) from the domain payload.
 */
public data class EventEnvelope(
    public val realmId: String,
    public val entityId: String,
    public val entityPath: String? = null,
    public val eventId: String? = null,
    public val correlationId: String? = null,
    public val sequence: Int? = null,
    public val timestamp: String? = null,
    public val deliverySeq: Int? = null,
) {
    public companion object {
        /** Extract the envelope from a raw [RealmEvent]. */
        public fun from(event: RealmEvent): EventEnvelope = EventEnvelope(
            realmId = event.realmId ?: "",
            entityId = event.entityId ?: "",
            entityPath = event.entityPath,
            eventId = event.eventId,
            correlationId = event.correlationId,
            sequence = event.sequence,
            timestamp = event.timestamp,
            deliverySeq = event.deliverySeq,
        )
    }
}

/**
 * A discriminated event type pairing a strongly-typed domain payload with its
 * [EventEnvelope]. Convert a raw [RealmEvent] with [from], then `when`-match
 * exhaustively.
 */
public sealed class TypedEvent {
    /** The envelope for this event, or null for [Unknown]. */
    public abstract val envelope: EventEnvelope?

    // Core domain
    public data class OperationCreated(val operation: Operation, override val envelope: EventEnvelope) : TypedEvent()
    public data class OperationUpdated(val operation: Operation, override val envelope: EventEnvelope) : TypedEvent()
    public data class EventCreated(val event: ArcaEvent, override val envelope: EventEnvelope) : TypedEvent()
    public data class ObjectCreated(val arcaObject: ArcaObject?, override val envelope: EventEnvelope) : TypedEvent()
    public data class ObjectUpdated(val arcaObject: ArcaObject?, override val envelope: EventEnvelope) : TypedEvent()
    public data class ObjectDeleted(override val envelope: EventEnvelope) : TypedEvent()
    public data class BalanceUpdated(override val envelope: EventEnvelope) : TypedEvent()

    // Exchange / trading
    public data class ExchangeUpdated(val state: ExchangeState, override val envelope: EventEnvelope) : TypedEvent()
    public data class FillPreview(val fill: SimFill, override val envelope: EventEnvelope) : TypedEvent()
    public data class FillRecorded(val fill: Fill, override val envelope: EventEnvelope) : TypedEvent()
    public data class FundingPaymentEvent(val payment: FundingPayment, override val envelope: EventEnvelope) : TypedEvent()

    // Market data
    public data class CandleClosed(val candle: CandleEvent, override val envelope: EventEnvelope) : TypedEvent()
    public data class CandleUpdated(val candle: CandleEvent, override val envelope: EventEnvelope) : TypedEvent()
    public data class OIUpdated(val oi: OIEvent, override val envelope: EventEnvelope) : TypedEvent()
    public data class TradeExecuted(val trade: TradeEvent, override val envelope: EventEnvelope) : TypedEvent()
    public data class MidsUpdated(val mids: Map<String, String>, override val envelope: EventEnvelope) : TypedEvent()

    // Aggregation
    public data class AggregationUpdated(val aggregation: PathAggregation?, override val envelope: EventEnvelope) : TypedEvent()
    public data class ObjectValuationEvent(
        val valuation: ObjectValuation,
        val path: String,
        val watchId: String,
        override val envelope: EventEnvelope,
    ) : TypedEvent()

    // Realm
    public data class RealmCreated(val realm: Realm, override val envelope: EventEnvelope) : TypedEvent()

    // TWAP
    public data class TwapStarted(val twap: Twap, override val envelope: EventEnvelope) : TypedEvent()
    public data class TwapProgress(val twap: Twap, override val envelope: EventEnvelope) : TypedEvent()
    public data class TwapCompleted(val twap: Twap, override val envelope: EventEnvelope) : TypedEvent()
    public data class TwapCancelled(val twap: Twap, override val envelope: EventEnvelope) : TypedEvent()
    public data class TwapFailed(val twap: Twap, override val envelope: EventEnvelope) : TypedEvent()

    /** An event whose `type` doesn't match any known case. */
    public data class Unknown(val event: RealmEvent) : TypedEvent() {
        override val envelope: EventEnvelope? get() = null
    }

    public companion object {
        /** Convert a raw [RealmEvent] into a discriminated [TypedEvent]. */
        public fun from(event: RealmEvent): TypedEvent {
            val envelope = EventEnvelope.from(event)
            return when (EventType.fromWire(event.type)) {
                EventType.OPERATION_CREATED ->
                    event.operation?.let { OperationCreated(it, envelope) } ?: Unknown(event)
                EventType.OPERATION_UPDATED ->
                    event.operation?.let { OperationUpdated(it, envelope) } ?: Unknown(event)
                EventType.EVENT_CREATED ->
                    event.event?.let { EventCreated(it, envelope) } ?: Unknown(event)
                EventType.OBJECT_CREATED -> ObjectCreated(event.`object`, envelope)
                EventType.OBJECT_UPDATED -> ObjectUpdated(event.`object`, envelope)
                EventType.OBJECT_DELETED -> ObjectDeleted(envelope)
                EventType.BALANCE_UPDATED -> BalanceUpdated(envelope)
                EventType.EXCHANGE_UPDATED ->
                    event.exchangeState?.let { ExchangeUpdated(it, envelope) } ?: Unknown(event)
                EventType.FILL_PREVIEWED ->
                    event.fill?.let { FillPreview(it, envelope) } ?: Unknown(event)
                EventType.FILL_RECORDED ->
                    event.recordedFill?.let { FillRecorded(it, envelope) } ?: Unknown(event)
                EventType.EXCHANGE_FUNDING ->
                    event.funding?.let { FundingPaymentEvent(it, envelope) } ?: Unknown(event)
                EventType.CANDLE_CLOSED -> candleEvent(event)?.let { CandleClosed(it, envelope) } ?: Unknown(event)
                EventType.CANDLE_UPDATED -> candleEvent(event)?.let { CandleUpdated(it, envelope) } ?: Unknown(event)
                EventType.OI_UPDATED -> oiEvent(event)?.let { OIUpdated(it, envelope) } ?: Unknown(event)
                EventType.TRADE_EXECUTED -> {
                    val market = event.market
                    val trade = event.trade
                    if (market != null && trade != null) {
                        TradeExecuted(TradeEvent(market, trade), envelope)
                    } else {
                        Unknown(event)
                    }
                }
                EventType.MIDS_UPDATED -> event.mids?.let { MidsUpdated(it, envelope) } ?: Unknown(event)
                EventType.AGGREGATION_UPDATED -> AggregationUpdated(event.aggregation, envelope)
                EventType.OBJECT_VALUATION -> {
                    val valuation = event.valuation
                    val path = event.path
                    val watchId = event.watchId
                    if (valuation != null && path != null && watchId != null) {
                        ObjectValuationEvent(valuation, path, watchId, envelope)
                    } else {
                        Unknown(event)
                    }
                }
                EventType.REALM_CREATED ->
                    event.realm?.let { RealmCreated(it, envelope) } ?: Unknown(event)
                EventType.TWAP_STARTED -> event.twap?.let { TwapStarted(it, envelope) } ?: Unknown(event)
                EventType.TWAP_PROGRESS -> event.twap?.let { TwapProgress(it, envelope) } ?: Unknown(event)
                EventType.TWAP_COMPLETED -> event.twap?.let { TwapCompleted(it, envelope) } ?: Unknown(event)
                EventType.TWAP_CANCELLED -> event.twap?.let { TwapCancelled(it, envelope) } ?: Unknown(event)
                EventType.TWAP_FAILED -> event.twap?.let { TwapFailed(it, envelope) } ?: Unknown(event)
                else -> Unknown(event)
            }
        }

        private fun candleEvent(event: RealmEvent): CandleEvent? {
            val market = event.market ?: return null
            val interval = event.interval?.let { CandleInterval.fromWire(it) } ?: return null
            val candle = event.candle ?: return null
            return CandleEvent(market, interval, candle)
        }

        private fun oiEvent(event: RealmEvent): OIEvent? {
            val market = event.market ?: return null
            val interval = event.interval?.let { CandleInterval.fromWire(it) } ?: return null
            val bar = event.bar ?: return null
            return OIEvent(market, interval, bar, event.isClosed ?: false)
        }
    }
}
