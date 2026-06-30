package network.arca.sdk.models

/** Event types emitted by the Arca WebSocket stream. */
public enum class EventType(public val wire: String) {
    OPERATION_CREATED("operation.created"),
    OPERATION_UPDATED("operation.updated"),
    EVENT_CREATED("event.created"),
    OBJECT_CREATED("object.created"),
    OBJECT_UPDATED("object.updated"),
    OBJECT_DELETED("object.deleted"),
    BALANCE_UPDATED("balance.updated"),
    EXCHANGE_UPDATED("exchange.updated"),
    AGGREGATION_UPDATED("aggregation.updated"),
    MIDS_UPDATED("mids.updated"),
    CANDLE_CLOSED("candle.closed"),
    CANDLE_UPDATED("candle.updated"),
    OI_UPDATED("oi.updated"),
    TRADE_EXECUTED("trade.executed"),
    TRADES_BATCH("trades.batch"),
    REALM_CREATED("realm.created"),
    AGENT_TEXT("agent.text"),
    AGENT_TOOL_USE("agent.tool_use"),
    AGENT_PLAN("agent.plan"),
    AGENT_CONVERSATION_LOG("agent.conversation_log"),
    AGENT_DONE("agent.done"),
    AGENT_STEP_UPDATED("agent.step_updated"),
    AGENT_EXECUTION_DONE("agent.execution_done"),

    /**
     * Phase 1 of two-phase fill delivery: an instant, incomplete venue-level
     * fill echo. `FILL_RECORDED` (Phase 2) follows with the authoritative
     * record; merge the pair by `correlationId`.
     */
    FILL_PREVIEWED("fill.previewed"),
    FILL_RECORDED("fill.recorded"),
    EXCHANGE_FUNDING("exchange.funding"),
    OBJECT_VALUATION("object.valuation"),
    CHART_SNAPSHOT_UPDATED("chart.snapshot.updated"),
    TWAP_STARTED("twap.started"),
    TWAP_PROGRESS("twap.progress"),
    TWAP_COMPLETED("twap.completed"),
    TWAP_CANCELLED("twap.cancelled"),
    TWAP_FAILED("twap.failed"),
    ;

    public companion object {
        public fun fromWire(value: String): EventType? = entries.firstOrNull { it.wire == value }
    }
}

/** Channel groups for WebSocket subscriptions. */
public enum class Channel(public val wire: String) {
    OPERATIONS("operations"),
    BALANCES("balances"),
    EXCHANGE("exchange"),
    OBJECTS("objects"),
    EVENTS("events"),
    AGGREGATION("aggregation"),
    AGENT("agent"),
}

/** WebSocket connection status. */
public enum class ConnectionStatus {
    CONNECTING,
    CONNECTED,
    DISCONNECTED,
}
