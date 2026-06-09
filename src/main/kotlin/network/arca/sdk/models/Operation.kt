package network.arca.sdk.models

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import network.arca.sdk.EventId
import network.arca.sdk.OperationId
import network.arca.sdk.RealmId
import network.arca.sdk.UserId
import network.arca.sdk.DeltaId

@Serializable
public data class Operation(
    public val id: OperationId,
    public val realmId: RealmId,
    public val path: String,
    public val type: OperationType,
    public val state: OperationState,
    public val sourceArcaPath: String? = null,
    public val targetArcaPath: String? = null,
    public val input: String? = null,
    public val outcome: String? = null,
    public val parsedOutcome: Map<String, JsonElement>? = null,
    public val failureMessage: String? = null,
    public val actorType: String? = null,
    public val actorId: UserId? = null,
    public val tokenJti: String? = null,
    public val createdAt: String,
    public val updatedAt: String,
    public val context: OperationContext? = null,
)

@Serializable
public data class OperationListResponse(
    public val operations: List<Operation>,
    public val total: Int,
)

@Serializable
public data class OperationDetailResponse(
    public val operation: Operation,
    public val context: OperationContext? = null,
    public val events: List<ArcaEvent> = emptyList(),
    public val deltas: List<StateDelta> = emptyList(),
)

// MARK: - Operation context

@Serializable
public data class OperationContext(
    public val type: String,
    public val fill: FillContext? = null,
    public val transfer: TransferContext? = null,
    public val deposit: DepositContext? = null,
    public val withdrawal: WithdrawalContext? = null,
    public val order: OrderPlacedContext? = null,
    public val cancel: CancelContext? = null,
    public val delete: DeleteContext? = null,
)

@Serializable
public data class FeeBreakdown(
    public val exchange: String,
    public val platform: String,
    public val builder: String,
)

@Serializable
public data class FillContext(
    public val side: String,
    public val size: String,
    public val price: String,
    public val market: String,
    public val direction: String? = null,
    public val orderId: String? = null,
    public val orderOperationId: String? = null,
    public val realizedPnl: String,
    public val fee: String,
    public val feeBreakdown: FeeBreakdown? = null,
    public val netBalanceChange: String,
    public val startPosition: String? = null,
    public val resultingPosition: FillResultingPosition? = null,
    public val isLiquidation: Boolean,
)

@Serializable
public data class TransferContext(
    public val amount: String,
    public val denomination: String,
    public val sourceArcaPath: String,
    public val targetArcaPath: String,
    public val feeAmount: String? = null,
)

@Serializable
public data class DepositContext(
    public val amount: String,
    public val denomination: String,
    public val destination: String? = null,
)

@Serializable
public data class WithdrawalContext(
    public val amount: String,
    public val denomination: String,
    public val txHash: String? = null,
)

@Serializable
public data class OrderPlacedContext(
    public val orderId: String,
    public val market: String,
    public val side: String,
    public val orderType: String,
    public val size: String,
    public val leverage: String? = null,
)

@Serializable
public data class CancelContext(
    public val orderId: String,
)

@Serializable
public data class DeleteContext(
    public val objectPath: String,
)

// MARK: - Event

@Serializable
public data class ArcaEvent(
    public val id: EventId,
    public val realmId: RealmId,
    public val operationId: OperationId? = null,
    public val arcaPath: String? = null,
    public val type: String,
    public val path: String? = null,
    public val payload: String? = null,
    public val createdAt: String,
)

@Serializable
public data class EventListResponse(
    public val events: List<ArcaEvent>,
    public val total: Int,
)

@Serializable
public data class EventDetailResponse(
    public val event: ArcaEvent,
    public val operation: Operation? = null,
    public val deltas: List<StateDelta> = emptyList(),
)

// MARK: - State delta

@Serializable
public data class StateDelta(
    public val id: DeltaId,
    public val realmId: RealmId,
    public val eventId: EventId? = null,
    public val arcaPath: String,
    public val deltaType: DeltaType,
    public val beforeValue: String? = null,
    public val afterValue: String? = null,
    @kotlinx.serialization.SerialName("internal")
    public val isInternal: Boolean? = null,
    public val createdAt: String,
)

@Serializable
public data class StateDeltaListResponse(
    public val deltas: List<StateDelta>,
    public val total: Int,
)
