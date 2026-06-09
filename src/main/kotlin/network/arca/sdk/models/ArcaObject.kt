package network.arca.sdk.models

import kotlinx.serialization.Serializable
import network.arca.sdk.BalanceId
import network.arca.sdk.ObjectId
import network.arca.sdk.OperationId
import network.arca.sdk.OperationResponse
import network.arca.sdk.OrgId
import network.arca.sdk.PositionId
import network.arca.sdk.RealmId
import network.arca.sdk.ReservedBalanceId
import network.arca.sdk.UserId

/**
 * Per-boundary recovery-hatch state surfaced on [ArcaObject]. Present
 * (non-null) only when the object's isolation boundary is `soft_frozen` or
 * `hard_frozen`. Active boundaries omit the field.
 */
@Serializable
public data class BoundarySnapshot(
    public val boundaryId: String,
    public val status: BoundaryStatus,
    public val lockedAt: String? = null,
    public val frozenAt: String? = null,
    public val recoveryActor: String? = null,
    public val recoveryTxHash: String? = null,
    public val recoveryArcaPath: String? = null,
)

@Serializable
public data class ArcaObject(
    public val id: ObjectId,
    public val realmId: RealmId,
    public val path: String,
    public val type: ArcaObjectType,
    public val denomination: String? = null,
    public val status: ArcaObjectStatus,
    public val metadata: String? = null,
    public val deletedAt: String? = null,
    public val systemOwned: Boolean,
    public val createdAt: String,
    public val updatedAt: String,
    /**
     * Recovery-hatch state. Null when the boundary is `active` (the happy
     * path). When non-null, callers should refuse to dispatch new operations on
     * this object — the server rejects them with `BOUNDARY_FROZEN` anyway.
     */
    public val boundary: BoundarySnapshot? = null,
)

// MARK: - Balances

@Serializable
public data class ArcaBalance(
    public val id: BalanceId? = null,
    public val arcaId: ObjectId? = null,
    public val denomination: String,
    public val amount: String? = null,
    public val arriving: String? = null,
    public val settled: String? = null,
    public val departing: String? = null,
    public val total: String? = null,
)

@Serializable
public data class ArcaBalanceListResponse(
    public val balances: List<ArcaBalance>,
)

// MARK: - Reserved balances

@Serializable
public data class ReservedBalance(
    public val id: ReservedBalanceId,
    public val arcaId: ObjectId,
    public val operationId: OperationId,
    public val denomination: String,
    public val amount: String,
    public val status: ReservedBalanceStatus,
    public val direction: ReservedBalanceDirection,
    public val sourceArcaPath: String? = null,
    public val destinationArcaPath: String? = null,
    public val createdAt: String,
    public val updatedAt: String,
)

// MARK: - Positions

@Serializable
public data class ArcaPositionCurrent(
    public val id: PositionId,
    public val realmId: RealmId,
    public val arcaId: ObjectId,
    public val market: String,
    public val side: String,
    public val size: String,
    public val leverage: Int,
    public val entryPx: String? = null,
    public val updatedAt: String,
)

// MARK: - Response types

@Serializable
public data class ArcaObjectListResponse(
    public val objects: List<ArcaObject>,
    public val total: Int,
)

@Serializable
public data class ArcaObjectBrowseResponse(
    public val folders: List<String>,
    public val objects: List<ArcaObject>,
    public val total: Int? = null,
)

@Serializable
public data class CreateArcaObjectResponse(
    public val `object`: ArcaObject,
    override val operation: Operation,
) : OperationResponse {
    override fun withOperation(operation: Operation): CreateArcaObjectResponse = copy(operation = operation)
}

@Serializable
public data class DeleteArcaObjectResponse(
    public val `object`: ArcaObject,
    override val operation: Operation,
) : OperationResponse {
    override fun withOperation(operation: Operation): DeleteArcaObjectResponse = copy(operation = operation)
}

@Serializable
public data class ArcaObjectDetailResponse(
    public val `object`: ArcaObject,
    public val operations: List<Operation> = emptyList(),
    public val events: List<ArcaEvent> = emptyList(),
    public val deltas: List<StateDelta> = emptyList(),
    public val balances: List<ArcaBalance> = emptyList(),
    public val reservedBalances: List<ReservedBalance>? = null,
    public val positions: List<ArcaPositionCurrent>? = null,
)

@Serializable
public data class ArcaObjectVersionsResponse(
    public val versions: List<ArcaObject>,
)

// MARK: - Realm

@Serializable
public data class RealmSettings(
    public val defaultApplicationFeeTenthsBps: Int? = null,
)

@Serializable
public data class Realm(
    public val id: RealmId,
    public val orgId: OrgId,
    public val name: String,
    public val slug: String,
    public val type: RealmType,
    public val asset: RealmAsset? = null,
    public val lifecycle: RealmLifecycle? = null,
    public val backing: RealmBacking? = null,
    public val description: String? = null,
    public val settings: RealmSettings? = null,
    public val archivedAt: String? = null,
    public val createdBy: UserId? = null,
    public val createdAt: String,
    public val updatedAt: String,
)
