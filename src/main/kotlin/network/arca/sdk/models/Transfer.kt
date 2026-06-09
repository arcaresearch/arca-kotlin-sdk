package network.arca.sdk.models

import kotlinx.serialization.Serializable
import network.arca.sdk.ObjectId
import network.arca.sdk.OperationResponse
import network.arca.sdk.PositionId
import network.arca.sdk.RealmId

@Serializable
public data class TransferFee(
    public val amount: String,
    public val denomination: String,
)

@Serializable
public data class TransferResponse(
    override val operation: Operation,
    public val fee: TransferFee? = null,
) : OperationResponse {
    override fun withOperation(operation: Operation): TransferResponse = copy(operation = operation)
}

@Serializable
public data class FundAccountResponse(
    override val operation: Operation,
    public val poolAddress: String? = null,
    public val tokenAddress: String? = null,
    public val chain: String? = null,
    public val expiresAt: String? = null,
) : OperationResponse {
    override fun withOperation(operation: Operation): FundAccountResponse = copy(operation = operation)
}

@Serializable
public data class DefundAccountResponse(
    override val operation: Operation,
    public val txHash: String? = null,
) : OperationResponse {
    override fun withOperation(operation: Operation): DefundAccountResponse = copy(operation = operation)
}

@Serializable
public data class NonceResponse(
    public val nonce: Int,
    public val path: String,
)

@Serializable
public data class CanonicalPosition(
    public val id: PositionId,
    public val realmId: RealmId,
    public val arcaId: ObjectId,
    public val market: String,
    public val side: String,
    public val size: String,
    public val leverage: Int,
    public val updatedAt: String,
)

@Serializable
public data class SnapshotBalancesResponse(
    public val realmId: String,
    public val arcaId: String,
    public val asOf: String,
    public val balances: List<ArcaBalance> = emptyList(),
    public val positions: List<CanonicalPosition> = emptyList(),
)
