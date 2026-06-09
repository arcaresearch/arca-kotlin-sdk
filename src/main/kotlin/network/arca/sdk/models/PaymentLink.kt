package network.arca.sdk.models

import kotlinx.serialization.Serializable
import network.arca.sdk.OperationResponse

@Serializable
public data class PaymentLinkResponse(
    public val id: String,
    public val url: String,
    public val token: String? = null,
    public val type: PaymentLinkType,
    public val status: String,
    public val amount: String,
    public val denomination: String,
    public val operationId: String,
    public val expiresAt: String,
    public val returnUrl: String? = null,
    public val createdAt: String,
)

@Serializable
public data class CreatePaymentLinkResponse(
    public val paymentLink: PaymentLinkResponse,
    override val operation: Operation,
) : OperationResponse {
    override fun withOperation(operation: Operation): CreatePaymentLinkResponse = copy(operation = operation)
}

@Serializable
public data class PaymentLinkListResponse(
    public val paymentLinks: List<PaymentLinkResponse>,
    public val total: Int,
)
