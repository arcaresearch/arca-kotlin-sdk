package network.arca.sdk

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.CreatePaymentLinkResponse
import network.arca.sdk.models.PaymentLinkListResponse
import network.arca.sdk.models.PaymentLinkType

// MARK: - Payment Links

/**
 * Create a payment link for deposit or withdrawal.
 *
 * Returns an [OperationHandle] — use `handle.settle()` to wait for full
 * settlement, or `handle.submitted()` for the HTTP response.
 *
 * @param type Whether this is a deposit or withdrawal link.
 * @param arcaRef Target Arca path.
 * @param amount Amount as decimal string.
 * @param returnUrl Optional URL to redirect after payment.
 * @param expiresInMinutes Optional expiration window.
 * @param metadata Optional key-value metadata (encoded as a JSON string).
 */
public fun Arca.createPaymentLink(
    type: PaymentLinkType,
    arcaRef: String,
    amount: String,
    returnUrl: String? = null,
    expiresInMinutes: Int? = null,
    metadata: JsonObject? = null,
): OperationHandle<CreatePaymentLinkResponse> {
    val metadataStr = metadata?.toString()
    return operationHandle {
        val body = arcaJson.encodeToJsonElement(
            CreatePaymentLinkRequest.serializer(),
            CreatePaymentLinkRequest(
                realmId = realm,
                type = type.wire,
                arcaPath = arcaRef,
                amount = amount,
                returnUrl = returnUrl,
                expiresInMinutes = expiresInMinutes,
                metadata = metadataStr,
            ),
        )
        client.post<CreatePaymentLinkResponse>("/payment-links", body = body)
    }
}

/**
 * List payment links, optionally filtered by type and/or status.
 *
 * @param type Filter by deposit or withdrawal.
 * @param status Filter by status string.
 */
public suspend fun Arca.listPaymentLinks(
    type: PaymentLinkType? = null,
    status: String? = null,
): PaymentLinkListResponse {
    val query = mutableMapOf("realmId" to realm)
    if (type != null) query["type"] = type.wire
    if (status != null) query["status"] = status
    return client.get("/payment-links", query = query)
}

// MARK: - Request Bodies

@Serializable
private data class CreatePaymentLinkRequest(
    val realmId: String,
    val type: String,
    val arcaPath: String,
    val amount: String,
    val returnUrl: String? = null,
    val expiresInMinutes: Int? = null,
    val metadata: String? = null,
)
