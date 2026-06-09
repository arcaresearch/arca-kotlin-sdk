package network.arca.sdk

import kotlinx.serialization.Serializable
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.DefundAccountResponse
import network.arca.sdk.models.FundAccountResponse
import network.arca.sdk.models.TransferResponse

// MARK: - Transfers, Fund/Defund Account

/**
 * Execute a transfer between two Arca objects.
 *
 * Settlement is immediate for denominated targets, or async for targets that
 * require a receiver workflow (e.g. exchange objects).
 *
 * @param path Operation path (idempotency key).
 * @param from Source Arca path.
 * @param to Target Arca path.
 * @param amount Amount as decimal string.
 * @param feeOverride Override the transfer fee (e.g. `"0"` to disable). Non-production realms only.
 */
public fun Arca.transfer(
    path: String,
    from: String,
    to: String,
    amount: String,
    feeOverride: String? = null,
): OperationHandle<TransferResponse> =
    operationHandle {
        val body = arcaJson.encodeToJsonElement(
            TransferRequest.serializer(),
            TransferRequest(
                realmId = realm,
                path = path,
                sourceArcaPath = from,
                targetArcaPath = to,
                amount = amount,
                feeOverride = feeOverride,
            ),
        )
        client.post<TransferResponse>("/transfer", body = body)
    }

/**
 * Programmatically fund an Arca object. A developer tool for development-realm
 * use (competitions, programmatic account seeding). For production deposit flows,
 * use [createPaymentLink].
 *
 * @param arcaRef Target Arca path.
 * @param amount Amount as decimal string.
 * @param path Optional operation path for idempotency.
 * @param senderAddress Optional sender wallet address (for on-chain matching).
 */
public fun Arca.fundAccount(
    arcaRef: String,
    amount: String,
    path: String? = null,
    senderAddress: String? = null,
): OperationHandle<FundAccountResponse> =
    operationHandle {
        val body = arcaJson.encodeToJsonElement(
            FundAccountRequest.serializer(),
            FundAccountRequest(
                realmId = realm,
                arcaPath = arcaRef,
                amount = amount,
                path = path,
                senderAddress = senderAddress,
            ),
        )
        client.post<FundAccountResponse>("/fund-account", body = body)
    }

/**
 * Programmatically withdraw from an Arca object. A developer tool for
 * development-realm use. For production withdrawal flows, use [createPaymentLink].
 *
 * @param arcaPath Source Arca path.
 * @param amount Amount as decimal string.
 * @param destinationAddress On-chain destination address (omit to burn in development mode).
 * @param path Optional operation path for idempotency.
 */
public fun Arca.defundAccount(
    arcaPath: String,
    amount: String,
    destinationAddress: String? = null,
    path: String? = null,
): OperationHandle<DefundAccountResponse> =
    operationHandle {
        val body = arcaJson.encodeToJsonElement(
            DefundAccountRequest.serializer(),
            DefundAccountRequest(
                realmId = realm,
                arcaPath = arcaPath,
                amount = amount,
                destinationAddress = destinationAddress ?: "",
                path = path,
            ),
        )
        client.post<DefundAccountResponse>("/defund-account", body = body)
    }

// MARK: - Request Bodies

@Serializable
private data class TransferRequest(
    val realmId: String,
    val path: String,
    val sourceArcaPath: String,
    val targetArcaPath: String,
    val amount: String,
    val feeOverride: String? = null,
)

@Serializable
private data class FundAccountRequest(
    val realmId: String,
    val arcaPath: String,
    val amount: String,
    val path: String? = null,
    val senderAddress: String? = null,
)

@Serializable
private data class DefundAccountRequest(
    val realmId: String,
    val arcaPath: String,
    val amount: String,
    val destinationAddress: String,
    val path: String? = null,
)
