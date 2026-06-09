package network.arca.sdk.models

// MARK: - Transfer direction

/** Direction of a transfer relative to a specific Arca object. */
public enum class TransferDirection(public val wire: String) {
    INCOMING("incoming"),
    OUTGOING("outgoing"),
}

/**
 * Determine whether this transfer is incoming or outgoing relative to
 * [objectPath]. Returns `null` for non-transfer operations or when neither
 * path matches.
 */
public fun Operation.transferDirection(objectPath: String): TransferDirection? {
    if (type != OperationType.TRANSFER) return null
    if (targetArcaPath?.startsWith(objectPath) == true) return TransferDirection.INCOMING
    if (sourceArcaPath?.startsWith(objectPath) == true) return TransferDirection.OUTGOING
    return null
}

/**
 * A human-readable label for the counterparty in a transfer (e.g. "Vault").
 * Given the arca path of the object whose history is being viewed, returns a
 * friendly name for the *other* side of the transfer.
 */
public fun Operation.counterpartyLabel(objectPath: String): String? {
    if (type != OperationType.TRANSFER) return null
    val otherPath: String? = when {
        sourceArcaPath?.startsWith(objectPath) == true -> targetArcaPath
        targetArcaPath?.startsWith(objectPath) == true -> sourceArcaPath
        else -> return null
    }
    val path = otherPath ?: return null
    val segments = path.split("/").filter { it.isNotEmpty() }
    if (segments.lastOrNull() == "main") return "Vault"
    return segments.lastOrNull() ?: path
}

// MARK: - Context convenience accessors

/** Transfer amount (null for non-transfer contexts). */
public val OperationContext.transferAmount: String? get() = transfer?.amount

/** Transfer fee amount, if a fee was charged. */
public val OperationContext.transferFee: String? get() = transfer?.feeAmount

/** Transfer denomination (e.g. "USD"). */
public val OperationContext.transferDenomination: String? get() = transfer?.denomination
