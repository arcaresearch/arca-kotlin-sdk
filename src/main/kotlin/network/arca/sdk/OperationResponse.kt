package network.arca.sdk

import network.arca.sdk.models.Operation

/**
 * A response that wraps a platform [Operation]. Mutating endpoints return one
 * of these; [network.arca.sdk.OperationHandle] uses [withOperation] to graft a
 * freshly-settled operation back onto the original response shape.
 */
public interface OperationResponse {
    public val operation: Operation

    /** Returns a copy of this response with [operation] replaced. */
    public fun withOperation(operation: Operation): OperationResponse
}
