package network.arca.sdk

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.withTimeout
import network.arca.sdk.models.Operation
import network.arca.sdk.models.OperationState

/**
 * A handle returned synchronously from mutation methods.
 *
 * The HTTP call starts immediately when the handle is created. Use [settled]
 * to wait for both submission and operation settlement, or [submitted] to
 * access the HTTP response before settlement.
 *
 * ```kotlin
 * // Simple: one-liner await to settlement
 * arca.deposit(arcaRef = "/wallets/main", amount = "1000").settle()
 *
 * // Progressive disclosure
 * val deposit = arca.deposit(arcaRef = "/wallets/main", amount = "1000")
 * val response = deposit.submitted()   // before settlement
 * deposit.settle()                     // full settlement
 * deposit.settled(timeoutSeconds = 15.0)
 *
 * // Batching
 * coroutineScope {
 *     val d1 = async { arca.deposit(arcaRef = "/wallets/main", amount = "500").settled() }
 *     val d2 = async { arca.deposit(arcaRef = "/wallets/savings", amount = "300").settled() }
 *     awaitAll(d1, d2)
 * }
 * ```
 */
public class OperationHandle<R : OperationResponse> internal constructor(
    scope: CoroutineScope,
    submit: suspend () -> R,
    waitForSettlement: suspend (String) -> Operation,
) {
    private val submittedDeferred: Deferred<R> = scope.async { submit() }

    private val settledDeferred: Deferred<R> = scope.async {
        val response = submittedDeferred.await()
        if (response.operation.state != OperationState.PENDING) {
            response
        } else {
            val completed = waitForSettlement(response.operation.id.value)
            @Suppress("UNCHECKED_CAST")
            response.withOperation(completed) as R
        }
    }

    /**
     * The HTTP response (before settlement). Resolves as soon as the server
     * accepts the request; the operation may still be `pending`.
     */
    public suspend fun submitted(): R = submittedDeferred.await()

    /**
     * Wait for full operation settlement. Resolves when the operation reaches a
     * terminal state (`completed`, `failed`, or `expired`). Throws
     * [ArcaException.OperationFailed] if the terminal state is `failed` or
     * `expired`.
     */
    public suspend fun settled(): R = settledDeferred.await()

    /** Wait for full operation settlement; convenience alias for [settled]. */
    public suspend fun settle(): R = settledDeferred.await()

    /**
     * Wait for settlement with an explicit timeout. Throws
     * [ArcaException.Unknown] with code `TIMEOUT` if the deadline passes. The
     * underlying settlement continues independently of the timeout.
     */
    public suspend fun settled(timeoutSeconds: Double): R =
        try {
            withTimeout((timeoutSeconds * 1000).toLong()) { settledDeferred.await() }
        } catch (_: TimeoutCancellationException) {
            throw ArcaException.Unknown(
                "TIMEOUT",
                "Operation timed out after ${timeoutSeconds.toInt()}s",
                null,
            )
        }
}
