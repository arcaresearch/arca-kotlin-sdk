package network.arca.sdk

import network.arca.sdk.models.Operation

/**
 * Base type for every error thrown by the Arca SDK.
 *
 * Each subtype carries a human-readable [message] and, where the server
 * supplied one, an [errorId] correlation token. This mirrors the Swift SDK's
 * `ArcaError` enum as a Kotlin sealed hierarchy so callers can `when`-match
 * exhaustively.
 */
public sealed class ArcaException(
    message: String,
    public val errorId: String? = null,
    cause: Throwable? = null,
) : Exception(message, cause) {

    /** Validation error (HTTP 400). */
    public class Validation(message: String, errorId: String? = null) : ArcaException(message, errorId)

    /** Authentication failed (HTTP 401). */
    public class Unauthorized(message: String, errorId: String? = null) : ArcaException(message, errorId)

    /**
     * Forbidden — insufficient permissions (HTTP 403). [code] carries the
     * domain-specific variant (`FORBIDDEN` or `REALM_SCOPE_MISMATCH`).
     *
     * On a token-provider client a 403 commonly means the cached token is
     * still valid but scoped to a different identity than the one the
     * provider would now mint for (e.g. the app switched signed-in users).
     * The SDK reacts by re-invoking the provider once and retrying; an
     * unrecoverable 403 is surfaced through `onAuthError` so integrators
     * can tear down and rebuild the [Arca] instance.
     */
    public class Forbidden(
        message: String,
        errorId: String? = null,
        public val code: String = "FORBIDDEN",
    ) : ArcaException(message, errorId)

    /**
     * Resource not found (HTTP 404). [code] carries the domain-specific
     * variant (e.g. `OBJECT_NOT_FOUND`, `REALM_NOT_FOUND`).
     */
    public class NotFound(public val code: String, message: String, errorId: String? = null) :
        ArcaException(message, errorId)

    /** Conflict (HTTP 409). Covers duplicates, idempotency violations, etc. */
    public class Conflict(public val code: String, message: String, errorId: String? = null) :
        ArcaException(message, errorId)

    /** Unexpected server error (HTTP 500). */
    public class Internal(message: String, errorId: String? = null) : ArcaException(message, errorId)

    /** Upstream exchange service error (HTTP 502). */
    public class Exchange(public val code: String, message: String, errorId: String? = null) :
        ArcaException(message, errorId)

    /** Network-level failure (no response received). */
    public class Network(cause: Throwable) :
        ArcaException("Network error: ${cause.message}", null, cause)

    /** Failed to decode the response body. */
    public class Decoding(cause: Throwable) :
        ArcaException("Decoding error: ${cause.message}", null, cause)

    /** Server returned a non-JSON response. */
    public class NonJsonResponse(public val statusCode: Int, public val body: String) :
        ArcaException("Non-JSON response (HTTP $statusCode): ${body.take(200)}")

    /**
     * The operation completed with a non-success terminal state (`failed` or
     * `expired`). The full [operation] is available for inspection (e.g.
     * `operation.outcome`).
     */
    public class OperationFailed(public val operation: Operation) : ArcaException(
        "Operation ${operation.id} ${operation.state.wire}: ${operation.outcome ?: operation.state.wire}",
    )

    /** Unknown API error code. */
    public class Unknown(public val code: String, message: String, errorId: String? = null) :
        ArcaException("$code: $message", errorId)
}

/** Maps an API error response code to the appropriate [ArcaException] subtype. */
public fun mapApiError(code: String, message: String, errorId: String?): ArcaException = when (code) {
    "VALIDATION_ERROR" -> ArcaException.Validation(message, errorId)

    "UNAUTHORIZED", "UNAUTHENTICATED" -> ArcaException.Unauthorized(message, errorId)

    "FORBIDDEN", "REALM_SCOPE_MISMATCH" -> ArcaException.Forbidden(message, errorId, code)

    "NOT_FOUND", "USER_NOT_FOUND", "REALM_NOT_FOUND", "OBJECT_NOT_FOUND",
    "ORG_NOT_FOUND", "ORDER_NOT_FOUND", "ACCOUNT_NOT_FOUND",
    "MEMBER_NOT_FOUND", "PROFILE_NOT_FOUND", "INVITATION_NOT_FOUND",
    -> ArcaException.NotFound(code, message, errorId)

    "CONFLICT", "ALREADY_EXISTS", "ALREADY_MEMBER", "ALREADY_DELETED",
    "DUPLICATE_REALM", "ALREADY_REVOKED", "IDEMPOTENCY_VIOLATION",
    -> ArcaException.Conflict(code, message, errorId)

    "INTERNAL_ERROR" -> ArcaException.Internal(message, errorId)

    "EXCHANGE_ERROR", "EXCHANGE_UNAVAILABLE", "ORDER_FAILED", "INVALID_REQUEST",
    -> ArcaException.Exchange(code, message, errorId)

    else -> ArcaException.Unknown(code, message, errorId)
}
