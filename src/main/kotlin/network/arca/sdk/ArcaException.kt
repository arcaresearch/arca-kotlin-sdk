package network.arca.sdk

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
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

    /**
     * Conflict (HTTP 409). Covers duplicates, idempotency violations, and
     * venue refusals where [code] carries the specific reason:
     * `NO_LIQUIDITY` (empty book side, retry or use a marketable limit),
     * `MARKET_DELISTED` (market delisted, positions settled by the venue),
     * `MARKET_NOT_TRADABLE` (halted or not yet live),
     * `MARKET_NOT_USDC_COLLATERAL`, `VENUE_RATE_LIMITED` (the account's venue
     * request allowance is spent — on Hyperliquid it is earned by cumulative
     * volume traded rather than elapsed time, so waiting does not restore it),
     * or `ORDER_FAILED` (a refusal with no narrower code — the message carries
     * the venue's verbatim text).
     *
     * None are retryable as-is: the venue evaluated the request and said no.
     */
    public class Conflict(public val code: String, message: String, errorId: String? = null) :
        ArcaException(message, errorId)

    /** Unexpected server error (HTTP 500). */
    public class Internal(message: String, errorId: String? = null) : ArcaException(message, errorId)

    /**
     * The request couldn't be delivered to the upstream exchange, or its answer
     * couldn't be read (HTTP 502) — a transport fault, so retryable. A refusal
     * *by* the venue is [Conflict] or [Validation] instead, carrying the
     * venue's own reason.
     */
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

    /**
     * The operation would move value out of a co-sign-armed boundary without
     * the owner's signature (HTTP 412 `COSIGN_REQUIRED`).
     *
     * The SDK cannot transparently retry this the way it could a browser
     * confirmation: a co-signature comes from a key the platform does not
     * hold. Route [challenge] to whatever holds the boundary's co-sign key.
     *
     * For venue hops, `hopVenues(..., sign = ...)` handles this end to end.
     */
    public class CosignRequired(
        message: String,
        public val challenge: CosignRequiredChallenge,
        errorId: String? = null,
    ) : ArcaException(message, errorId)

    /** Unknown API error code. */
    public class Unknown(public val code: String, message: String, errorId: String? = null) :
        ArcaException("$code: $message", errorId)
}

/**
 * The structured payload accompanying a 412 `COSIGN_REQUIRED` response.
 *
 * [surface] is the discriminator worth branching on: `transfer.venue_hop`,
 * `transfer.venue_deposit`, `transfer.cross_boundary`,
 * `deposit.venue_deposit`, `withdrawal.plain`.
 */
public data class CosignRequiredChallenge(
    public val surface: String,
    public val boundaryId: String,
    /** Set on single-object surfaces (deposit, withdrawal). */
    public val arcaPath: String? = null,
    /** Set on the two-ended surfaces (transfer, hop). */
    public val sourceArcaPath: String? = null,
    public val targetArcaPath: String? = null,
    /** Endpoints that collect the signature, when the surface has a pair. */
    public val propose: String? = null,
    public val submit: String? = null,
)

/**
 * Extracts a co-sign challenge from the server's `error.details`.
 *
 * Only `boundaryId` is required: the surfaces differ in which path fields they
 * carry, and a challenge naming the boundary is still actionable even if a
 * future surface adds fields this version does not know.
 */
internal fun parseCosignChallenge(details: JsonObject?): CosignRequiredChallenge? {
    if (details == null) return null
    fun str(key: String): String? = (details[key] as? JsonPrimitive)?.takeIf { it.isString }?.content
    val boundaryId = str("boundaryId") ?: return null
    return CosignRequiredChallenge(
        surface = str("surface") ?: "",
        boundaryId = boundaryId,
        arcaPath = str("arcaPath"),
        sourceArcaPath = str("sourceArcaPath"),
        targetArcaPath = str("targetArcaPath"),
        propose = str("propose"),
        submit = str("submit"),
    )
}

/** Maps an API error response code to the appropriate [ArcaException] subtype. */
public fun mapApiError(
    code: String,
    message: String,
    errorId: String?,
    details: JsonObject? = null,
): ArcaException = when (code) {
    "VALIDATION_ERROR" -> ArcaException.Validation(message, errorId)

    "UNAUTHORIZED", "UNAUTHENTICATED" -> ArcaException.Unauthorized(message, errorId)

    "FORBIDDEN", "REALM_SCOPE_MISMATCH" -> ArcaException.Forbidden(message, errorId, code)

    "NOT_FOUND", "USER_NOT_FOUND", "REALM_NOT_FOUND", "OBJECT_NOT_FOUND",
    "ORG_NOT_FOUND", "ORDER_NOT_FOUND", "ACCOUNT_NOT_FOUND",
    "MEMBER_NOT_FOUND", "PROFILE_NOT_FOUND", "INVITATION_NOT_FOUND",
    -> ArcaException.NotFound(code, message, errorId)

    "CONFLICT", "ALREADY_EXISTS", "ALREADY_MEMBER", "ALREADY_DELETED",
    "DUPLICATE_REALM", "ALREADY_REVOKED", "IDEMPOTENCY_VIOLATION",
    // Venue refusals (409): the venue evaluated a well-formed request and said
    // no. NO_LIQUIDITY = empty book side (retry / marketable limit);
    // MARKET_DELISTED = market delisted, positions settled by the venue;
    // MARKET_NOT_TRADABLE = halted or not yet live; VENUE_RATE_LIMITED = the
    // account's venue request allowance is spent (volume-earned on HL, so
    // waiting does not help); ORDER_FAILED = a refusal with no narrower code,
    // verbatim venue text in the message.
    "NO_LIQUIDITY", "MARKET_DELISTED", "MARKET_NOT_TRADABLE",
    "MARKET_NOT_USDC_COLLATERAL", "VENUE_RATE_LIMITED", "ORDER_FAILED",
    -> ArcaException.Conflict(code, message, errorId)

    "INTERNAL_ERROR" -> ArcaException.Internal(message, errorId)

    "EXCHANGE_ERROR", "EXCHANGE_UNAVAILABLE", "INVALID_REQUEST",
    -> ArcaException.Exchange(code, message, errorId)

    "COSIGN_REQUIRED" -> parseCosignChallenge(details)
        ?.let { ArcaException.CosignRequired(message, it, errorId) }
        ?: ArcaException.Unknown(code, message, errorId)

    else -> ArcaException.Unknown(code, message, errorId)
}
