package network.arca.sdk

import kotlinx.serialization.Serializable
import network.arca.sdk.internal.arcaJson

// MARK: - Token Minting

/** Permission preset for [Arca.mintDeviceToken]. */
public enum class DeviceTokenPermissions {
    /** `arca:Read` (the default — view-only). */
    READ,

    /**
     * `arca:Read` + `arca:Exchange` (place/cancel orders + TWAPs + read exchange
     * state). The "trading" preset for retail-style device tokens.
     */
    TRADE,

    /**
     * `arca:Read` + `arca:Write` (place orders, transfer, lifecycle). The widest
     * preset; use only when the end-user needs full control over their resources.
     */
    FULL,
}

/** Response from a token-minting call. */
@Serializable
public data class MintTokenResponse(
    public val token: String,
    public val expiresAt: String,
    public val jti: String? = null,
)

/** An IAM token scope: a set of policy statements. */
@Serializable
public data class TokenScope(
    public val statements: List<PolicyStatement>,
)

/** A single IAM policy statement. */
@Serializable
public data class PolicyStatement(
    public val effect: String,
    public val actions: List<String>,
    public val resources: List<String>,
)

/**
 * Mint a scoped JWT for a single end-user device with one of three preset
 * permission levels, instead of constructing IAM [PolicyStatement]s by hand.
 *
 * This call requires the **caller's** current token to have sufficient
 * permission to mint scoped tokens (typically a builder-issued JWT, not a device
 * token). It is intended for builder-side code that issues device tokens for end
 * users.
 *
 * The presets are:
 *
 * | preset  | actions                                          |
 * |---------|--------------------------------------------------|
 * | [DeviceTokenPermissions.READ]  | `arca:Read`                       |
 * | [DeviceTokenPermissions.TRADE] | `arca:Read` + `arca:Exchange`     |
 * | [DeviceTokenPermissions.FULL]  | `arca:Read` + `arca:Write`        |
 *
 * @param realmId Realm the token is scoped to.
 * @param sub Subject (user identifier) embedded in the token.
 * @param forUserPath Resource scope: the path subtree the end-user owns (e.g.
 *   `"/users/alice"`). The token is usable only on resources at or below that
 *   path. Defaults to `"*"` (no resource restriction).
 * @param permissions Permission preset (default [DeviceTokenPermissions.READ]).
 * @param expirationMinutes Optional token lifetime in minutes.
 */
public suspend fun Arca.mintDeviceToken(
    realmId: String,
    sub: String,
    forUserPath: String? = null,
    permissions: DeviceTokenPermissions = DeviceTokenPermissions.READ,
    expirationMinutes: Int? = null,
): MintTokenResponse {
    val resource = forUserPath ?: "*"
    val actions = when (permissions) {
        DeviceTokenPermissions.READ -> listOf("arca:Read")
        DeviceTokenPermissions.TRADE -> listOf("arca:Read", "arca:Exchange")
        DeviceTokenPermissions.FULL -> listOf("arca:Read", "arca:Write")
    }
    val body = arcaJson.encodeToJsonElement(
        MintTokenRequest.serializer(),
        MintTokenRequest(
            realmId = realmId,
            sub = sub,
            scope = TokenScope(
                statements = listOf(
                    PolicyStatement(effect = "Allow", actions = actions, resources = listOf(resource)),
                ),
            ),
            expirationMinutes = expirationMinutes,
        ),
    )
    return client.post("/auth/token", body = body)
}

// MARK: - Internal Request Types

@Serializable
private data class MintTokenRequest(
    val realmId: String,
    val sub: String,
    val scope: TokenScope,
    val expirationMinutes: Int? = null,
)
