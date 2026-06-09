package network.arca.sdk.internal

import kotlinx.serialization.json.Json

/**
 * The single [Json] instance used for every encode/decode in the SDK.
 *
 * - [Json.ignoreUnknownKeys]: forward-compatible with new server fields.
 * - [Json.explicitNulls] = false: omit null fields on encode (mirrors Swift's
 *   `encodeIfPresent`) and tolerate absent keys on decode.
 * - [Json.coerceInputValues] = true: an unknown enum value or explicit null for
 *   a property that has a default decodes to that default instead of throwing.
 */
internal val arcaJson: Json = Json {
    ignoreUnknownKeys = true
    explicitNulls = false
    coerceInputValues = true
    isLenient = false
}
