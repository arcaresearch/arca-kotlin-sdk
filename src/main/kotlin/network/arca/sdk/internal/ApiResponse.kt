package network.arca.sdk.internal

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject

/**
 * The standard Arca API response envelope. All API responses are wrapped in
 * `{ success, data?, error? }`.
 */
@Serializable
internal data class ApiResponse<T>(
    val success: Boolean = false,
    val data: T? = null,
    val error: ApiErrorBody? = null,
)

@Serializable
internal data class ApiErrorBody(
    val code: String,
    val message: String,
    val errorId: String? = null,
    /**
     * Structured challenge some refusals carry (co-sign, step-up). Kept as a
     * raw [JsonObject] rather than a typed shape because the fields differ per
     * error code, and an unrecognised one must not fail decoding of the error
     * itself.
     */
    val details: JsonObject? = null,
)
