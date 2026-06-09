package network.arca.sdk.internal

import kotlinx.serialization.Serializable

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
)
