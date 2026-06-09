package network.arca.sdk

import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.ArcaBalance
import network.arca.sdk.models.ArcaBalanceListResponse
import network.arca.sdk.models.ArcaObject
import network.arca.sdk.models.ArcaObjectBrowseResponse
import network.arca.sdk.models.ArcaObjectDetailResponse
import network.arca.sdk.models.ArcaObjectListResponse
import network.arca.sdk.models.ArcaObjectType
import network.arca.sdk.models.ArcaObjectVersionsResponse
import network.arca.sdk.models.CreateArcaObjectResponse
import network.arca.sdk.models.DeleteArcaObjectResponse
import network.arca.sdk.models.SnapshotBalancesResponse
import kotlinx.serialization.Serializable

// MARK: - Arca Object Operations

/**
 * Ensure a denominated Arca object exists at the given path (idempotent).
 *
 * Returns an [OperationHandle] — use `handle.settle()` to wait for full
 * settlement, or `handle.submitted()` for the HTTP response.
 *
 * @param ref Full Arca path (e.g. `/users/u123/main`).
 * @param metadata Optional metadata string.
 * @param operationPath Optional idempotency key (use the nonce API with separator `:`).
 */
public fun Arca.ensureDenominatedArca(
    ref: String,
    metadata: String? = null,
    operationPath: String? = null,
): OperationHandle<CreateArcaObjectResponse> =
    operationHandle {
        val body = arcaJson.encodeToJsonElement(
            CreateObjectRequest.serializer(),
            CreateObjectRequest(
                realmId = realm,
                path = ref,
                type = "denominated",
                metadata = metadata,
                operationPath = operationPath,
            ),
        )
        client.post<CreateArcaObjectResponse>("/objects", body = body)
    }

/**
 * Ensure an Arca object of any type exists at the given path (idempotent).
 *
 * @param ref Full Arca path.
 * @param type Object type.
 * @param metadata Optional metadata string.
 * @param operationPath Optional idempotency key.
 */
public fun Arca.ensureArca(
    ref: String,
    type: ArcaObjectType,
    metadata: String? = null,
    operationPath: String? = null,
): OperationHandle<CreateArcaObjectResponse> =
    operationHandle {
        val body = arcaJson.encodeToJsonElement(
            CreateObjectRequest.serializer(),
            CreateObjectRequest(
                realmId = realm,
                path = ref,
                type = type.value,
                metadata = metadata,
                operationPath = operationPath,
            ),
        )
        client.post<CreateArcaObjectResponse>("/objects", body = body)
    }

/**
 * Delete an Arca object by path.
 *
 * @param ref Arca path to delete.
 * @param sweepTo Optional path to sweep remaining funds into before deletion.
 * @param liquidatePositions If true, liquidate all exchange positions first.
 * @param operationPath Optional idempotency key.
 */
public fun Arca.ensureDeleted(
    ref: String,
    sweepTo: String? = null,
    liquidatePositions: Boolean = false,
    operationPath: String? = null,
): OperationHandle<DeleteArcaObjectResponse> =
    operationHandle {
        val body = arcaJson.encodeToJsonElement(
            DeleteObjectRequest.serializer(),
            DeleteObjectRequest(
                realmId = realm,
                path = ref,
                sweepToPath = sweepTo,
                liquidatePositions = liquidatePositions,
                operationPath = operationPath,
            ),
        )
        client.post<DeleteArcaObjectResponse>("/objects/delete", body = body)
    }

/** Get an Arca object by path. */
public suspend fun Arca.getObject(path: String): ArcaObject =
    client.get("/objects/by-path", query = mapOf("realmId" to realm, "path" to path))

/** Get full detail for an Arca object by ID (operations, events, deltas, balances, positions). */
public suspend fun Arca.getObjectDetail(objectId: String): ArcaObjectDetailResponse =
    client.get("/objects/$objectId")

/**
 * List Arca objects, optionally filtered by path.
 *
 * @param path Object path or path prefix to filter. An exact path (no trailing
 *   slash) returns the single matching object; a path prefix (trailing slash)
 *   returns all objects under that prefix.
 * @param includeDeleted Include soft-deleted objects.
 */
public suspend fun Arca.listObjects(
    path: String? = null,
    includeDeleted: Boolean = false,
): ArcaObjectListResponse {
    val query = mutableMapOf("realmId" to realm)
    if (path != null) {
        validatePath(path)
        query["prefix"] = path
    }
    if (includeDeleted) query["includeDeleted"] = "true"
    return client.get("/objects", query = query)
}

/** Get balances for an Arca object by ID. */
public suspend fun Arca.getBalances(objectId: String): List<ArcaBalance> {
    val response: ArcaBalanceListResponse = client.get("/objects/$objectId/balances")
    return response.balances
}

/** Get balances for an Arca object by path. */
public suspend fun Arca.getBalancesByPath(path: String): List<ArcaBalance> {
    val obj = getObject(path)
    return getBalances(obj.id.value)
}

/**
 * Browse objects in a folder-like structure at the given path.
 *
 * @param path Path prefix to browse (default `/`). Must start with `/`.
 * @param includeDeleted Include soft-deleted objects.
 */
public suspend fun Arca.browseObjects(
    path: String = "/",
    includeDeleted: Boolean = false,
): ArcaObjectBrowseResponse {
    validatePath(path)
    val query = mutableMapOf("realmId" to realm, "prefix" to path)
    if (includeDeleted) query["includeDeleted"] = "true"
    return client.get("/objects/browse", query = query)
}

/** Get version history for an Arca object. */
public suspend fun Arca.getObjectVersions(objectId: String): ArcaObjectVersionsResponse =
    client.get("/objects/$objectId/versions")

/** Get snapshot balances at a specific point in time. */
public suspend fun Arca.getSnapshotBalances(objectId: String, asOf: String): SnapshotBalancesResponse =
    client.get("/objects/$objectId/snapshot", query = mapOf("realmId" to realm, "asOf" to asOf))

// MARK: - Request Bodies

@Serializable
private data class CreateObjectRequest(
    val realmId: String,
    val path: String,
    val type: String,
    val metadata: String? = null,
    val operationPath: String? = null,
)

@Serializable
private data class DeleteObjectRequest(
    val realmId: String,
    val path: String,
    val sweepToPath: String? = null,
    val liquidatePositions: Boolean,
    val operationPath: String? = null,
)
