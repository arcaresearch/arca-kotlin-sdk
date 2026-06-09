package network.arca.sdk.models

import kotlinx.serialization.Serializable

@Serializable
public data class ExplorerSummary(
    public val objectCount: Int,
    public val operationCount: Int,
    public val eventCount: Int,
    public val pendingOperationCount: Int? = null,
    public val expiredOperationCount: Int? = null,
)
