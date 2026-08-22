package network.arca.sdk.models

import kotlinx.serialization.Serializable
import network.arca.sdk.ObjectId

// MARK: - Projections

/**
 * A registered per-realm projection: a named field-filter over the canonical
 * [ObjectValuation] shape plus the path patterns defining which objects it
 * covers. Managed via `arca:ManageProjection`; read via `arca:ReadProjection`
 * grants on the projection NAME.
 */
@Serializable
public data class RealmProjection(
    public val realmId: String,
    public val name: String,
    /** Projectable fields: `equity`, `realizedValue`, `unrealizedValue`, `positions`. */
    public val fields: List<String>,
    /** Path patterns: an exact path, a bare `*`, or a trailing-wildcard prefix. */
    public val resources: List<String>,
    public val createdAt: String,
    public val updatedAt: String,
)

/**
 * The projection-filtered view of an [ObjectValuation] delivered to
 * `arca:ReadProjection` readers. Identity fields are always present;
 * everything else appears only when the projection's registered field set
 * includes it. Balances, reserved balances, and pending inbound are never
 * projectable.
 */
@Serializable
public data class ProjectedValuation(
    public val objectId: ObjectId,
    public val path: String,
    public val type: String,
    /**
     * Mirrors [ObjectValuation]'s mids completeness: present (as `false`)
     * only when a position's mid price was missing, so the price-derived
     * fields understate reality.
     */
    public val midsComplete: Boolean? = null,
    /** Pricing mode hint for client-side re-marking against the mids feed. */
    public val pricingMode: PricingMode? = null,
    /** Total equity in USD (projection field `equity`). */
    public val equity: String? = null,
    /** Realized/cash component (projection field `realizedValue`). */
    public val realizedValue: String? = null,
    /** Unrealized P&L (projection field `unrealizedValue`). */
    public val unrealizedValue: String? = null,
    /** Open positions (projection field `positions`). */
    public val positions: List<PositionValue>? = null,
)

/** One page of the batched projection read, keyset-paginated by path. */
@Serializable
public data class ProjectionValuationsPage(
    public val projection: String,
    public val fields: List<String>,
    public val valuations: List<ProjectedValuation> = emptyList(),
    /** Set when more pages remain; pass it back verbatim as `cursor`. */
    public val cursor: String? = null,
)
