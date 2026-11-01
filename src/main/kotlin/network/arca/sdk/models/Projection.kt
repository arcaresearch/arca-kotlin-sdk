package network.arca.sdk.models

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.builtins.MapSerializer
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
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
    /**
     * Projectable fields: `equity`, `realizedValue`, `unrealizedValue`,
     * `positions`, and the windowed `pnl` and `volume`.
     */
    public val fields: List<String>,
    /** Path patterns: an exact path, a bare `*`, or a trailing-wildcard prefix. */
    public val resources: List<String>,
    public val createdAt: String,
    public val updatedAt: String,
)

/** A rolling window a projection read or watch can ask for. */
public enum class ProjectionWindow(public val wire: String) {
    THIRTY_DAYS("30d"),
}

/** Readiness of one windowed figure. */
public enum class WindowedStatus(public val wire: String) {
    /** The value covers the whole window. */
    COMPLETE("complete"),

    /**
     * P&L only: a mark was missing at `from` or now, so the value is off by
     * that position's move.
     */
    INCOMPLETE("incomplete"),

    /** Not computed yet. The value is absent, so it never reads as zero. */
    PENDING("pending"),
    ;

    public companion object {
        /** The status for a wire value; null for one a newer server added. */
        public fun fromWire(value: String): WindowedStatus? = entries.firstOrNull { it.wire == value }
    }
}

/**
 * An account's P&L over `(from, asOf]`: `A(asOf) - A(from)`, where
 * `A(t) = equity(t) - cumInflows(t) + cumOutflows(t)`. Deposits, withdrawals
 * and transfers are flows and never count; realized and unrealized gains,
 * fees, funding and liquidations do. Equals `getPnlHistory`'s P&L for the
 * same `from`. Rank or sum rows only when their `from` matches.
 */
@Serializable
public data class ProjectedPnl(
    public val window: String,
    /** Window start (an hour boundary), RFC 3339. */
    public val from: String,
    /** When the value was computed. Absent while pending. */
    public val asOf: String? = null,
    /** `"full"`, or `"since_creation"` for an account younger than the window. */
    public val coverage: String? = null,
    /** The account's first point, set with `coverage == "since_creation"`. */
    public val coveredFrom: String? = null,
    /** `"complete"`, `"incomplete"` or `"pending"`; see [windowedStatus]. */
    public val status: String,
    /** Exact decimal USD. Absent while pending. */
    public val pnlUsd: String? = null,
) {
    /** [status] as a known value; null for one a newer server added. */
    public val windowedStatus: WindowedStatus? get() = WindowedStatus.fromWire(status)
}

/**
 * An account's traded volume over `(from, asOf]`: the exact sum of
 * `|fill size × fill price|` over its fill records booked in the window,
 * liquidations and partial fills included, each fill counted once. Deposits,
 * transfers, fees, funding and unfilled orders never count, and a price move
 * never changes it.
 */
@Serializable
public data class ProjectedVolume(
    public val window: String,
    /** Window start (an hour boundary), RFC 3339. */
    public val from: String,
    /** The ledger read the sum is true at. Absent while pending. */
    public val asOf: String? = null,
    public val coverage: String? = null,
    public val coveredFrom: String? = null,
    /** `"complete"` or `"pending"`; see [windowedStatus]. */
    public val status: String,
    /** Exact decimal USD. Absent while pending. */
    public val volumeUsd: String? = null,
) {
    /** [status] as a known value; null for one a newer server added. */
    public val windowedStatus: WindowedStatus? get() = WindowedStatus.fromWire(status)
}

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
    /** Windowed P&L (projection field `pnl`), present when the read asked for a window. */
    @Serializable(with = LenientPnlSerializer::class)
    public val pnl: ProjectedPnl? = null,
    /** Windowed volume (projection field `volume`), present when the read asked for a window. */
    @Serializable(with = LenientVolumeSerializer::class)
    public val volume: ProjectedVolume? = null,
)

/** One page of the batched projection read, keyset-paginated by path. */
@Serializable
public data class ProjectionValuationsPage(
    public val projection: String,
    public val fields: List<String>,
    /** The window the rows' `pnl` / `volume` figures cover, when one was asked for. */
    public val window: String? = null,
    @Serializable(with = ProjectedRowListSerializer::class)
    public val valuations: List<ProjectedValuation> = emptyList(),
    /** Set when more pages remain; pass it back verbatim as `cursor`. */
    public val cursor: String? = null,
)

// MARK: - Lenient decoding
//
// Projection rows decode one by one: a row (or a windowed figure) this SDK
// cannot read is dropped on its own and never empties a snapshot or a frame.

private fun <T> JsonDecoder.decodeOrNull(serializer: KSerializer<T>, element: JsonElement): T? =
    runCatching { json.decodeFromJsonElement(serializer, element) }.getOrNull()

/** Decodes a value, or null when it does not decode, without failing its container. */
internal open class LenientNullableSerializer<T : Any>(private val inner: KSerializer<T>) : KSerializer<T?> {
    private val delegate = inner.nullable
    override val descriptor: SerialDescriptor get() = delegate.descriptor
    override fun serialize(encoder: Encoder, value: T?) = delegate.serialize(encoder, value)
    override fun deserialize(decoder: Decoder): T? {
        val json = decoder as? JsonDecoder ?: return delegate.deserialize(decoder)
        return json.decodeOrNull(inner, json.decodeJsonElement())
    }
}

internal object LenientPnlSerializer : LenientNullableSerializer<ProjectedPnl>(ProjectedPnl.serializer())

internal object LenientVolumeSerializer : LenientNullableSerializer<ProjectedVolume>(ProjectedVolume.serializer())

/** A list of projection rows, skipping rows that do not decode. */
internal object ProjectedRowListSerializer : KSerializer<List<ProjectedValuation>> {
    private val delegate = ListSerializer(ProjectedValuation.serializer())
    override val descriptor: SerialDescriptor get() = delegate.descriptor
    override fun serialize(encoder: Encoder, value: List<ProjectedValuation>) = delegate.serialize(encoder, value)
    override fun deserialize(decoder: Decoder): List<ProjectedValuation> {
        val json = decoder as? JsonDecoder ?: return delegate.deserialize(decoder)
        val array = json.decodeJsonElement() as? JsonArray ?: return emptyList()
        return array.mapNotNull { json.decodeOrNull(ProjectedValuation.serializer(), it) }
    }
}

/** Projection rows keyed by path, skipping rows that do not decode. */
internal object ProjectedRowMapSerializer : KSerializer<Map<String, ProjectedValuation>> {
    private val delegate = MapSerializer(String.serializer(), ProjectedValuation.serializer())
    override val descriptor: SerialDescriptor get() = delegate.descriptor
    override fun serialize(encoder: Encoder, value: Map<String, ProjectedValuation>) = delegate.serialize(encoder, value)
    override fun deserialize(decoder: Decoder): Map<String, ProjectedValuation> {
        val json = decoder as? JsonDecoder ?: return delegate.deserialize(decoder)
        val obj = json.decodeJsonElement() as? JsonObject ?: return emptyMap()
        return buildMap {
            for ((path, element) in obj) {
                json.decodeOrNull(ProjectedValuation.serializer(), element)?.let { put(path, it) }
            }
        }
    }
}
