package network.arca.sdk

import kotlinx.serialization.Serializable

/*
 * Type-safe identifier wrappers. All entity IDs follow the TypeID format
 * `prefix_base32suffix`. Each is a zero-overhead inline value class that
 * serializes transparently as its underlying string, giving compile-time
 * safety (you cannot pass an [OperationId] where an [ObjectId] is expected)
 * without any runtime cost — the Kotlin equivalent of Swift's phantom-typed
 * `TypedID<Tag>`.
 */

@Serializable
@JvmInline
public value class ObjectId(public val value: String) {
    override fun toString(): String = value
}

@Serializable
@JvmInline
public value class OperationId(public val value: String) {
    override fun toString(): String = value
}

@Serializable
@JvmInline
public value class EventId(public val value: String) {
    override fun toString(): String = value
}

@Serializable
@JvmInline
public value class DeltaId(public val value: String) {
    override fun toString(): String = value
}

@Serializable
@JvmInline
public value class BalanceId(public val value: String) {
    override fun toString(): String = value
}

@Serializable
@JvmInline
public value class ReservedBalanceId(public val value: String) {
    override fun toString(): String = value
}

@Serializable
@JvmInline
public value class PositionId(public val value: String) {
    override fun toString(): String = value
}

@Serializable
@JvmInline
public value class RealmId(public val value: String) {
    override fun toString(): String = value
}

@Serializable
@JvmInline
public value class UserId(public val value: String) {
    override fun toString(): String = value
}

@Serializable
@JvmInline
public value class OrgId(public val value: String) {
    override fun toString(): String = value
}

@Serializable
@JvmInline
public value class SimAccountId(public val value: String) {
    override fun toString(): String = value
}

@Serializable
@JvmInline
public value class SimPositionId(public val value: String) {
    override fun toString(): String = value
}

@Serializable
@JvmInline
public value class SimOrderId(public val value: String) {
    override fun toString(): String = value
}

@Serializable
@JvmInline
public value class SimFillId(public val value: String) {
    override fun toString(): String = value
}

@Serializable
@JvmInline
public value class ErrorId(public val value: String) {
    override fun toString(): String = value
}

@Serializable
@JvmInline
public value class WatchId(public val value: String) {
    override fun toString(): String = value
}
