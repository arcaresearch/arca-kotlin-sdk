package network.arca.sdk.internal

import kotlinx.serialization.Serializable

/** Temporary scaffold type used to validate the build toolchain. Replaced by the real SDK. */
@Serializable
internal data class SdkInfo(val name: String, val version: String)
