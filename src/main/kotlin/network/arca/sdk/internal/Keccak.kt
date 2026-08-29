package network.arca.sdk.internal

/**
 * Keccak-256, the hash Ethereum uses.
 *
 * Hand-rolled rather than taken from a dependency because this SDK ships to
 * Android with three dependencies and adding BouncyCastle (several MB) to
 * compute one hash is a poor trade. The JVM's own `MessageDigest("SHA3-256")`
 * is NOT a substitute: SHA-3 finalises with the 0x06 domain byte where
 * original Keccak uses 0x01, so it produces a different digest for every
 * input, and is absent below Android API 29 regardless.
 *
 * Correctness is pinned by the NIST-era Keccak vectors and by the cross-SDK
 * co-sign golden vectors in `CosignVectorsTest`.
 */
internal object Keccak {
    private const val RATE_BYTES = 136 // 1600-bit state, 256-bit capacity

    private val ROUND_CONSTANTS = longArrayOf(
        0x0000000000000001uL.toLong(), 0x0000000000008082uL.toLong(),
        0x800000000000808AuL.toLong(), 0x8000000080008000uL.toLong(),
        0x000000000000808BuL.toLong(), 0x0000000080000001uL.toLong(),
        0x8000000080008081uL.toLong(), 0x8000000000008009uL.toLong(),
        0x000000000000008AuL.toLong(), 0x0000000000000088uL.toLong(),
        0x0000000080008009uL.toLong(), 0x000000008000000AuL.toLong(),
        0x000000008000808BuL.toLong(), 0x800000000000008BuL.toLong(),
        0x8000000000008089uL.toLong(), 0x8000000000008003uL.toLong(),
        0x8000000000008002uL.toLong(), 0x8000000000000080uL.toLong(),
        0x000000000000800AuL.toLong(), 0x800000008000000AuL.toLong(),
        0x8000000080008081uL.toLong(), 0x8000000000008080uL.toLong(),
        0x0000000080000001uL.toLong(), 0x8000000080008008uL.toLong(),
    )

    // Lane destinations and rotation offsets for the combined rho+pi step,
    // in the order the standard's compact formulation visits them.
    private val PI_LANES = intArrayOf(
        10, 7, 11, 17, 18, 3, 5, 16, 8, 21, 24, 4,
        15, 23, 19, 13, 12, 2, 20, 14, 22, 9, 6, 1,
    )
    private val RHO_OFFSETS = intArrayOf(
        1, 3, 6, 10, 15, 21, 28, 36, 45, 55, 2, 14,
        27, 41, 56, 8, 25, 43, 62, 18, 39, 61, 20, 44,
    )

    fun digest(input: ByteArray): ByteArray {
        val state = LongArray(25)

        // Absorb every whole block.
        var offset = 0
        while (input.size - offset >= RATE_BYTES) {
            absorbBlock(state, input, offset)
            permute(state)
            offset += RATE_BYTES
        }

        // Absorb the tail under pad10*1 with Keccak's 0x01 domain byte.
        val tail = ByteArray(RATE_BYTES)
        input.copyInto(tail, 0, offset, input.size)
        tail[input.size - offset] = 0x01
        tail[RATE_BYTES - 1] = (tail[RATE_BYTES - 1].toInt() or 0x80).toByte()
        absorbBlock(state, tail, 0)
        permute(state)

        // Squeeze 32 bytes; the rate is wider, so one pass suffices.
        val out = ByteArray(32)
        for (i in 0 until 4) {
            var lane = state[i]
            for (b in 0 until 8) {
                out[i * 8 + b] = (lane and 0xFF).toByte()
                lane = lane ushr 8
            }
        }
        return out
    }

    private fun absorbBlock(state: LongArray, block: ByteArray, offset: Int) {
        for (i in 0 until RATE_BYTES / 8) {
            var lane = 0L
            for (b in 7 downTo 0) {
                lane = (lane shl 8) or (block[offset + i * 8 + b].toLong() and 0xFF)
            }
            state[i] = state[i] xor lane
        }
    }

    private fun permute(a: LongArray) {
        val c = LongArray(5)
        for (round in 0 until 24) {
            // theta
            for (x in 0 until 5) {
                c[x] = a[x] xor a[x + 5] xor a[x + 10] xor a[x + 15] xor a[x + 20]
            }
            for (x in 0 until 5) {
                val d = c[(x + 4) % 5] xor java.lang.Long.rotateLeft(c[(x + 1) % 5], 1)
                for (y in 0 until 25 step 5) {
                    a[x + y] = a[x + y] xor d
                }
            }

            // rho + pi
            var last = a[1]
            for (i in 0 until 24) {
                val lane = PI_LANES[i]
                val held = a[lane]
                a[lane] = java.lang.Long.rotateLeft(last, RHO_OFFSETS[i])
                last = held
            }

            // chi
            for (y in 0 until 25 step 5) {
                for (x in 0 until 5) {
                    c[x] = a[y + x]
                }
                for (x in 0 until 5) {
                    a[y + x] = c[x] xor (c[(x + 1) % 5].inv() and c[(x + 2) % 5])
                }
            }

            // iota
            a[0] = a[0] xor ROUND_CONSTANTS[round]
        }
    }
}
