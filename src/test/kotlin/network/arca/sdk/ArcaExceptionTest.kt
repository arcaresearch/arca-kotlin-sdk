package network.arca.sdk

import network.arca.sdk.internal.ApiResponse
import network.arca.sdk.internal.arcaJson
import network.arca.sdk.models.ExplorerSummary
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ArcaExceptionTest {

    @Test
    fun validationErrorMapping() {
        val error = mapApiError("VALIDATION_ERROR", "Name is required", "err_01abc")
        val v = assertInstanceOf(ArcaException.Validation::class.java, error)
        assertEquals("Name is required", v.message)
        assertEquals("err_01abc", v.errorId)
    }

    @Test
    fun unauthorizedMapping() {
        val error = mapApiError("UNAUTHORIZED", "Invalid token", null)
        val u = assertInstanceOf(ArcaException.Unauthorized::class.java, error)
        assertEquals("Invalid token", u.message)
    }

    @Test
    fun notFoundVariants() {
        val variants = listOf(
            "NOT_FOUND", "USER_NOT_FOUND", "REALM_NOT_FOUND", "OBJECT_NOT_FOUND",
            "ORG_NOT_FOUND", "ORDER_NOT_FOUND", "ACCOUNT_NOT_FOUND",
            "MEMBER_NOT_FOUND", "PROFILE_NOT_FOUND", "INVITATION_NOT_FOUND",
        )
        for (code in variants) {
            val error = mapApiError(code, "Not found", null)
            val nf = assertInstanceOf(ArcaException.NotFound::class.java, error, "Expected NotFound for $code")
            assertEquals(code, nf.code)
        }
    }

    @Test
    fun conflictVariants() {
        val variants = listOf(
            "CONFLICT", "ALREADY_EXISTS", "ALREADY_MEMBER", "ALREADY_DELETED",
            "DUPLICATE_REALM", "ALREADY_REVOKED", "IDEMPOTENCY_VIOLATION",
            // Order-placement conflicts carry their specific code.
            "NO_LIQUIDITY", "MARKET_DELISTED",
        )
        for (code in variants) {
            val error = mapApiError(code, "Conflict", null)
            val c = assertInstanceOf(ArcaException.Conflict::class.java, error, "Expected Conflict for $code")
            assertEquals(code, c.code)
        }
    }

    @Test
    fun exchangeErrorVariants() {
        val variants = listOf("EXCHANGE_ERROR", "EXCHANGE_UNAVAILABLE", "ORDER_FAILED", "INVALID_REQUEST")
        for (code in variants) {
            val error = mapApiError(code, "Exchange error", null)
            val e = assertInstanceOf(ArcaException.Exchange::class.java, error, "Expected Exchange for $code")
            assertEquals(code, e.code)
        }
    }

    @Test
    fun internalErrorMapping() {
        val error = mapApiError("INTERNAL_ERROR", "Something went wrong", "err_01xyz")
        val i = assertInstanceOf(ArcaException.Internal::class.java, error)
        assertEquals("Something went wrong", i.message)
        assertEquals("err_01xyz", i.errorId)
    }

    @Test
    fun unknownCodeMapping() {
        val error = mapApiError("SOME_NEW_CODE", "Unknown", null)
        val u = assertInstanceOf(ArcaException.Unknown::class.java, error)
        assertEquals("SOME_NEW_CODE", u.code)
        assertEquals("SOME_NEW_CODE: Unknown", u.message)
    }

    @Test
    fun forbiddenMapping() {
        val error = mapApiError("FORBIDDEN", "Access denied", null)
        val f = assertInstanceOf(ArcaException.Forbidden::class.java, error)
        assertEquals("Access denied", f.message)
    }

    @Test
    fun errorMessages() {
        assertEquals("Bad input", ArcaException.Validation("Bad input").message)
        assertEquals("Expired", ArcaException.Unauthorized("Expired").message)
        assertEquals(
            "Non-JSON response (HTTP 500): <html>",
            ArcaException.NonJsonResponse(500, "<html>").message,
        )
    }

    @Test
    fun apiResponseSuccessDecoding() {
        val json = """
            { "success": true, "data": { "objectCount": 5, "operationCount": 10, "eventCount": 20 } }
        """.trimIndent()
        val response = arcaJson.decodeFromString(ApiResponse.serializer(ExplorerSummary.serializer()), json)
        assertTrue(response.success)
        assertEquals(5, response.data?.objectCount)
        assertNull(response.error)
    }

    @Test
    fun apiResponseErrorDecoding() {
        val json = """
            {
                "success": false,
                "error": { "code": "VALIDATION_ERROR", "message": "Realm name is required", "errorId": "err_01abc" }
            }
        """.trimIndent()
        val response = arcaJson.decodeFromString(ApiResponse.serializer(ExplorerSummary.serializer()), json)
        assertFalse(response.success)
        assertNull(response.data)
        assertEquals("VALIDATION_ERROR", response.error?.code)
        assertEquals("Realm name is required", response.error?.message)
        assertEquals("err_01abc", response.error?.errorId)
    }
}
