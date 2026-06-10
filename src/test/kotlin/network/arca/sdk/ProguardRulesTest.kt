package network.arca.sdk

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test

/**
 * Validates the embedded consumer R8 rules (`META-INF/proguard/arca-sdk.pro`).
 *
 * Regression guard for the v1.0.0 packaging bug where a `-keepclassmembers`
 * consequent referenced wildcard group `<2>` while the preceding `-if`
 * condition only captured one group (`network.arca.sdk.**`): R8 rejects the
 * merged config with "Wildcard <2> is invalid", failing every consumer's
 * minified build at `minifyReleaseWithR8`.
 *
 * The checker mirrors ProGuard/R8 numbering: wildcards (`***`, `**`, `*`,
 * `?`, `...`) are numbered left-to-right across the `-if` condition, and a
 * backreference `<N>` in the consequent rule must resolve to one of them.
 * Backreferences themselves do not create new groups.
 */
class ProguardRulesTest {

    private fun loadRules(): String {
        val url = javaClass.classLoader.getResource("META-INF/proguard/arca-sdk.pro")
            ?: fail("META-INF/proguard/arca-sdk.pro missing from the jar resources")
        return url.readText()
    }

    /** Strip comments and split into `-directive ...` statements. */
    private fun statements(text: String): List<String> {
        val noComments = text.lines().joinToString("\n") { it.substringBefore('#') }
        val result = mutableListOf<StringBuilder>()
        for (line in noComments.lines()) {
            if (line.startsWith("-")) {
                result.add(StringBuilder(line))
            } else if (result.isNotEmpty() && line.isNotBlank()) {
                result.last().append('\n').append(line)
            }
        }
        return result.map { it.toString() }
    }

    /** Count referenceable wildcard groups, skipping `<N>` backreferences. */
    private fun wildcardGroupCount(statement: String): Int {
        val withoutRefs = statement.replace(Regex("<\\d+>"), "")
        return Regex("\\*\\*\\*|\\*\\*|\\*|\\?|\\.\\.\\.").findAll(withoutRefs).count()
    }

    private fun maxBackreference(statement: String): Int =
        Regex("<(\\d+)>").findAll(statement).maxOfOrNull { it.groupValues[1].toInt() } ?: 0

    @Test
    fun backreferencesResolveToCapturedWildcardGroups() {
        val stmts = statements(loadRules())
        assertTrue(stmts.isNotEmpty(), "Rules file parsed to zero statements")

        var pendingIfGroups: Int? = null
        for (stmt in stmts) {
            if (stmt.startsWith("-if")) {
                pendingIfGroups = wildcardGroupCount(stmt)
                continue
            }
            val maxRef = maxBackreference(stmt)
            if (maxRef > 0) {
                val available = pendingIfGroups
                    ?: fail<Nothing>("Backreference <$maxRef> without a preceding -if condition in:\n$stmt")
                assertTrue(
                    maxRef <= available,
                    "Backreference <$maxRef> exceeds the $available wildcard group(s) captured by the " +
                        "preceding -if condition (R8: \"Wildcard <$maxRef> is invalid\") in:\n$stmt",
                )
            }
            pendingIfGroups = null
        }
    }

    @Test
    fun ifConditionsAreConsumedByExactlyOneKeepRule() {
        val stmts = statements(loadRules())
        var pendingIf: String? = null
        for (stmt in stmts) {
            if (stmt.startsWith("-if")) {
                if (pendingIf != null) fail<Nothing>("Two consecutive -if conditions; the first is dangling:\n$pendingIf")
                pendingIf = stmt
            } else {
                assertTrue(stmt.startsWith("-keep"), "Only -keep* rules expected after -if, got:\n$stmt")
                pendingIf = null
            }
        }
        if (pendingIf != null) fail<Nothing>("Trailing -if condition with no keep rule:\n$pendingIf")
    }
}
