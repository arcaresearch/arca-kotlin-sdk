# Consumer R8/ProGuard rules for the Arca Kotlin SDK.
#
# AGP merges `META-INF/proguard/*.pro` from library jars into the app's
# shrinker configuration automatically, so Android consumers need no manual
# keep rules for the SDK.
#
# The SDK resolves every kotlinx-serialization serializer statically (reified
# `serializer()` intrinsics and explicit `X.serializer()` calls), so most of
# the serialization machinery survives shrinking via plain reachability. These
# rules cover the reflective lookups R8 full mode can't see, e.g. a consumer
# calling `Json.decodeFromString<Market>(...)` / `serializer(typeOf<T>())`
# against SDK model types from their own code.
#
# They follow the official kotlinx.serialization R8 template, scoped to
# `network.arca.sdk.`. NOTE: the package prefix is outside the wildcard, so
# `**` is the ONLY capture group — consequent rules must reconstruct the full
# class name as `network.arca.sdk.<1>`. Referencing a second group (`<2>`)
# does not exist here and makes R8 fail the whole consumer build with
# "Wildcard <2> is invalid". Guarded by ProguardRulesTest.

# 1. Keep `Companion` object fields of serializable SDK classes, so the
#    reflective lookup `SerializerCache` performs can find them.
-if @kotlinx.serialization.Serializable class network.arca.sdk.**
-keepclassmembers class network.arca.sdk.<1> {
    static network.arca.sdk.<1>$Companion Companion;
}

# 2. Keep `serializer()` on companion objects of serializable SDK classes.
#    Groups: <1> = class suffix, <2> = the `**` in `**$Companion`, <3> = `*`.
-if @kotlinx.serialization.Serializable class network.arca.sdk.** {
    static **$Companion *;
}
-keepclassmembers class <2>$Companion {
    kotlinx.serialization.KSerializer serializer(...);
}

# 3. Keep generated `$$serializer` singletons (INSTANCE + descriptor) for
#    serializable SDK classes.
-if @kotlinx.serialization.Serializable class network.arca.sdk.**
-keepclassmembers class network.arca.sdk.<1>$$serializer {
    static network.arca.sdk.<1>$$serializer INSTANCE;
}

# 4. Keep `INSTANCE.serializer()` of serializable SDK objects.
-if @kotlinx.serialization.Serializable class network.arca.sdk.** {
    public static ** INSTANCE;
}
-keepclassmembers class network.arca.sdk.<1> {
    public static network.arca.sdk.<1> INSTANCE;
    kotlinx.serialization.KSerializer serializer(...);
}

# kotlinx-serialization-core and okhttp ship their own embedded consumer rules;
# nothing further is required for the SDK's dependencies.
