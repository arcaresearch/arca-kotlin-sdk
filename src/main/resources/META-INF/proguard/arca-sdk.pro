# Consumer R8/ProGuard rules for the Arca Kotlin SDK.
#
# AGP merges `META-INF/proguard/*.pro` from library jars into the app's
# shrinker configuration automatically, so Android consumers need no manual
# keep rules for the SDK.
#
# The SDK resolves every kotlinx-serialization serializer statically (reified
# `serializer()` intrinsics and explicit `X.serializer()` calls), so most of
# the serialization machinery survives shrinking via plain reachability. These
# rules cover the reflective lookups R8 full mode ("isMinifyEnabled" + AGP 8
# defaults) can't see:
#
# 1. `@Serializable` companion objects are looked up reflectively by
#    kotlinx-serialization when a consumer calls `serializer(typeOf<T>())` /
#    `Json.decodeFromString<T>` against SDK model types from their own code.
-if @kotlinx.serialization.Serializable class network.arca.sdk.**
-keepclassmembers class <1>.<2> {
    static <1>.<2>$Companion Companion;
}

# 2. Generated `$serializer` singletons (INSTANCE + descriptor) for SDK models.
-if @kotlinx.serialization.Serializable class network.arca.sdk.**
-keepclassmembers class <1>.<2>$$serializer {
    static <1>.<2>$$serializer INSTANCE;
}

# 3. Named companions / `serializer()` factory methods on serializable classes.
-keepclassmembers @kotlinx.serialization.Serializable class network.arca.sdk.** {
    *** Companion;
    *** serializer(...);
}

# kotlinx-serialization-core and okhttp ship their own embedded consumer rules;
# nothing further is required for the SDK's dependencies.
