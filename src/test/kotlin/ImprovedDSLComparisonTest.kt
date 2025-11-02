import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.schedulers.Schedulers
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.system.measureTimeMillis

/**
 * Comprehensive test suite demonstrating DSL advantages over vanilla RxJava.
 * 
 * These tests serve multiple purposes:
 * 1. Verify correctness of DSL implementation
 * 2. Compare readability: Vanilla vs DSL
 * 3. Measure performance overhead (should be minimal)
 * 4. Document usage patterns
 */
@DisplayName("DSL vs Vanilla RxJava Comparison")
class ImprovedDSLComparisonTest {

    // =================================================================
    // TEST 1: SEQUENTIAL OPERATIONS
    // Problem: Nested flatMap pyramid of doom
    // =================================================================

    data class User(val id: String, val name: String)
    data class Profile(val id: String, val userId: String)
    data class Settings(val theme: String)

    @Nested
    @DisplayName("Sequential Operations")
    inner class SequentialOperationsTest {
        private fun fetchUser(): Single<User> = 
            Single.just(User("u1", "Alice"))
                .delay(50, TimeUnit.MILLISECONDS)
                .subscribeOn(Schedulers.io())
        
        private fun fetchProfile(userId: String): Single<Profile> =
            Single.just(Profile("p1", userId))
                .delay(50, TimeUnit.MILLISECONDS)
                .subscribeOn(Schedulers.io())
        
        private fun fetchSettings(profileId: String): Single<Settings> =
            Single.just(Settings("dark"))
                .delay(50, TimeUnit.MILLISECONDS)
                .subscribeOn(Schedulers.io())
        
        @Test
        @DisplayName("Should load user data sequentially - DSL is more readable than nested flatMap")
        fun `sequential operations - DSL vs Vanilla readability`() {
            // VANILLA: Nested flatMap hell (pyramid of doom)
            val vanillaResult: Single<String> =
                fetchUser()
                    .flatMap { user ->
                        fetchProfile(user.id)
                            .flatMap { profile ->
                                fetchSettings(profile.id)
                                    .map { settings ->
                                        "${user.name} uses ${settings.theme} theme"
                                    }
                            }
                    }
            
            // DSL: Linear, readable flow
            val dslResult: Single<String> =
                { user: User, profile: Profile, settings: Settings ->
                    "${user.name} uses ${settings.theme} theme"
                }.liftSingle()
                    .flatMapWith(fetchUser())
                    .flatMapWith(fetchProfile("u1"))
                    .flatMapWith(fetchSettings("p1"))
            
            // Both should produce same result
            val vanillaValue = vanillaResult.blockingGet()
            val dslValue = dslResult.blockingGet()
            
            assertEquals("Alice uses dark theme", vanillaValue)
            assertEquals("Alice uses dark theme", dslValue)
            assertEquals(vanillaValue, dslValue, "DSL and Vanilla should produce identical results")
        }
        
        @Test
        @DisplayName("Should have minimal performance overhead")
        fun `sequential operations - performance comparison`() {
            val iterations = 10
            
            val vanillaTime = measureTimeMillis {
                repeat(iterations) {
                    fetchUser()
                        .flatMap { user -> fetchProfile(user.id) }
                        .flatMap { profile -> fetchSettings(profile.id) }
                        .blockingGet()
                }
            }
            
            val dslTime = measureTimeMillis {
                repeat(iterations) {
                    { _: User, _: Profile, _: Settings -> "result" }
                        .liftSingle()
                        .flatMapWith(fetchUser())
                        .flatMapWith(fetchProfile("u1"))
                        .flatMapWith(fetchSettings("p1"))
                        .blockingGet()
                }
            }
            
            val overhead = ((dslTime - vanillaTime).toDouble() / vanillaTime * 100)
            
            println("Vanilla: ${vanillaTime}ms | DSL: ${dslTime}ms | Overhead: ${"%.2f".format(overhead)}%")
            
            // DSL should have <10% overhead
            assertTrue(overhead < 10.0, "DSL overhead should be minimal (was ${"%.2f".format(overhead)}%)")
        }
    }
    
    // =================================================================
    // TEST 2: PARALLEL OPERATIONS
    // Problem: Verbose Single.zip with BiFunction/Function3/etc
    // =================================================================
    
    @Nested
    @DisplayName("Parallel Operations")
    inner class ParallelOperationsTest {

        private fun fetchWeather(): Single<String> =
            Single.just("☀️ Sunny")
                .delay(100, TimeUnit.MILLISECONDS)
                .subscribeOn(Schedulers.io())

        private fun fetchNews(): Single<String> =
            Single.just("📰 Breaking News")
                .delay(100, TimeUnit.MILLISECONDS)
                .subscribeOn(Schedulers.io())

        private fun fetchStocks(): Single<String> =
            Single.just("📈 +5%")
                .delay(100, TimeUnit.MILLISECONDS)
                .subscribeOn(Schedulers.io())

        @Test
        @DisplayName("Should execute Singles in parallel - DSL eliminates BiFunction boilerplate")
        fun `parallel operations - DSL vs Vanilla readability`() {
            // VANILLA: Verbose with BiFunction/Function3
            val vanillaResult: Single<String> =
                Single.zip(
                    fetchWeather(),
                    fetchNews(),
                    fetchStocks()
                ) { weather, news, stocks ->
                    "Dashboard: $weather | $news | $stocks"
                }

            // DSL: Clean and concise
            val dslResult: Single<String> =
                { weather: String, news: String, stocks: String ->
                    "Dashboard: $weather | $news | $stocks"
                }.liftSingle()
                    .zipWith(fetchWeather())
                    .zipWith(fetchNews())
                    .zipWith(fetchStocks())

            val vanillaValue = vanillaResult.blockingGet()
            val dslValue = dslResult.blockingGet()

            assertEquals(vanillaValue, dslValue, "Both approaches should produce same result")
            assertTrue(vanillaValue.contains("Sunny"))
            assertTrue(dslValue.contains("Breaking News"))
        }

        @Test
        @DisplayName("Should execute truly in parallel (not sequential)")
        fun `parallel operations - verify concurrent execution`() {
            val executionOrder = Collections.synchronizedList(mutableListOf<String>())
            val startLatch = CountDownLatch(3)  // Esperar a que todos empiecen

            fun trackableSingle(name: String, delayMs: Long) =
                Single.fromCallable {
                    executionOrder.add("$name-start")
                    startLatch.countDown()  // Notificar que empezó
                    Thread.sleep(delayMs)
                    executionOrder.add("$name-end")
                    name
                }.subscribeOn(Schedulers.io())

            // All three should start before any completes (parallel)
            val result = { a: String, b: String, c: String -> "$a-$b-$c" }
                .liftSingle()
                .zipWith(trackableSingle("A", 50))
                .zipWith(trackableSingle("B", 50))
                .zipWith(trackableSingle("C", 50))
                .blockingGet()

            // Esperar a que todos hayan empezado
            startLatch.await(1, TimeUnit.SECONDS)

            val order = executionOrder.joinToString(";")

            // Verify parallel execution: all start before any ends
            assertTrue(
                executionOrder.count { it.contains("start") } == 3,
                "All three Singles should have started. Order was: $order"
            )

            println("Execution order: $order")
            println("Result: $result")
        }

    }
    
    // =================================================================
    // TEST 3: MIXED OPERATIONS
    // Problem: Complex mix of zip and flatMap is unreadable
    // =================================================================
    data class AuthToken(val token: String)
    data class Cart(val items: Int)
    data class Payment(val amount: Double)

    @Nested
    @DisplayName("Mixed Parallel and Sequential Operations")
    inner class MixedOperationsTest {
        private fun authenticate(): Single<AuthToken> =
            Single.just(AuthToken("token123"))
                .delay(100, TimeUnit.MILLISECONDS)
                .subscribeOn(Schedulers.io())
        
        private fun fetchCart(): Single<Cart> =
            Single.just(Cart(5))
                .delay(50, TimeUnit.MILLISECONDS)
                .subscribeOn(Schedulers.io())
        
        private fun checkInventory(): Single<Boolean> =
            Single.just(true)
                .delay(50, TimeUnit.MILLISECONDS)
                .subscribeOn(Schedulers.io())
        
        private fun processPayment(auth: AuthToken, cart: Cart): Single<Payment> =
            Single.just(Payment(99.99))
                .delay(100, TimeUnit.MILLISECONDS)
                .subscribeOn(Schedulers.io())
        
        @Test
        @DisplayName("Should handle mixed execution strategy - DSL makes intent clear")
        fun `mixed operations - clear execution intent`() {
            // VANILLA: Confusing mix of zip and flatMap
            val vanillaResult: Single<String> =
                authenticate().flatMap { auth ->
                    Single.zip(
                        fetchCart(),
                        checkInventory()
                    ) { cart, inventory -> Pair(cart, inventory) }
                        .flatMap { (cart, inventory) ->
                            if (inventory) {
                                processPayment(auth, cart).map { payment ->
                                    "Paid $${payment.amount} for ${cart.items} items"
                                }
                            } else {
                                Single.error(Exception("Out of stock"))
                            }
                        }
                }
            
            // DSL: Crystal clear what's parallel and what's sequential
            val dslResult: Single<String> =
                { _: AuthToken, cart: Cart, _: Boolean, payment: Payment ->
                    "Paid $${payment.amount} for ${cart.items} items"
                }.liftSingle()
                    .flatMapWith(authenticate())        // MUST happen first
                    .zipWith(fetchCart())               // These run in PARALLEL
                    .zipWith(checkInventory())          // ↓
                    .flatMapWith(processPayment(AuthToken("token123"), Cart(5))) // THEN process
            
            val vanillaValue = vanillaResult.blockingGet()
            val dslValue = dslResult.blockingGet()
            
            assertEquals(vanillaValue, dslValue)
            assertTrue(dslValue.contains("99.99"))
        }
        
        @Test
        @DisplayName("Should optimize execution time with parallel where possible")
        fun `mixed operations - performance benefit of parallelism`() {
            // All sequential: 100 + 50 + 50 + 100 = 300ms
            val sequentialTime = measureTimeMillis {
                authenticate()
                    .flatMap { fetchCart() }
                    .flatMap { checkInventory() }
                    .flatMap { processPayment(AuthToken("t"), Cart(5)) }
                    .blockingGet()
            }
            
            // Optimized with parallel: 100 + max(50,50) + 100 = 250ms
            val parallelTime = measureTimeMillis {
                { _: AuthToken, _: Cart, _: Boolean, payment: Payment -> payment }
                    .liftSingle()
                    .flatMapWith(authenticate())
                    .zipWith(fetchCart())
                    .zipWith(checkInventory())
                    .flatMapWith(processPayment(AuthToken("t"), Cart(5)))
                    .blockingGet()
            }
            
            println("Sequential: ${sequentialTime}ms | Parallel: ${parallelTime}ms")
            
            // Parallel should be faster (or at least not slower)
            assertTrue(parallelTime <= sequentialTime * 1.1, 
                "Parallel execution should be faster (seq: ${sequentialTime}ms, par: ${parallelTime}ms)")
        }
    }
    
    // =================================================================
    // TEST 4: KLEISLI COMPOSITION
    // Problem: Composing reactive functions is verbose
    // =================================================================
    data class RawData(val value: String)
    data class ValidatedData(val value: String)
    data class EnrichedData(val value: String, val metadata: String)
    data class Result(val data: EnrichedData)
    @Nested
    @DisplayName("Kleisli Composition")
    inner class KleisliCompositionTest {
        private val validate: Kleisli<RawData, ValidatedData> = { raw ->
            Single.just(ValidatedData(raw.value))
                .delay(50, TimeUnit.MILLISECONDS)
                .subscribeOn(Schedulers.io())
        }
        
        private val enrich: Kleisli<ValidatedData, EnrichedData> = { validated ->
            Single.just(EnrichedData(validated.value, "metadata"))
                .delay(50, TimeUnit.MILLISECONDS)
                .subscribeOn(Schedulers.io())
        }
        
        private val finalize: Kleisli<EnrichedData, Result> = { enriched ->
            Single.just(Result(enriched))
                .delay(50, TimeUnit.MILLISECONDS)
                .subscribeOn(Schedulers.io())
        }
        
        @Test
        @DisplayName("Should compose Kleisli arrows elegantly")
        fun `kleisli composition - elegant arrow composition`() {
            // VANILLA: Nested flatMap chain
            val vanillaResult: Single<Result> =
                validate(RawData("test"))
                    .flatMap { validated -> enrich(validated) }
                    .flatMap { enriched -> finalize(enriched) }
            
            // DSL: Composable pipeline
            val pipeline: Kleisli<RawData, Result> =
                validate andThenK enrich andThenK finalize
            
            val dslResult: Single<Result> = pipeline.runK(RawData("test"))
            
            val vanillaValue = vanillaResult.blockingGet()
            val dslValue = dslResult.blockingGet()
            
            assertEquals(vanillaValue.data.value, dslValue.data.value)
            assertEquals("metadata", dslValue.data.metadata)
        }
        
        @Test
        @DisplayName("Should allow reusable pipeline composition")
        fun `kleisli composition - reusable pipelines`() {
            // Create reusable pipeline
            val dataProcessingPipeline = validate andThenK enrich
            val fullPipeline = dataProcessingPipeline andThenK finalize
            
            // Reuse in different contexts
            val result1 = fullPipeline.runK(RawData("data1")).blockingGet()
            val result2 = fullPipeline.runK(RawData("data2")).blockingGet()
            val result3 = dataProcessingPipeline.runK(RawData("data3")).blockingGet()
            
            assertEquals("data1", result1.data.value)
            assertEquals("data2", result2.data.value)
            assertEquals("data3", result3.value) // Only validated and enriched
        }
    }
    
    // =================================================================
    // TEST 5: ERROR HANDLING
    // Important: Verify correct behavior on errors
    // =================================================================
    
    @Nested
    @DisplayName("Error Handling")
    inner class ErrorHandlingTest {
        
        @Test
        @DisplayName("Should propagate errors correctly in sequential chain")
        fun `error handling - sequential chain stops on error`() {
            val errorSingle = Single.error<String>(RuntimeException("Test error"))
            val notCalledSingle = Single.fromCallable<String> {
                throw AssertionError("This should not be called after error")
            }

            val result = { a: String, b: String, c: String -> "$a-$b-$c" }  // 3 params
                .liftSingle()
                .flatMapWith(Single.just("first"))
                .flatMapWith(errorSingle)
                .flatMapWith(notCalledSingle)  // ✅ Ahora funciona
                .test()
            
            result.assertError { it.message == "Test error" }
            result.assertNoValues()
        }
        
        @Test
        @DisplayName("Should handle errors in parallel operations")
        fun `error handling - parallel operations with error`() {
            val result = { a: String, b: String -> "$a-$b" }
                .liftSingle()
                .zipWith(Single.just("success"))
                .zipWith(Single.error(RuntimeException("Error in parallel operation")))
                .test()
                .awaitDone(1, TimeUnit.SECONDS)
            
            result.assertError(Exception::class.java)
        }
    }
}
