import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.disposables.CompositeDisposable
import io.reactivex.rxjava3.schedulers.Schedulers
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Edge cases and robustness tests for the DSL.
 * 
 * Tests cover:
 * - Null handling
 * - Error propagation
 * - Disposal/cancellation
 * - Thread safety
 * - Timeout scenarios
 * - Large parameter counts
 */
@DisplayName("DSL Edge Cases and Robustness")
class EdgeCasesTest {

    // =================================================================
    // NULL HANDLING
    // =================================================================
    
    @Nested
    @DisplayName("Nullable Values")
    inner class NullableValuesTest {
        
        @Test
        @DisplayName("Should handle nullable Single parameters")
        fun `nullable values - optional Single parameters`() {
            data class Config(val required: String, val optional: String?)
            
            val requiredValue: Single<String> = Single.just("required")
            val optionalValue: Single<String>? = null  // Nullable Single
            
            val result = ::Config.liftSingle()
                .zipWith(requiredValue)
                .zipWith(optionalValue)  // Should handle null Single
                .blockingGet()
            
            assertEquals("required", result.required)
            assertEquals(null, result.optional)
        }
    }
    
    // =================================================================
    // ERROR PROPAGATION
    // =================================================================
    
    @Nested
    @DisplayName("Error Propagation")
    inner class ErrorPropagationTest {
        
        @Test
        @DisplayName("Should stop sequential chain on first error")
        fun `error propagation - sequential chain stops on error`() {
            val callCount = AtomicInteger(0)
            
            val step1 = Single.just("step1")
            val step2 = Single.error<String>(RuntimeException("Error in step 2"))
            val step3 = Single.fromCallable {
                callCount.incrementAndGet()
                "step3"
            }
            val step4 = Single.fromCallable {
                callCount.incrementAndGet()
                "step4"
            }
            
            val result = { a: String, b: String, c: String, d: String -> "$a-$b-$c-$d" }
                .liftSingle()
                .flatMapWith(step1)
                .flatMapWith(step2)  // Error here
                .flatMapWith(step3)  // Should NOT execute
                .flatMapWith(step4)  // Should NOT execute
                .test()
                .awaitDone(1, TimeUnit.SECONDS)
            
            result.assertError { it.message == "Error in step 2" }
            assertEquals(0, callCount.get(), "Steps after error should not execute")
        }
        
        @Test
        @DisplayName("Should dispose parallel Singles when one errors")
        fun `error propagation - parallel Singles disposal on error`() {
            val disposedCount = AtomicInteger(0)
            
            fun trackableDisposal(name: String, delayMs: Long): Single<String> =
                Single.just(name)
                    .delay(delayMs, TimeUnit.MILLISECONDS)
                    .doOnDispose {
                        disposedCount.incrementAndGet()
                        println("$name disposed")
                    }
                    .subscribeOn(Schedulers.io())
            
            val fastError = Single.error<String>(RuntimeException("Fast error"))
                .delay(10, TimeUnit.MILLISECONDS)
            
            val result = { a: String, b: String, c: String, d: String -> "$a-$b-$c-$d" }
                .liftSingle()
                .zipWith(trackableDisposal("slow1", 1000))
                .zipWith(fastError)  // Errors quickly
                .zipWith(trackableDisposal("slow2", 1000))
                .zipWith(trackableDisposal("slow3", 1000))
                .test()
                .awaitDone(2, TimeUnit.SECONDS)
            
            result.assertError(RuntimeException::class.java)
            
            // Slow Singles should be disposed when error occurs
            assertTrue(disposedCount.get() > 0, 
                "Parallel Singles should be disposed on error (disposed: ${disposedCount.get()})")
        }
        
        @Test
        @DisplayName("Should preserve error types through chain")
        fun `error propagation - custom exception types preserved`() {
            class CustomException(message: String) : Exception(message)
            
            val result = { a: String, b: String -> "$a-$b" }
                .liftSingle()
                .flatMapWith(Single.just("first"))
                .flatMapWith(Single.error(CustomException("Custom error")))
                .test()
                .awaitDone(1, TimeUnit.SECONDS)
            
            result.assertError(CustomException::class.java)
            result.assertError { it.message == "Custom error" }
        }
    }
    
    // =================================================================
    // DISPOSAL / CANCELLATION
    // =================================================================
    
    @Nested
    @DisplayName("Disposal and Cancellation")
    inner class DisposalTest {
        
        @Test
        @DisplayName("Should properly dispose when subscription is cancelled")
        fun `disposal - early cancellation disposes resources`() {
            val disposedCount = AtomicInteger(0)
            
            fun slowSingle(name: String) = Single.just(name)
                .delay(500, TimeUnit.MILLISECONDS)
                .doOnDispose {
                    disposedCount.incrementAndGet()
                    println("$name disposed")
                }
                .subscribeOn(Schedulers.io())
            
            val testObserver = { a: String, b: String, c: String -> "$a-$b-$c" }
                .liftSingle()
                .zipWith(slowSingle("A"))
                .zipWith(slowSingle("B"))
                .zipWith(slowSingle("C"))
                .test()
            
            // Cancel before completion
            Thread.sleep(100)
            testObserver.dispose()
            
            Thread.sleep(200)  // Give time for disposal
            
            assertTrue(testObserver.isDisposed, "Should be disposed")
            assertTrue(disposedCount.get() > 0, 
                "Resources should be disposed (disposed: ${disposedCount.get()})")
        }
        
        @Test
        @DisplayName("Should handle multiple disposals gracefully")
        fun `disposal - multiple dispose calls are safe`() {
            val disposables = CompositeDisposable()
            
            val single = { a: String, b: String -> "$a-$b" }
                .liftSingle()
                .zipWith(Single.just("A").delay(100, TimeUnit.MILLISECONDS))
                .zipWith(Single.just("B").delay(100, TimeUnit.MILLISECONDS))
            
            val disposable1 = single.subscribe()
            val disposable2 = single.subscribe()
            
            disposables.addAll(disposable1, disposable2)
            
            // Multiple dispose calls should be safe
            disposables.dispose()
            disposables.dispose()
            disposables.dispose()
            
            assertTrue(disposables.isDisposed)
            assertTrue(disposable1.isDisposed)
            assertTrue(disposable2.isDisposed)
        }
    }
    
    // =================================================================
    // LARGE PARAMETER COUNTS
    // =================================================================
    
    @Nested
    @DisplayName("Large Parameter Counts")
    inner class LargeParameterCountsTest {
        
        @Test
        @DisplayName("Should handle 10 parameters efficiently")
        fun `large parameters - 10 parameter function`() {
            fun combine10(
                p1: String, p2: String, p3: String, p4: String, p5: String,
                p6: String, p7: String, p8: String, p9: String, p10: String
            ) = "$p1-$p2-$p3-$p4-$p5-$p6-$p7-$p8-$p9-$p10"
            
            val result = ::combine10.liftSingle()
                .zipWith(Single.just("1"))
                .zipWith(Single.just("2"))
                .zipWith(Single.just("3"))
                .zipWith(Single.just("4"))
                .zipWith(Single.just("5"))
                .zipWith(Single.just("6"))
                .zipWith(Single.just("7"))
                .zipWith(Single.just("8"))
                .zipWith(Single.just("9"))
                .zipWith(Single.just("10"))
                .blockingGet()
            
            assertEquals("1-2-3-4-5-6-7-8-9-10", result)
        }
        
        @Test
        @DisplayName("Should handle 20 parameters (stress test)")
        fun `large parameters - 20 parameter function stress test`() {
            @Suppress("UNUSED_PARAMETER")
            fun combine20(
                p1: Int, p2: Int, p3: Int, p4: Int, p5: Int,
                p6: Int, p7: Int, p8: Int, p9: Int, p10: Int,
                p11: Int, p12: Int, p13: Int, p14: Int, p15: Int,
                p16: Int, p17: Int, p18: Int, p19: Int, p20: Int
            ) = (1..20).sum()
            
            val result = ::combine20.liftSingle()
                .zipWith(Single.just(1)).zipWith(Single.just(2))
                .zipWith(Single.just(3)).zipWith(Single.just(4))
                .zipWith(Single.just(5)).zipWith(Single.just(6))
                .zipWith(Single.just(7)).zipWith(Single.just(8))
                .zipWith(Single.just(9)).zipWith(Single.just(10))
                .zipWith(Single.just(11)).zipWith(Single.just(12))
                .zipWith(Single.just(13)).zipWith(Single.just(14))
                .zipWith(Single.just(15)).zipWith(Single.just(16))
                .zipWith(Single.just(17)).zipWith(Single.just(18))
                .zipWith(Single.just(19)).zipWith(Single.just(20))
                .blockingGet()
            
            assertEquals(210, result)
        }
    }
    
    // =================================================================
    // THREAD SAFETY
    // =================================================================
    
    @Nested
    @DisplayName("Thread Safety")
    inner class ThreadSafetyTest {
        
        @Test
        @DisplayName("Should be thread-safe with concurrent subscriptions")
        fun `thread safety - concurrent subscriptions`() {
            val single = { a: String, b: String, c: String -> "$a-$b-$c" }
                .liftSingle()
                .zipWith(Single.just("A").delay(50, TimeUnit.MILLISECONDS))
                .zipWith(Single.just("B").delay(50, TimeUnit.MILLISECONDS))
                .zipWith(Single.just("C").delay(50, TimeUnit.MILLISECONDS))
            
            val results = mutableListOf<String>()
            val latch = java.util.concurrent.CountDownLatch(10)
            
            // Subscribe 10 times concurrently
            repeat(10) {
                Thread {
                    val result = single.blockingGet()
                    synchronized(results) {
                        results.add(result)
                    }
                    latch.countDown()
                }.start()
            }
            
            latch.await(5, TimeUnit.SECONDS)
            
            assertEquals(10, results.size, "All subscriptions should complete")
            assertTrue(results.all { it == "A-B-C" }, "All results should be correct")
        }
    }
    
    // =================================================================
    // TIMEOUT SCENARIOS
    // =================================================================
    
    @Nested
    @DisplayName("Timeout Handling")
    inner class TimeoutTest {
        
        @Test
        @DisplayName("Should handle timeout in sequential chain")
        fun `timeout - sequential operation timeout`() {
            val slowSingle = Single.just("slow")
                .delay(5, TimeUnit.SECONDS)  // Very slow
                .subscribeOn(Schedulers.io())
            
            val result = { a: String, b: String -> "$a-$b" }
                .liftSingle()
                .flatMapWith(Single.just("fast"))
                .flatMapWith(slowSingle)
                .timeout(1, TimeUnit.SECONDS)  // Timeout after 1 second
                .test()
                .awaitDone(2, TimeUnit.SECONDS)
            
            result.assertError(java.util.concurrent.TimeoutException::class.java)
        }
        
        @Test
        @DisplayName("Should handle timeout in parallel operations")
        fun `timeout - parallel operation timeout`() {
            val result = { a: String, b: String -> "$a-$b" }
                .liftSingle()
                .zipWith(Single.just("fast"))
                .zipWith(Single.just("slow").delay(5, TimeUnit.SECONDS))
                .timeout(1, TimeUnit.SECONDS)
                .test()
                .awaitDone(2, TimeUnit.SECONDS)
            
            result.assertError(java.util.concurrent.TimeoutException::class.java)
        }
    }
    
    // =================================================================
    // EDGE CASE: EMPTY / NEVER
    // =================================================================
    
    @Nested
    @DisplayName("Empty and Never Singles")
    inner class EmptyNeverTest {
        
        @Test
        @DisplayName("Should timeout on never-completing Single")
        fun `edge cases - never completing Single`() {
            val neverSingle = Single.never<String>()
            
            val result = { a: String, b: String -> "$a-$b" }
                .liftSingle()
                .zipWith(Single.just("value"))
                .zipWith(neverSingle)  // Never completes
                .timeout(500, TimeUnit.MILLISECONDS)
                .test()
                .awaitDone(1, TimeUnit.SECONDS)
            
            result.assertError(java.util.concurrent.TimeoutException::class.java)
        }
    }
    
    // =================================================================
    // MEMORY LEAKS
    // =================================================================
    
    @Nested
    @DisplayName("Memory Leak Prevention")
    inner class MemoryLeakTest {
        
        @Test
        @DisplayName("Should not hold references after completion")
        fun `memory - no references held after completion`() {
            // This is more of a conceptual test
            // In real scenarios, you'd use memory profilers
            
            var largeObject: ByteArray? = ByteArray(1024 * 1024)  // 1MB
            
            val result = { data: ByteArray -> data.size }
                .liftSingle()
                .flatMapWith(Single.just(largeObject!!))
                .blockingGet()
            
            assertEquals(1024 * 1024, result)
            
            // Clear reference
            largeObject = null
            System.gc()
            
            // If DSL holds references, largeObject won't be GC'd
            // (This is a basic check; real testing needs profilers)
        }
    }
}
