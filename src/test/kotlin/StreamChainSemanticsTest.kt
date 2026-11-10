import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.core.Observable
import org.junit.jupiter.api.Test
import java.util.Collections
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Pins the documented contract of `chain`: sequential processing, i.e. each
 * emitted function's full pass over the stream completes before the next
 * function starts, and inner streams never interleave.
 *
 * These tests are deterministic: the ordering assertions rely only on the
 * operator contract (concatMap), never on wall-clock timing.
 */
class StreamChainSemanticsTest {

    // =================================================================
    // Observable
    // =================================================================

    @Test
    fun `observable chain applies each function to the whole stream in order`() {
        val f1: (Int) -> String = { "f1:$it" }
        val f2: (Int) -> String = { "f2:$it" }
        val stream = Observable.range(1, 3)

        val result = Observable.just(f1, f2)
            .chain(stream)
            .toList()
            .blockingGet()

        assertEquals(
            listOf("f1:1", "f1:2", "f1:3", "f2:1", "f2:2", "f2:3"),
            result,
            "chain must process each function's full pass before the next function"
        )
    }

    @Test
    fun `observable chain serializes inner streams - no interleaved subscriptions`() {
        val log: MutableList<String> = Collections.synchronizedList(mutableListOf<String>())

        val stream = Observable.range(1, 2)
            .delay(100, TimeUnit.MILLISECONDS)
            .doOnSubscribe { log.add("stream-subscribed") }
            .doOnComplete { log.add("stream-completed") }

        val f1: (Int) -> Int = { it }
        val f2: (Int) -> Int = { it }

        Observable.just(f1, f2)
            .chain(stream)
            .toList()
            .test()
            .awaitDone(5, TimeUnit.SECONDS)
            .assertComplete()
            .assertNoErrors()

        val subscribedCount = log.count { it == "stream-subscribed" }
        val firstComplete = log.indexOf("stream-completed")
        val lastSubscribe = log.lastIndexOf("stream-subscribed")

        assertEquals(2, subscribedCount, "stream must be subscribed once per function")
        assertTrue(
            firstComplete < lastSubscribe,
            "second subscription must start only after the first inner pass completed; log=$log"
        )
    }

    // =================================================================
    // Flowable
    // =================================================================

    @Test
    fun `flowable chain applies each function to the whole stream in order`() {
        val f1: (Int) -> String = { "f1:$it" }
        val f2: (Int) -> String = { "f2:$it" }
        val stream = Flowable.range(1, 3)

        val result = Flowable.just(f1, f2)
            .chain(stream)
            .toList()
            .blockingGet()

        assertEquals(
            listOf("f1:1", "f1:2", "f1:3", "f2:1", "f2:2", "f2:3"),
            result,
            "chain must process each function's full pass before the next function"
        )
    }

    @Test
    fun `flowable chain serializes inner streams - no interleaved subscriptions`() {
        val log: MutableList<String> = Collections.synchronizedList(mutableListOf<String>())

        val stream = Flowable.range(1, 2)
            .delay(100, TimeUnit.MILLISECONDS)
            .doOnSubscribe { log.add("stream-subscribed") }
            .doOnComplete { log.add("stream-completed") }

        val f1: (Int) -> Int = { it }
        val f2: (Int) -> Int = { it }

        Flowable.just(f1, f2)
            .chain(stream)
            .toList()
            .test()
            .awaitDone(5, TimeUnit.SECONDS)
            .assertComplete()
            .assertNoErrors()

        val subscribedCount = log.count { it == "stream-subscribed" }
        val firstComplete = log.indexOf("stream-completed")
        val lastSubscribe = log.lastIndexOf("stream-subscribed")

        assertEquals(2, subscribedCount, "stream must be subscribed once per function")
        assertTrue(
            firstComplete < lastSubscribe,
            "second subscription must start only after the first inner pass completed; log=$log"
        )
    }
}
