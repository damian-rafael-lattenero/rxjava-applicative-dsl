import io.reactivex.rxjava3.core.Single
import org.junit.jupiter.api.Test
import utils.*
import kotlin.test.assertEquals

/**
 * Value-level law tests for the algebra the DSL claims in its KDoc:
 *
 * - Applicative (zipWith = parallel ap, liftSingle = pure)
 * - Monad apply (flatMapWith = sequential ap)
 * - Kleisli arrows (andThenK = >=>, runK = run)
 * - join, then, liftA2
 *
 * Laws are checked on pure (non-erroring) sources with deterministic values;
 * zipWith and flatMapWith differ in execution strategy, not in values.
 *
 * Note: liftSingle lives in the default package (Applicative.kt / liftSingle.kt
 * declare no package), so no import is needed here.
 */
class AlgebraicLawsTest {

    private val f: (Int) -> String = { "f($it)" }
    private val g: (String) -> String = { "g($it)" }
    private val h: (String) -> String = { "h($it)" }

    // =================================================================
    // APPLICATIVE LAWS (zipWith, liftSingle = pure)
    // =================================================================

    @Test
    fun `applicative identity - pure(id) applied to v is v`() {
        val id: (Int) -> Int = { it }
        val v = Single.just(42)

        assertEquals(v.blockingGet(), Single.just(id).zipWith(v).blockingGet())
    }

    @Test
    fun `applicative homomorphism - pure(f) applied to pure(x) is pure(f(x))`() {
        val left = Single.just(f).zipWith(Single.just(7))
        val right = Single.just(f(7))

        assertEquals(right.blockingGet(), left.blockingGet())
    }

    @Test
    fun `applicative interchange - pure(f) applied to pure(x) equals pure(apply-to-x) applied to pure(f)`() {
        val applyToX: ((Int) -> String) -> String = { ff -> ff(9) }

        val left = Single.just(f).zipWith(Single.just(9))
        val right = Single.just(applyToX).zipWith(Single.just(f))

        assertEquals(left.blockingGet(), right.blockingGet())
    }

    @Test
    fun `applicative composition - pure(compose) applied to u v w equals u applied to (v applied to w)`() {
        val compose: ((String) -> String) -> ((Int) -> String) -> (Int) -> String =
            { b: (String) -> String -> { a: (Int) -> String -> { x: Int -> b(a(x)) } } }

        val u: Single<(String) -> String> = Single.just(g)
        val v: Single<(Int) -> String> = Single.just(f)
        val w: Single<Int> = Single.just(3)

        val left = Single.just(compose)
            .zipWith(u)
            .zipWith(v)
            .zipWith(w)

        val right = u.zipWith(v.zipWith(w))

        assertEquals(right.blockingGet(), left.blockingGet())
    }

    // =================================================================
    // MONADIC APPLY: zipWith vs flatMapWith agree on values
    // =================================================================

    @Test
    fun `zipWith and flatMapWith produce the same values for pure sources`() {
        val sa = Single.just(5)

        assertEquals(
            Single.just(f).zipWith(sa).blockingGet(),
            Single.just(f).flatMapWith(sa).blockingGet()
        )
    }

    // =================================================================
    // KLEISLI LAWS (andThenK = >=>)
    // =================================================================

    @Test
    fun `kleisli composition is associative`() {
        val kf: Kleisli<Int, String> = { Single.just(f(it)) }
        val kg: Kleisli<String, String> = { Single.just(g(it)) }
        val kh: Kleisli<String, String> = { Single.just(h(it)) }

        val left = (kf andThenK kg) andThenK kh
        val right = kf andThenK (kg andThenK kh)

        assertEquals(right.runK(1).blockingGet(), left.runK(1).blockingGet())
    }

    @Test
    fun `kleisli right identity - f andThenK pure is f`() {
        val kf: Kleisli<Int, String> = { Single.just(f(it)) }
        val pure: Kleisli<String, String> = { Single.just(it) }

        assertEquals(kf.runK(1).blockingGet(), (kf andThenK pure).runK(1).blockingGet())
    }

    // =================================================================
    // join / then / liftA2
    // =================================================================

    @Test
    fun `join flattens nested Single to its value`() {
        assertEquals(7, Single.just(Single.just(7)).join().blockingGet())
    }

    @Test
    fun `then keeps the right value when the left succeeds and propagates left errors`() {
        assertEquals("right", Single.just(1).then(Single.just("right")).blockingGet())

        val left = Single.error<Int>(IllegalStateException("left failed"))
        val result = left.then(Single.just("right")).test()
        result.assertError { it is IllegalStateException && it.message == "left failed" }
    }

    @Test
    fun `liftA2 agrees with map-then-zipWith`() {
        val combine: (Int, String) -> String = { i: Int, s: String -> "$i-$s" }

        val viaLiftA2: Single<String> = liftA2(combine, Single.just(1), Single.just("a"))
        val curried: (Int) -> (String) -> String = { i: Int -> { s: String -> combine(i, s) } }
        val viaZipWith: Single<String> = Single.just(curried).zipWith(Single.just(1)).zipWith(Single.just("a"))

        assertEquals(viaZipWith.blockingGet(), viaLiftA2.blockingGet())
    }

    // =================================================================
    // liftSingle (pure) sanity
    // =================================================================

    @Test
    fun `liftSingle wraps a function value without changing it`() {
        val lifted = ::parseIntText.liftSingle()
        assertEquals(12, lifted.zipWith(Single.just("12")).blockingGet())
    }

    private fun parseIntText(text: String): Int = text.toInt()
}
