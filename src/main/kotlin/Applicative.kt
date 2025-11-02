import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import utils.curried

/**
 * Represents a Kleisli function: takes a value of type [A]
 * and returns a `Single` of [B].
 */
typealias Kleisli<A, B> = (A) -> Single<B>

/**
 * Variant of Kleisli that accepts nullable input values.
 */
typealias KleisliNull<A, B> = (A?) -> Single<B>

/**
 * **Applicative (pure)**.
 *
 * Applies the function contained in this `Single` to a value contained in another `Single`,
 * combining them in parallel using `Single.zip`.
 */
@JvmName("apPure")
infix fun <A : Any, B : Any> Single<(A) -> B>.concurrent(applicativeValue: Single<A>): Single<B> =
    Single.zip(this, applicativeValue) { f, a -> f(a) }

/**
 * **Applicative (Kleisli)**.
 *
 * Applies a function of type `(A) -> Single<B>` contained in this `Single`
 * to a value contained in another `Single`. Combines in parallel and flattens.
 */
@JvmName("apKleisli")
infix fun <A : Any, B : Any> Single<Kleisli<A, B>>.concurrent(applicativeValue: Single<A>): Single<B> =
    Single.zip(this, applicativeValue) { f, a -> f(a) }.join()

/**
 * **Applicative with optional input**.
 */
@JvmName("apNull")
infix fun <A : Any, B : Any> Single<(A?) -> B>.concurrent(applicativeValue: Single<A>?): Single<B> =
    applicativeValue?.let { Single.zip(this, it) { f, a -> f(a) } }
        ?: this.map { it(null) }

/**
 * **Applicative Kleisli with optional input**.
 */
@JvmName("apNullKleisli")
infix fun <A : Any, B : Any> Single<KleisliNull<A, B>>.concurrent(applicativeValue: Single<A>?): Single<B> =
    applicativeValue?.let { Single.zip(this, it) { f, a -> f(a) }.join() }
        ?: this.flatMap { it(null) }

/**
 * **Monad (pure)**.
 */
@JvmName("flatApPure")
infix fun <A : Any, B : Any> Single<(A) -> B>.sequential(applicativeValue: Single<A>): Single<B> =
    this.flatMap { f -> applicativeValue.map(f) }

/**
 * **Monad (Kleisli)**.
 */
@JvmName("flatApKleisli")
infix fun <A : Any, B : Any> Single<Kleisli<A, B>>.sequential(applicativeValue: Single<A>): Single<B> =
    this.flatMap { f -> applicativeValue.flatMap(f) }

/**
 * **Monad with optional input**.
 */
@JvmName("flatApNull")
infix fun <A : Any, B : Any> Single<(A?) -> B>.sequential(applicativeValue: Single<A>?): Single<B> =
    applicativeValue?.let { this.flatMap { f -> it.map(f) } }
        ?: this.map { it(null) }

/**
 * **Monad Kleisli with optional input**.
 */
@JvmName("flatApNullKleisli")
infix fun <A : Any, B : Any> Single<KleisliNull<A, B>>.sequential(applicativeValue: Single<A>?): Single<B> =
    applicativeValue?.let { this.flatMap { f -> it.flatMap(f) } }
        ?: this.flatMap { it(null) }

/**
 * Flattens a `Single<Single<A>>` into a `Single<A>`.
 */
fun <A : Any> Single<Single<A>>.join(): Single<A> =
    this.flatMap { it }

/**
 * Sequential composition of Singles (ignore left, keep right).
 */
infix fun <A : Any, B : Any> Single<A>.then(sb: Single<B>): Single<B> =
    this.flatMap { sb }

/**
 * Compose two Kleisli arrows sequentially.
 */
infix fun <A : Any, B : Any, C : Any> Kleisli<A, B>.andThenK(g: Kleisli<B, C>): Kleisli<A, C> =
    { a -> this(a).flatMap(g) }

/**
 * Run a Kleisli arrow with an explicit input.
 */
infix fun <A : Any, B : Any> Kleisli<A, B>.runK(a: A): Single<B> =
    this(a)

/**
 * Run a Kleisli arrow with `Unit` input (no parameters).
 */
fun <B : Any> Kleisli<Unit, B>.runK(): Single<B> =
    this(Unit)

/**
 * Applicative lifting of a binary function.
 */
fun <A : Any, B : Any, C : Any> liftA2(
    f: (A, B) -> C,
    sa: Single<A>,
    sb: Single<B>
): Single<C> =
    Single.zip(sa, sb) { a, b -> f(a, b) }

// =================================================================
// OBSERVABLE STREAM PROCESSING DSL IMPLEMENTATION
// =================================================================

typealias StreamProcessor<A, B> = (A) -> Observable<B>

/**
 * **Stream Combiner (combineLatest)**.
 * Re-evaluates function whenever any input stream emits.
 */
@JvmName("combineStreams")
infix fun <A : Any, B : Any> Observable<(A) -> B>.combine(stream: Observable<A>): Observable<B> =
    Observable.combineLatest(this, stream) { f, a -> f(a) }

/**
 * **Stream Zipper (zip)**.
 * Waits for both streams to emit, then applies function once per pair.
 */
@JvmName("zipStreams")
infix fun <A : Any, B : Any> Observable<(A) -> B>.pair(stream: Observable<A>): Observable<B> =
    Observable.zip(this, stream) { f, a -> f(a) }

/**
 * **Stream Chain (flatMap)**.
 * Sequential processing - waits for upstream to emit before processing downstream.
 */
@JvmName("chainStreams")
infix fun <A : Any, B : Any> Observable<(A) -> B>.chain(stream: Observable<A>): Observable<B> =
    this.flatMap { f -> stream.map(f) }

/**
 * **Stream Processor Chain (flatMap with stream functions)**.
 */
@JvmName("chainProcessors")
infix fun <A : Any, B : Any> Observable<StreamProcessor<A, B>>.chain(stream: Observable<A>): Observable<B> =
    this.flatMap { processor -> stream.flatMap(processor) }

@JvmName("flowableCombine")
infix fun <A : Any, B : Any> Flowable<(A) -> B>.combine(stream: Flowable<A>): Flowable<B> =
    Flowable.combineLatest(this, stream) { f, a -> f(a) }

@JvmName("flowablePair")
infix fun <A : Any, B : Any> Flowable<(A) -> B>.pair(stream: Flowable<A>): Flowable<B> =
    Flowable.zip(this, stream) { f, a -> f(a) }

@JvmName("flowableChain")
infix fun <A : Any, B : Any> Flowable<(A) -> B>.chain(stream: Flowable<A>): Flowable<B> =
    this.flatMap { f -> stream.map(f) }