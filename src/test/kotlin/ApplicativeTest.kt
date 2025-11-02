import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.schedulers.Schedulers.io
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class ApplicativeTest {

    private val bothSubscribed = CountDownLatch(0) // Change this value to 2 to run the test slowly
    private val subscribeThreadsStillRunning = CountDownLatch(0) // Change this value to 1 to run the test slowly

    private fun <T: Any> createSingle(value: T): Single<T> =
        Observable
            .create { emitter ->
                println("Subscribe $value on ${Thread.currentThread().name}")
                bothSubscribed.countDown()
                subscribeThreadsStillRunning.await(5, TimeUnit.SECONDS)
                emitter.onNext(value)
                emitter.onComplete()
            }
            .singleOrError()
            .subscribeOn(io())

    @JvmInline value class AppId(val value: String)
    @JvmInline value class AppName(val value: String)
    @JvmInline value class Description(val value: String)
    @JvmInline value class VersionCode(val value: Int)
    @JvmInline value class Active(val value: Boolean)
    @JvmInline value class Owner(val value: String)
    @JvmInline value class Platform(val value: String)
    @JvmInline value class Region(val value: String)
    @JvmInline value class Language(val value: String)
    @JvmInline value class Framework(val value: String)
    @JvmInline value class Backend(val value: String)
    @JvmInline value class Database(val value: String)
    @JvmInline value class Cache(val value: String)
    @JvmInline value class Queue(val value: String)
    @JvmInline value class Analytics(val value: String)
    @JvmInline value class Ads(val value: String)
    @JvmInline value class Notifications(val value: Boolean) // <-- Boolean
    @JvmInline value class Permissions(val value: Boolean)  // <-- Boolean
    @JvmInline value class DarkMode(val value: Boolean)     // <-- Boolean
    @JvmInline value class StoreUrl(val value: String)


    data class MovilApp(
        val id: AppId,
        val name: AppName,
        val description: Description?,   // nullable wrapper
        val version: VersionCode,
        val active: Active,             // Boolean “pelado”
        val owner: Owner,
        val platform: Platform,
        val region: Region,
        val language: Language,
        val framework: Framework?,       // nullable wrapper
        val backend: Backend,
        val database: Database,
        val cache: Cache?,               // nullable wrapper
        val queue: Queue?,               // nullable wrapper
        val analytics: Analytics?,       // nullable wrapper
        val ads: Ads?,                   // nullable wrapper
        val notifications: Notifications,
        val permissions: Permissions,
        val darkMode: DarkMode,
        val storeUrl: StoreUrl,
    )

    @Test
    fun testMixBothKleisli() {
        // Given

        val s1 = createSingle("a1")
        val s2 = createSingle("a2")
        val s3 = createSingle("a3")
        val s4 = createSingle("a4")
        val s5 = createSingle("a5")
        val s6 = createSingle("a6")
        val s7 = createSingle("a7")
        val s8 = createSingle("a8")
        val s9 = createSingle("a9")
        val s10 = createSingle("a10")
        val s11 = createSingle("a11")

        val stepA: Kleisli<Unit, String> = { _ ->
            ::serviceA.liftSingle()
                .sequential(s1)
                .concurrent(s2)
                .concurrent(s3)
                .sequential(s4)
                .concurrent(s5)
                .concurrent(s6)
                .concurrent(s7)
                .concurrent(s8)
                .sequential(s9)
                .sequential(s10)
                .concurrent(s11)
        }

        val stepB: Kleisli<String, String> = { inputFromA ->
            ::serviceB.liftSingle()
                .sequential(createSingle("$inputFromA::b1"))
                .concurrent(createSingle("b2"))
                .concurrent(createSingle("b3"))
                .sequential(createSingle("b4"))
                .concurrent(createSingle("b5"))
                .concurrent(createSingle("b6"))
                .concurrent(createSingle("b7"))
                .concurrent(createSingle("b8"))
                .sequential(createSingle("b9"))
                .sequential(createSingle("b10"))
                .concurrent(createSingle("b11"))
        }

        val stepC: Kleisli<String, String> = { inputFromB ->
            ::serviceC.liftSingle()
                .sequential(createSingle("$inputFromB::c1"))
                .concurrent(createSingle("c2"))
                .sequential(createSingle("c3"))
        }

        val stepD: Single<MovilApp> =
            ::MovilApp.liftSingle()
                .sequential(createSingle(AppId("id-123")))
                .sequential(createSingle(AppName("Mi App")))
                .sequential(createSingle(Description("Una app de ejemplo")))
                .sequential(createSingle(VersionCode(42)))
                .sequential(Single.just(Active(true)))
                .sequential(createSingle(Owner("Damian")))
                .concurrent(createSingle(Platform("Android")))
                .sequential(createSingle(Region("LatAm")))
                .concurrent(createSingle(Language("es")))
                .concurrent(createSingle(Framework("Kotlin")))
                .sequential(createSingle(Backend("SpringBoot")))
                .sequential(createSingle(Database("Postgres")))
                .concurrent(createSingle(Cache("Redis")))
                .concurrent(createSingle(Queue("RabbitMQ")))
                .concurrent(createSingle(Analytics("FirebaseAnalytics")))
                .concurrent(createSingle(Ads("AdMob")))
                .concurrent(createSingle(Notifications(true)))
                .concurrent(createSingle(Permissions(false)))
                .concurrent(createSingle(DarkMode(true)))
                .sequential(createSingle(StoreUrl("https://store/app")))
                .subscribeOn(io())

        val pipeline = stepA andThenK stepB andThenK stepC andThenK { stepD.map { d -> "D:$it::$d" } } andThenK { createSingle("$it:END") }
        val result   = pipeline.runK()

        // Then
        result
            .test()
            .awaitDone(50, TimeUnit.SECONDS)
            .assertValues("D:C:B:A:a1;a2;a3;a4;a5;a6;a7;a8;a9;a10;a11::b1;b2;b3;b4;b5;b6;b7;b8;b9;b10;b11::c1;c2;c3::MovilApp(id=AppId(value=id-123), name=AppName(value=Mi App), description=Description(value=Una app de ejemplo), version=VersionCode(value=42), active=Active(value=true), owner=Owner(value=Damian), platform=Platform(value=Android), region=Region(value=LatAm), language=Language(value=es), framework=Framework(value=Kotlin), backend=Backend(value=SpringBoot), database=Database(value=Postgres), cache=Cache(value=Redis), queue=Queue(value=RabbitMQ), analytics=Analytics(value=FirebaseAnalytics), ads=Ads(value=AdMob), notifications=Notifications(value=true), permissions=Permissions(value=false), darkMode=DarkMode(value=true), storeUrl=StoreUrl(value=https://store/app)):END")
    }

    @Test
    fun testZipOver() {
        // Given
        val service: (String, Int, String?, Int, String, String, String, String, String, String, String?) -> Single<String> =
            { s1: String,
              s2: Int,
              s3: String?,
              s4: Int,
              s5: String,
              s6: String,
              s7: String,
              s8: String,
              s9: String,
              s10: String,
              s11: String? ->
                val result =
                    listOf(s1, "$s2", s3 ?: "none", "$s4", s5, s6, s7, s8, s9, s10, s11 ?: "none").joinToString(
                        separator = ";"
                    )
                Single.just("Values:$result")
            }

        val s1: Single<String> = createSingle("v1")
        val s2: Single<Int> = Single.just(2)
        // Here, we move the Nullable value outside, so the whole Single<String> is Nullable, and not the value inside the Single`enter code here`
        val s3: Single<String>? = null
        val s4: Single<Int> = Single.just(4)
        val s5: Single<String> = createSingle("v5")
        val s6: Single<String> = createSingle("v6")
        val s7: Single<String> = createSingle("v7")
        val s8: Single<String> = createSingle("v8")
        val s9: Single<String> = createSingle("v9")
        val s10 = createSingle("v11")
        val s11: Single<String>? = null

        // When
        // Here I curry the function, so I can apply one by one the the arguments via zipOver() and preserve the types

        val result = service
            .liftSingle()
            .concurrent(s1)
            .concurrent(s2)
            .concurrent(s3)
            .concurrent(s4)
            .concurrent(s5)
            .concurrent(s6)
            .concurrent(s7)
            .concurrent(s8)
            .concurrent(s9)
            .concurrent(s10)
            .concurrent(s11)

        // Then
        result
            .test()
            .awaitDone(50, TimeUnit.SECONDS)
            .assertValues("Values:v1;2;none;4;v5;v6;v7;v8;v9;v11;none")
    }

    @Test
    fun testMapOver() {
        // Given

        val service: (String, Int, String?, Int, String, String, String, String, String, String?, String) -> Single<String> =
            { s1: String,
              s2: Int,
              s3: String?,
              s4: Int,
              s5: String,
              s6: String,
              s7: String,
              s8: String,
              s9: String,
              s10: String?,
              s11: String ->
                val result =
                    listOf(s1, "$s2", s3 ?: "none", "$s4", s5, s6, s7, s8, s9, s10 ?: "none", s11).joinToString(
                        separator = ";"
                    )
                Single.just("Values:$result")
            }

        val s1: Single<String> = createSingle("v1")
        val s2: Single<Int> = Single.just(2)
        // Here, we move the Nullable value outside, so the whole Single<String> is Nullable, and not the value inside the Single`enter code here`
        val s3: Single<String>? = null
        val s4: Single<Int> = Single.just(4)
        val s5: Single<String> = createSingle("v5")
        val s6: Single<String> = createSingle("v6")
        val s7: Single<String> = createSingle("v7")
        val s8: Single<String> = createSingle("v8")
        val s9: Single<String> = createSingle("v9")
        val s10: Single<String>? = null
        val s11 = createSingle("v11")

        // When
        // Here I curry the function, so I can apply one by one the the arguments via zipOver() and preserve the types

        val result = service
            .liftSingle()
            .sequential(s1)
            .sequential(s2)
            .sequential(s3)
            .sequential(s4)
            .sequential(s5)
            .sequential(s6)
            .sequential(s7)
            .sequential(s8)
            .sequential(s9)
            .sequential(s10)
            .sequential(s11)
            .subscribeOn(io())

        // Then
        result
            .test()
            .awaitDone(50, TimeUnit.SECONDS)
            .assertValues("Values:v1;2;none;4;v5;v6;v7;v8;v9;none;v11")
    }

    @Test
    fun testMixBoth() {
        // Given

        val service: (String, String, String, String, String, String, String, String, String, String, String) -> Single<String> =
            { s1: String,
              s2: String,
              s3: String,
              s4: String,
              s5: String,
              s6: String,
              s7: String,
              s8: String,
              s9: String,
              s10: String,
              s11: String ->
                val result =
                    listOf(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11).joinToString(
                        separator = ";"
                    )
                Single.just("Values:$result")
            }

        val s1 = createSingle("v1")
        val s2 = createSingle("v2")
        val s3 = createSingle("v3")
        val s4 = createSingle("v4")
        val s5 = createSingle("v5")
        val s6 = createSingle("v6")
        val s7 = createSingle("v7")
        val s8 = createSingle("v8")
        val s9 = createSingle("v9")
        val s10 = createSingle("v10")
        val s11 = createSingle("v11")

        val result = service.liftSingle()
            .sequential(s1)
            .concurrent(s2)
            .concurrent(s3)
            .sequential(s4)
            .concurrent(s5)
            .concurrent(s6)
            .concurrent(s7)
            .concurrent(s8)
            .sequential(s9)
            .sequential(s10)
            .concurrent(s11)

        // Then
        result
            .test()
            .awaitDone(50, TimeUnit.SECONDS)
            .assertValues("Values:v1;v2;v3;v4;v5;v6;v7;v8;v9;v10;v11")
    }

    private fun createBlockingSingle(
        name: String,
        subscribeGate: CountDownLatch? = null,  // opcional: barrera para comprobar concurrencia
        emitGate: CountDownLatch? = null,       // opcional: retener emision para ordenar tiempos
        onDisposeCounter: AtomicInteger? = null // para verificar disposals
    ): Single<String> =
        Observable.create<String> { emitter ->
            println("SUB $name on ${Thread.currentThread().name}")
            subscribeGate?.countDown()
            emitGate?.await(5, TimeUnit.SECONDS)
            if (!emitter.isDisposed) {
                emitter.onNext(name)
                emitter.onComplete()
            }
        }
            .doOnDispose { onDisposeCounter?.incrementAndGet() }
            .singleOrError()
            .subscribeOn(io())

    @Test
    fun error_in_middle_mapOver_stops_following_zipOvers() {
        val disposeCounter = AtomicInteger(0)

        val s1 = Single.just("A")
        val s2 = Single.just(2)
        val s3 = Single.error<String>(RuntimeException("boom"))
        val s4 = createBlockingSingle("C", onDisposeCounter = disposeCounter)
        val s5 = createBlockingSingle("D", onDisposeCounter = disposeCounter)
        val s6 = createBlockingSingle("E", onDisposeCounter = disposeCounter)
        val s7 = createBlockingSingle("F", onDisposeCounter = disposeCounter)
        val s8 = createBlockingSingle("G", onDisposeCounter = disposeCounter)

        val service: (String, Int, String, String, String, String, String, String) -> String =
            { a, b, c, d, e, f, g, h -> listOf(a, b, c, d, e, f, g, h).joinToString(";") }

        val result = service.liftSingle()
            .sequential(s1)
            .concurrent(s2)
            .concurrent(s4)
            .concurrent(s5)
            .sequential(s3)
            .concurrent(s7)
            .sequential(s6)
            .concurrent(s8)
            .subscribeOn(io())

        data class Args(
            val a: String? = null,
            val b: Int? = null,
            val c: String? = null,
            val d: String? = null,
            val e: String? = null,
            val f: String? = null,
            val g: String? = null,
            val h: String? = null,
        )

        val result2: Single<String> =
            s1.flatMap { x1 ->
                s2.zipWith(s4) { x2, x4 ->
                    Args(a = x1, b = x2, d = x4)
                }.zipWith(s5) { args, x5 ->
                    args.copy(e = x5)
                }.flatMap { args ->
                    s3.map { x3 -> args.copy(c = x3) }
                }.flatMap { args ->
                    s7.zipWith(s6) { x7, x6 ->
                        args.copy(g = x7, f = x6)
                    }
                }.flatMap { args ->
                    s8.map { x8 -> args.copy(h = x8) }
                }.map { args ->
                    service(
                        args.a!!, args.b!!, args.c!!, args.d!!,
                        args.e!!, args.f!!, args.g!!, args.h!!
                    )
                }
            }.subscribeOn(io())

        val to = result.test()
        to.awaitDone(3, TimeUnit.SECONDS)
            .assertError { it.message == "boom" }

        check(disposeCounter.get() == 0) { "Se suscribieron ramas posteriores pese al error" }

        val to2 = result2.test()
        to2.awaitDone(3, TimeUnit.SECONDS)
            .assertError { it.message == "boom" }

        check(disposeCounter.get() == 0) { "Se suscribieron ramas posteriores pese al error" }
    }

    fun serviceA(
        s1: String, s2: String, s3: String, s4: String, s5: String,
        s6: String, s7: String, s8: String, s9: String, s10: String, s11: String
    ): Single<String> =
        Single.just("A:" + listOf(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11).joinToString(";"))

    fun serviceB(
        s1: String, s2: String, s3: String, s4: String, s5: String,
        s6: String, s7: String, s8: String, s9: String, s10: String, s11: String
    ): Single<String> =
        Single.just("B:" + listOf(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11).joinToString(";"))

    fun serviceC(
        s1: String, s2: String, s3: String
    ): Single<String> =
        Single.just("C:" + listOf(s1, s2, s3).joinToString(";"))


}