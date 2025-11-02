import io.reactivex.rxjava3.core.Single
import utils.curried

// Lift de valores directos
@JvmName("liftValue")
fun <P1 : Any> P1.liftSingle(): Single<P1> =
    Single.just(this)

// Lift de funciones
@JvmName("liftFun1")
fun <P1, R : Any> ((P1) -> R).liftSingle(): Single<(P1) -> R> =
    Single.just(this)

@JvmName("liftFun2")
fun <P1, P2, R : Any> ((P1, P2) -> R).liftSingle(): Single<(P1) -> (P2) -> R> =
    Single.just(this.curried())

@JvmName("liftFun3")
fun <P1, P2, P3, R : Any> ((P1, P2, P3) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> R> =
    Single.just(this.curried())

@JvmName("liftFun4")
fun <P1, P2, P3, P4, R : Any> ((P1, P2, P3, P4) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> R> =
    Single.just(this.curried())

@JvmName("liftFun5")
fun <P1, P2, P3, P4, P5, R : Any> ((P1, P2, P3, P4, P5) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> R> =
    Single.just(this.curried())

@JvmName("liftFun6")
fun <P1, P2, P3, P4, P5, P6, R : Any> ((P1, P2, P3, P4, P5, P6) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> R> =
    Single.just(this.curried())

@JvmName("liftFun7")
fun <P1, P2, P3, P4, P5, P6, P7, R : Any> ((P1, P2, P3, P4, P5, P6, P7) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> R> =
    Single.just(this.curried())

@JvmName("liftFun8")
fun <P1, P2, P3, P4, P5, P6, P7, P8, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> R> =
    Single.just(this.curried())

@JvmName("liftFun9")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> R> =
    Single.just(this.curried())

@JvmName("liftFun10")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> R> =
    Single.just(this.curried())

@JvmName("liftFun11")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> R> =
    Single.just(this.curried())

@JvmName("liftFun12")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> R> =
    Single.just(this.curried())

@JvmName("liftFun13")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> R> =
    Single.just(this.curried())

@JvmName("liftFun14")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> R> =
    Single.just(this.curried())

@JvmName("liftFun15")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> (P15) -> R> =
    Single.just(this.curried())

@JvmName("liftFun16")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> (P15) -> (P16) -> R> =
    Single.just(this.curried())

@JvmName("liftFun17")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> (P15) -> (P16) -> (P17) -> R> =
    Single.just(this.curried())

@JvmName("liftFun18")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> (P15) -> (P16) -> (P17) -> (P18) -> R> =
    Single.just(this.curried())

@JvmName("liftFun19")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> (P15) -> (P16) -> (P17) -> (P18) -> (P19) -> R> =
    Single.just(this.curried())

@JvmName("liftFun20")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> (P15) -> (P16) -> (P17) -> (P18) -> (P19) -> (P20) -> R> =
    Single.just(this.curried())

@JvmName("liftFun21")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> (P15) -> (P16) -> (P17) -> (P18) -> (P19) -> (P20) -> (P21) -> R> =
    Single.just(this.curried())

@JvmName("liftFun22")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21, P22, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21, P22) -> R).liftSingle(): Single<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> (P15) -> (P16) -> (P17) -> (P18) -> (P19) -> (P20) -> (P21) -> (P22) -> R> =
    Single.just(this.curried())
