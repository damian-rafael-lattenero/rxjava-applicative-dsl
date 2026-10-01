package utils

import io.reactivex.rxjava3.core.Flowable

// =================================================================
// PROCESS FUNCTIONS FOR FLOWABLE STREAM DSL (1-22 PARAMETERS)
// =================================================================

@JvmName("liftFlowable1")
fun <P1, R : Any> ((P1) -> R).liftFlowable(): Flowable<(P1) -> R> =
    Flowable.just(this)

@JvmName("liftFlowable2")
fun <P1, P2, R : Any> ((P1, P2) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable3")
fun <P1, P2, P3, R : Any> ((P1, P2, P3) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable4")
fun <P1, P2, P3, P4, R : Any> ((P1, P2, P3, P4) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable5")
fun <P1, P2, P3, P4, P5, R : Any> ((P1, P2, P3, P4, P5) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable6")
fun <P1, P2, P3, P4, P5, P6, R : Any> ((P1, P2, P3, P4, P5, P6) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable7")
fun <P1, P2, P3, P4, P5, P6, P7, R : Any> ((P1, P2, P3, P4, P5, P6, P7) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable8")
fun <P1, P2, P3, P4, P5, P6, P7, P8, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable9")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable10")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable11")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable12")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable13")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable14")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable15")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> (P15) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable16")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> (P15) -> (P16) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable17")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> (P15) -> (P16) -> (P17) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable18")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> (P15) -> (P16) -> (P17) -> (P18) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable19")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> (P15) -> (P16) -> (P17) -> (P18) -> (P19) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable20")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> (P15) -> (P16) -> (P17) -> (P18) -> (P19) -> (P20) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable21")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> (P15) -> (P16) -> (P17) -> (P18) -> (P19) -> (P20) -> (P21) -> R> =
    Flowable.just(this.curried())

@JvmName("liftFlowable22")
fun <P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21, P22, R : Any> ((P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21, P22) -> R).liftFlowable(): Flowable<(P1) -> (P2) -> (P3) -> (P4) -> (P5) -> (P6) -> (P7) -> (P8) -> (P9) -> (P10) -> (P11) -> (P12) -> (P13) -> (P14) -> (P15) -> (P16) -> (P17) -> (P18) -> (P19) -> (P20) -> (P21) -> (P22) -> R> =
    Flowable.just(this.curried())