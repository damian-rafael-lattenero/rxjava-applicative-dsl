import io.reactivex.rxjava3.core.Flowable
import org.junit.jupiter.api.Test
import utils.liftFlowable

class FlowableStreamDSL11ParameterTest {

    // Business domain: Real-time trading risk assessment system
    data class TradingRiskProfile(
        val symbol: String,
        val currentPrice: Double,
        val volume: Long,
        val volatility: Double,
        val marketSentiment: String,
        val sectorHealth: String,
        val economicIndicators: String,
        val portfolioExposure: Double,
        val liquidityRatio: Double,
        val regulatoryRisk: String,
        val timeWindow: String,
        val riskScore: Double,
        val recommendation: String
    )

    /**
     * Complex 11-parameter risk assessment function that would be nightmare
     * to handle with vanilla RxJava but elegant with our DSL
     */
    fun assessTradingRisk(
        symbol: String,           // Real-time stock symbol
        price: Double,            // Current market price
        volume: Long,             // Trading volume
        volatility: Double,       // Market volatility index
        sentiment: String,        // Market sentiment analysis
        sectorHealth: String,     // Sector performance indicator
        economicData: String,     // Economic indicators
        exposure: Double,         // Current portfolio exposure
        liquidity: Double,        // Liquidity ratio
        regulatory: String,       // Regulatory environment
        timeWindow: String        // Analysis time window
    ): TradingRiskProfile {

        // Complex risk calculation logic
        val baseRisk = when {
            volatility > 0.8 -> 0.9
            volatility > 0.6 -> 0.7
            volatility > 0.4 -> 0.5
            else -> 0.3
        }

        val sentimentMultiplier = when (sentiment) {
            "BULLISH" -> 0.8
            "BEARISH" -> 1.2
            "NEUTRAL" -> 1.0
            else -> 1.1
        }

        val sectorMultiplier = when (sectorHealth) {
            "STRONG" -> 0.9
            "WEAK" -> 1.1
            else -> 1.0
        }

        val exposureRisk = if (exposure > 0.3) 1.2 else 1.0
        val liquidityRisk = if (liquidity < 0.2) 1.3 else 1.0

        val finalRiskScore = baseRisk * sentimentMultiplier * sectorMultiplier * exposureRisk * liquidityRisk

        val recommendation = when {
            finalRiskScore > 0.8 -> "SELL_IMMEDIATELY"
            finalRiskScore > 0.6 -> "REDUCE_POSITION"
            finalRiskScore > 0.4 -> "HOLD_MONITOR"
            finalRiskScore > 0.2 -> "CAUTIOUS_BUY"
            else -> "STRONG_BUY"
        }

        return TradingRiskProfile(
            symbol = symbol,
            currentPrice = price,
            volume = volume,
            volatility = volatility,
            marketSentiment = sentiment,
            sectorHealth = sectorHealth,
            economicIndicators = economicData,
            portfolioExposure = exposure,
            liquidityRatio = liquidity,
            regulatoryRisk = regulatory,
            timeWindow = timeWindow,
            riskScore = finalRiskScore,
            recommendation = recommendation
        )
    }

    @Test
    fun test11ParameterComplexRiskAssessment() {
        println("\n=== 11-Parameter Complex Risk Assessment System ===")
        println("=".repeat(80))

        // Simplified streams with predictable timing for testing
        val symbolStream = Flowable.just("AAPL")
            .doOnNext { println("📈 Symbol: $it") }

        val priceStream = Flowable.just(150.0, 155.0, 160.0)
            .doOnNext { println("💰 Price: $$it") }

        val volumeStream = Flowable.just(1000L, 1200L, 1400L)
            .doOnNext { println("📊 Volume: $it") }

        val volatilityStream = Flowable.just(0.3, 0.5, 0.7)
            .doOnNext { println("⚡ Volatility: $it") }

        val sentimentStream = Flowable.just("BULLISH", "NEUTRAL", "BEARISH")
            .doOnNext { println("🎯 Sentiment: $it") }

        val sectorHealthStream = Flowable.just("STRONG")
            .doOnNext { println("🏭 Sector: $it") }

        val economicDataStream = Flowable.just("ECON_DATA_1")
            .doOnNext { println("🏛️ Economic: $it") }

        val exposureStream = Flowable.just(0.25)
            .doOnNext { println("📋 Exposure: $it") }

        val liquidityStream = Flowable.just(0.35)
            .doOnNext { println("💧 Liquidity: $it") }

        val regulatoryStream = Flowable.just("REG_LEVEL_1")
            .doOnNext { println("⚖️ Regulatory: $it") }

        val timeWindowStream = Flowable.just("1H_WINDOW")
            .doOnNext { println("⏰ Time Window: $it") }

        println("\n--- DSL VERSION (11-Parameter Processing) ---")

        val riskAssessmentResult = ::assessTradingRisk.liftFlowable()
            .combine(symbolStream)        // combine for live updates
            .combine(priceStream)
            .combine(volumeStream)
            .combine(volatilityStream)
            .pair(sentimentStream)        // pair for synchronized analysis
            .pair(sectorHealthStream)
            .chain(economicDataStream)    // chain for sequential processing
            .combine(exposureStream)
            .combine(liquidityStream)
            .pair(regulatoryStream)
            .pair(timeWindowStream)

        // Subscribe and collect results
        val results = mutableListOf<TradingRiskProfile>()

        riskAssessmentResult
            .take(3)  // Limit results
            .subscribe(
                { profile ->
                    println(
                        "🎯 RISK ANALYSIS: ${profile.symbol} -> ${profile.recommendation} (Risk: ${
                            String.format(
                                "%.2f",
                                profile.riskScore
                            )
                        })"
                    )
                    results.add(profile)
                },
                { error ->
                    println("❌ Error: $error")
                },
                {
                    println("✅ Stream completed with ${results.size} risk assessments")
                }
            )

        // Wait a moment for async processing
        Thread.sleep(1000)

        // Assertions
        assert(results.isNotEmpty()) { "Should have generated risk assessments" }
        assert(results.all { it.symbol == "AAPL" }) { "All results should be for AAPL" }

        println("✅ Test completed successfully with ${results.size} results")
    }
}