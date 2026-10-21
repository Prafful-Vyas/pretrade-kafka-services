package com.pretrade.risk;

import static com.pretrade.risk.PreTradeRiskEngine.ACCOUNT_LIMIT;
import static com.pretrade.risk.PreTradeRiskEngine.MAX_ORDER_NOTIONAL;
import static com.pretrade.risk.PreTradeRiskEngine.MAX_QUANTITY;
import static com.pretrade.risk.PreTradeRiskEngine.NO_REFERENCE_PRICE;
import static com.pretrade.risk.PreTradeRiskEngine.PRICE_BAND;
import static com.pretrade.risk.PreTradeRiskEngine.RESTRICTED_INSTRUMENT;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.pretrade.common.OrderRequested;
import com.pretrade.common.ReferencePrice;
import com.pretrade.common.Side;

class PreTradeRiskEngineTest {
    static final Instant NOW = Instant.parse("2026-10-21T09:30:00Z");
    static final String ISIN = "GB0002634946";

    ReferencePriceCache prices;
    ExposureBook exposure;
    PreTradeRiskEngine engine;

    @BeforeEach
    void setUp() {
        RiskLimits limits = new RiskLimits(10_000, new BigDecimal("1000000"), new BigDecimal("5"),
                Duration.ofSeconds(30), new BigDecimal("2000000"),
                Map.of("BIG", new BigDecimal("50000000")), Set.of("GB00RESTRICT1"));
        prices = new ReferencePriceCache(Clock.fixed(NOW, ZoneOffset.UTC));
        exposure = new ExposureBook();
        engine = new PreTradeRiskEngine(limits, prices, exposure);
        prices.update(new ReferencePrice(ISIN, new BigDecimal("100.00"), NOW.minusSeconds(5)));
        prices.update(new ReferencePrice("GB00RESTRICT1", new BigDecimal("10"), NOW));
    }

    static OrderRequested order(String account, String isin, Side side, long qty, String px) {
        return new OrderRequested("o-1", account, isin, side, qty, new BigDecimal(px), "GBP", NOW);
    }

    @Test
    void cleanOrderPasses() {
        assertThat(engine.evaluate(order("A", ISIN, Side.BUY, 1000, "101.00"))).isEmpty();
    }

    @ParameterizedTest(name = "{0} {1} @ {2} -> band breach {3}")
    @CsvSource({"BUY, 105.00, false", "BUY, 105.01, true", "SELL, 95.00, false", "SELL, 94.99, true",
            "BUY, 50.00, false", "SELL, 150.00, false"})
    void priceBandIsDirectional(Side side, String px, boolean breach) {
        // Buying far below / selling far above the reference only hurts the order's own fill chance.
        assertThat(engine.evaluate(order("A", ISIN, side, 10, px)).contains(PRICE_BAND)).isEqualTo(breach);
    }

    @Test
    void stalePriceIsTreatedAsMissing() {
        prices.update(new ReferencePrice("GB00STALE001", new BigDecimal("10"), NOW.minusSeconds(31)));
        assertThat(engine.evaluate(order("A", "GB00STALE001", Side.BUY, 1, "10"))).containsExactly(NO_REFERENCE_PRICE);
    }

    @Test
    void olderPriceNeverOverwritesNewer() {
        prices.update(new ReferencePrice(ISIN, new BigDecimal("200.00"), NOW.minusSeconds(20)));
        assertThat(prices.fresh(ISIN, Duration.ofSeconds(30)).orElseThrow().price()).isEqualByComparingTo("100.00");
    }

    @Test
    void reportsEveryFailingRule() {
        assertThat(engine.evaluate(order("A", "GB00RESTRICT1", Side.BUY, 250_000, "10.00")))
                .containsExactly(RESTRICTED_INSTRUMENT, MAX_QUANTITY, MAX_ORDER_NOTIONAL, ACCOUNT_LIMIT);
    }

    @Test
    void accountLimitUsesAcceptedExposureAndOverrides() {
        exposure.add("A", new BigDecimal("1950000"));
        // 1.95m + 60k > 2m; exactly reaching the limit (1.95m + 50k) is allowed
        assertThat(engine.evaluate(order("A", ISIN, Side.BUY, 500, "100.00"))).isEmpty();
        assertThat(engine.evaluate(order("A", ISIN, Side.BUY, 600, "100.00"))).containsExactly(ACCOUNT_LIMIT);
        exposure.add("BIG", new BigDecimal("1950000"));
        assertThat(engine.evaluate(order("BIG", ISIN, Side.BUY, 600, "100.00"))).isEmpty();
    }
}
