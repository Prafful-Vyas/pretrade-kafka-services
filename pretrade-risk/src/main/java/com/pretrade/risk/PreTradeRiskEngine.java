package com.pretrade.risk;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Component;

import com.pretrade.common.OrderRequested;
import com.pretrade.common.ReferencePrice;
import com.pretrade.common.Side;

/**
 * Stateless rule evaluation. Every failing rule is reported, not only the first, so a
 * rejected client sees all reasons in one response.
 */
@Component
public class PreTradeRiskEngine {
    public static final String RESTRICTED_INSTRUMENT = "RESTRICTED_INSTRUMENT";
    public static final String MAX_QUANTITY = "MAX_QUANTITY";
    public static final String NO_REFERENCE_PRICE = "NO_REFERENCE_PRICE";
    public static final String PRICE_BAND = "PRICE_BAND";
    public static final String MAX_ORDER_NOTIONAL = "MAX_ORDER_NOTIONAL";
    public static final String ACCOUNT_LIMIT = "ACCOUNT_LIMIT";

    private static final BigDecimal HUNDRED = BigDecimal.valueOf(100);

    private final RiskLimits limits;
    private final ReferencePriceCache prices;
    private final ExposureBook exposure;

    public PreTradeRiskEngine(RiskLimits limits, ReferencePriceCache prices, ExposureBook exposure) {
        this.limits = limits;
        this.prices = prices;
        this.exposure = exposure;
    }

    public List<String> evaluate(OrderRequested o) {
        List<String> reasons = new ArrayList<>();
        if (limits.restrictedInstruments().contains(o.instrument())) {
            reasons.add(RESTRICTED_INSTRUMENT);
        }
        if (o.quantity() > limits.maxQuantity()) {
            reasons.add(MAX_QUANTITY);
        }
        Optional<ReferencePrice> ref = prices.fresh(o.instrument(), limits.maxPriceAge());
        if (ref.isEmpty()) {
            reasons.add(NO_REFERENCE_PRICE);
        } else if (outsideBand(o, ref.get().price())) {
            reasons.add(PRICE_BAND);
        }
        BigDecimal notional = o.notional();
        if (notional.compareTo(limits.maxOrderNotional()) > 0) {
            reasons.add(MAX_ORDER_NOTIONAL);
        }
        if (exposure.current(o.accountId()).add(notional).compareTo(limits.limitFor(o.accountId())) > 0) {
            reasons.add(ACCOUNT_LIMIT);
        }
        return reasons;
    }

    /** A buy above ref*(1+band) or a sell below ref*(1-band) is very likely a keying error. */
    private boolean outsideBand(OrderRequested o, BigDecimal ref) {
        BigDecimal band = ref.multiply(limits.priceBandPct()).divide(HUNDRED, 8, RoundingMode.HALF_UP);
        return o.side() == Side.BUY
                ? o.limitPrice().compareTo(ref.add(band)) > 0
                : o.limitPrice().compareTo(ref.subtract(band)) < 0;
    }
}
