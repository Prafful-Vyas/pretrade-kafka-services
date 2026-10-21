package com.pretrade.risk;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Pre-trade limits, bound from {@code risk.*} properties.
 *
 * @param maxQuantity           fat-finger cap on units per order
 * @param maxOrderNotional      cap on quantity x limit price for a single order
 * @param priceBandPct          how far (in %) a limit price may be from the reference price
 * @param maxPriceAge           reference prices older than this are treated as missing
 * @param defaultAccountLimit   gross accepted notional per account per day
 * @param accountLimits         per-account overrides of the default limit
 * @param restrictedInstruments instruments on the restricted / watch list
 */
@ConfigurationProperties("risk")
public record RiskLimits(
        @DefaultValue("1000000") long maxQuantity,
        @DefaultValue("5000000") BigDecimal maxOrderNotional,
        @DefaultValue("5") BigDecimal priceBandPct,
        @DefaultValue("30s") Duration maxPriceAge,
        @DefaultValue("20000000") BigDecimal defaultAccountLimit,
        Map<String, BigDecimal> accountLimits,
        Set<String> restrictedInstruments) {

    public RiskLimits {
        accountLimits = accountLimits == null ? Map.of() : Map.copyOf(accountLimits);
        restrictedInstruments = restrictedInstruments == null ? Set.of() : Set.copyOf(restrictedInstruments);
    }

    public BigDecimal limitFor(String accountId) {
        return accountLimits.getOrDefault(accountId, defaultAccountLimit);
    }
}
