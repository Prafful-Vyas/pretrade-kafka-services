package com.pretrade.risk;

import java.math.BigDecimal;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

/**
 * Gross accepted notional per account for the current trading day.
 *
 * <p>Orders are keyed by accountId, so all orders for one account are consumed by one
 * listener thread, in order. The read-check-add for an account is never concurrent,
 * so the limit check is exact without a lock.
 */
@Component
public class ExposureBook {
    private final ConcurrentHashMap<String, BigDecimal> gross = new ConcurrentHashMap<>();

    public BigDecimal current(String accountId) {
        return gross.getOrDefault(accountId, BigDecimal.ZERO);
    }

    public void add(String accountId, BigDecimal notional) {
        gross.merge(accountId, notional, BigDecimal::add);
    }

    /** Called by the start-of-day job. */
    public void reset() {
        gross.clear();
    }
}
