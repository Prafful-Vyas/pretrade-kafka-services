package com.pretrade.common;

import java.math.BigDecimal;
import java.time.Instant;

/** Published by order-gateway (via its outbox) when a client order passes request validation. */
public record OrderRequested(
        String orderId,
        String accountId,
        String instrument,
        Side side,
        long quantity,
        BigDecimal limitPrice,
        String currency,
        Instant requestedAt) {

    public BigDecimal notional() {
        return limitPrice.multiply(BigDecimal.valueOf(quantity));
    }
}
