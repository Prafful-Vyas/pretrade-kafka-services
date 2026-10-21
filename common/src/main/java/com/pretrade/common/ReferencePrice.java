package com.pretrade.common;

import java.math.BigDecimal;
import java.time.Instant;

/** Last reference price for an instrument, used for price-band (fat-finger) checks. */
public record ReferencePrice(String instrument, BigDecimal price, Instant asOf) {
}
