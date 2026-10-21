package com.pretrade.risk;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

import com.pretrade.common.ReferencePrice;

/** Latest reference price per instrument, fed from the market-data topic. */
@Component
public class ReferencePriceCache {
    private final ConcurrentHashMap<String, ReferencePrice> prices = new ConcurrentHashMap<>();
    private final Clock clock;

    public ReferencePriceCache(Clock clock) {
        this.clock = clock;
    }

    /** Keeps the newest price; market data can arrive out of order across partitions. */
    public void update(ReferencePrice p) {
        prices.merge(p.instrument(), p, (old, nu) -> nu.asOf().isAfter(old.asOf()) ? nu : old);
    }

    /** The price, if one exists and is younger than {@code maxAge}. */
    public Optional<ReferencePrice> fresh(String instrument, Duration maxAge) {
        ReferencePrice p = prices.get(instrument);
        if (p == null || p.asOf().plus(maxAge).isBefore(clock.instant())) {
            return Optional.empty();
        }
        return Optional.of(p);
    }
}
