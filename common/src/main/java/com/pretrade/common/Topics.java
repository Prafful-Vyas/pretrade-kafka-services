package com.pretrade.common;

/** Kafka topic names. All order topics are keyed by accountId, so every event for an
 * account lands on one partition and is processed in order by one consumer thread. */
public final class Topics {
    public static final String ORDERS_REQUESTED = "orders.requested";
    public static final String ORDERS_DECISIONS = "orders.decisions";
    public static final String REFERENCE_PRICES = "marketdata.reference-prices";
    public static final String ORDERS_REQUESTED_DLT = ORDERS_REQUESTED + ".DLT";

    private Topics() {
    }
}
