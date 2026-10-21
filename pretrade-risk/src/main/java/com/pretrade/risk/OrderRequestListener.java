package com.pretrade.risk;

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pretrade.common.DecisionStatus;
import com.pretrade.common.OrderDecision;
import com.pretrade.common.OrderRequested;
import com.pretrade.common.ReferencePrice;
import com.pretrade.common.Topics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Component
public class OrderRequestListener {
    private static final Logger log = LoggerFactory.getLogger(OrderRequestListener.class);
    private static final int DEDUP_WINDOW = 100_000;

    private final PreTradeRiskEngine engine;
    private final ExposureBook exposure;
    private final ReferencePriceCache prices;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final Clock clock;
    private final MeterRegistry meters;
    private final Timer decisionTimer;

    /**
     * The gateway outbox relay delivers at least once, so an order can arrive twice.
     * Recently seen order IDs are kept in a bounded LRU set.
     */
    private final Set<String> seen = Collections.newSetFromMap(Collections.synchronizedMap(
            new LinkedHashMap<>(1024, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > DEDUP_WINDOW;
                }
            }));

    public OrderRequestListener(PreTradeRiskEngine engine, ExposureBook exposure, ReferencePriceCache prices,
                                KafkaTemplate<String, String> kafka, ObjectMapper json, Clock clock,
                                MeterRegistry meters) {
        this.engine = engine;
        this.exposure = exposure;
        this.prices = prices;
        this.kafka = kafka;
        this.json = json;
        this.clock = clock;
        this.meters = meters;
        this.decisionTimer = Timer.builder("pretrade.decision.latency")
                .description("Time from order request to published decision")
                .publishPercentiles(0.5, 0.99, 0.999)
                .register(meters);
    }

    @KafkaListener(topics = Topics.ORDERS_REQUESTED, groupId = "pretrade-risk",
            concurrency = "${risk.listener-concurrency:3}")
    public void onOrder(String payload) throws JsonProcessingException {
        OrderRequested order = json.readValue(payload, OrderRequested.class);
        if (order.orderId() == null || order.accountId() == null) {
            throw new IllegalArgumentException("order without id/account: " + payload);
        }
        if (!seen.add(order.orderId())) {
            meters.counter("pretrade.duplicates").increment();
            log.info("duplicate delivery of order {} ignored", order.orderId());
            return;
        }
        List<String> reasons = engine.evaluate(order);
        DecisionStatus status = reasons.isEmpty() ? DecisionStatus.ACCEPTED : DecisionStatus.REJECTED;
        if (status == DecisionStatus.ACCEPTED) {
            exposure.add(order.accountId(), order.notional());
        }
        OrderDecision decision = new OrderDecision(order.orderId(), order.accountId(), status, reasons,
                clock.instant());
        kafka.send(Topics.ORDERS_DECISIONS, order.accountId(), json.writeValueAsString(decision));

        decisionTimer.record(Duration.between(order.requestedAt(), decision.decidedAt()));
        meters.counter("pretrade.decisions", "status", status.name()).increment();
        reasons.forEach(r -> meters.counter("pretrade.rejections", "reason", r).increment());
    }

    @KafkaListener(topics = Topics.REFERENCE_PRICES, groupId = "pretrade-risk-prices-${random.uuid}",
            properties = "auto.offset.reset=earliest")
    public void onPrice(String payload) throws JsonProcessingException {
        prices.update(json.readValue(payload, ReferencePrice.class));
    }
}
