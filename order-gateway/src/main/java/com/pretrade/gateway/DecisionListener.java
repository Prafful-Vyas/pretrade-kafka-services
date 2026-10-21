package com.pretrade.gateway;

import java.time.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pretrade.common.DecisionStatus;
import com.pretrade.common.OrderDecision;
import com.pretrade.common.Topics;

/** Applies risk decisions to stored orders. Re-delivered decisions are no-ops. */
@Component
public class DecisionListener {
    private static final Logger log = LoggerFactory.getLogger(DecisionListener.class);

    private final OrderRepository orders;
    private final ObjectMapper json;
    private final Clock clock;

    public DecisionListener(OrderRepository orders, ObjectMapper json, Clock clock) {
        this.orders = orders;
        this.json = json;
        this.clock = clock;
    }

    @KafkaListener(topics = Topics.ORDERS_DECISIONS, groupId = "order-gateway")
    @Transactional
    public void onDecision(String payload) throws JsonProcessingException {
        OrderDecision d = json.readValue(payload, OrderDecision.class);
        orders.findById(d.orderId()).ifPresentOrElse(
                o -> {
                    if (!o.decide(d.status() == DecisionStatus.ACCEPTED, String.join(",", d.reasons()), clock.instant())) {
                        log.debug("order {} already decided; ignoring duplicate decision", d.orderId());
                    }
                },
                () -> log.warn("decision for unknown order {}", d.orderId()));
    }
}
