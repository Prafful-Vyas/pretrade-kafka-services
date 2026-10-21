package com.pretrade.gateway;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Publishes outbox rows to Kafka in insertion order and marks them as sent.
 *
 * <p>Delivery is at least once. If the process dies after the broker acks but before
 * the commit, the batch is re-sent on restart, and pretrade-risk de-duplicates by
 * orderId. Each send waits for the ack (acks=all, idempotent producer) before the row
 * is marked, so a row is never marked published without being durable in Kafka.
 * Run one relay instance (or use a DB lease) to keep per-account ordering.
 */
@Component
public class OutboxRelay {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final Clock clock;

    public OutboxRelay(OutboxRepository outbox, KafkaTemplate<String, String> kafka, Clock clock, MeterRegistry meters) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.clock = clock;
        Gauge.builder("gateway.outbox.backlog", outbox, OutboxRepository::countByPublishedAtIsNull)
                .description("Outbox rows not yet published").register(meters);
    }

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:100}")
    @Transactional
    public void relay() {
        List<OutboxEvent> batch = outbox.findTop100ByPublishedAtIsNullOrderByIdAsc();
        for (OutboxEvent e : batch) {
            try {
                kafka.send(e.getTopic(), e.getMessageKey(), e.getPayload()).get(10, TimeUnit.SECONDS);
            } catch (Exception ex) {
                // Stop at the first failure so later events for the same account are not sent ahead of it.
                log.warn("outbox publish failed at id {}; will retry: {}", e.getId(), ex.toString());
                return;
            }
            e.markPublished(clock.instant());
        }
    }
}
