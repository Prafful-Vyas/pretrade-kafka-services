package com.pretrade.risk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pretrade.common.DecisionStatus;
import com.pretrade.common.OrderDecision;
import com.pretrade.common.OrderRequested;
import com.pretrade.common.ReferencePrice;
import com.pretrade.common.Side;
import com.pretrade.common.Topics;

@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 3, topics = {Topics.ORDERS_REQUESTED, Topics.ORDERS_DECISIONS,
        Topics.ORDERS_REQUESTED_DLT, Topics.REFERENCE_PRICES})
class RiskServiceIntegrationTest {
    static final String ISIN = "GB0002634946";

    @Autowired EmbeddedKafkaBroker broker;
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ObjectMapper json;
    @Autowired ExposureBook exposure;

    Consumer<String, String> consumer;

    @BeforeEach
    void setUp() throws Exception {
        Map<String, Object> props = KafkaTestUtils.consumerProps("it-" + UUID.randomUUID(), "false", broker);
        props.put("auto.offset.reset", "earliest");
        consumer = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer())
                .createConsumer();
        broker.consumeFromEmbeddedTopics(consumer, Topics.ORDERS_DECISIONS, Topics.ORDERS_REQUESTED_DLT);
        exposure.reset();
        kafka.send(Topics.REFERENCE_PRICES, ISIN,
                json.writeValueAsString(new ReferencePrice(ISIN, new BigDecimal("100"), Instant.now()))).get();
        Thread.sleep(500); // let the price listener apply it before orders arrive
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    void send(OrderRequested o) throws Exception {
        kafka.send(Topics.ORDERS_REQUESTED, o.accountId(), json.writeValueAsString(o)).get();
    }

    List<ConsumerRecord<String, String>> collect(String topic, int expected) {
        List<ConsumerRecord<String, String>> got = new ArrayList<>();
        await().atMost(Duration.ofSeconds(20)).until(() -> {
            KafkaTestUtils.getRecords(consumer, Duration.ofMillis(500)).records(topic).forEach(got::add);
            return got.size() >= expected;
        });
        return got;
    }

    static OrderRequested order(String id, String account, long qty, String px) {
        return new OrderRequested(id, account, ISIN, Side.BUY, qty, new BigDecimal(px), "GBP", Instant.now());
    }

    @Test
    void acceptsThenRejectsWhenAccountLimitIsReached_andIgnoresDuplicates() throws Exception {
        String acct = "ACC-" + UUID.randomUUID();
        OrderRequested first = order("o-" + UUID.randomUUID(), acct, 40_000, "100"); // 4.0m notional
        send(first);
        send(first); // redelivery from the outbox relay
        for (int i = 0; i < 4; i++) {
            send(order("o-" + UUID.randomUUID(), acct, 40_000, "100"));
        }
        List<OrderDecision> decisions = new ArrayList<>();
        for (var r : collect(Topics.ORDERS_DECISIONS, 5)) {
            assertThat(r.key()).isEqualTo(acct);
            decisions.add(json.readValue(r.value(), OrderDecision.class));
        }
        assertThat(decisions).hasSize(5); // duplicate produced no second decision
        // 20m default limit: the first five 4m orders fit exactly; nothing more may be accepted.
        assertThat(decisions).extracting(OrderDecision::status).containsOnly(DecisionStatus.ACCEPTED);
        send(order("o-" + UUID.randomUUID(), acct, 1, "100"));
        OrderDecision last = json.readValue(collect(Topics.ORDERS_DECISIONS, 1).get(0).value(), OrderDecision.class);
        assertThat(last.status()).isEqualTo(DecisionStatus.REJECTED);
        assertThat(last.reasons()).containsExactly(PreTradeRiskEngine.ACCOUNT_LIMIT);
    }

    @Test
    void malformedMessageGoesToDeadLetterTopic() throws Exception {
        kafka.send(Topics.ORDERS_REQUESTED, "ACC-X", "{not json").get();
        ConsumerRecord<String, String> dlt = collect(Topics.ORDERS_REQUESTED_DLT, 1).get(0);
        assertThat(dlt.value()).isEqualTo("{not json");
        assertThat(dlt.headers().lastHeader("kafka_dlt-exception-fqcn")).isNotNull();
    }
}
