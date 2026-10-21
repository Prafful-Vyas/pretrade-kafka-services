package com.pretrade.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pretrade.common.DecisionStatus;
import com.pretrade.common.OrderDecision;
import com.pretrade.common.OrderRequested;
import com.pretrade.common.Topics;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 3, topics = {Topics.ORDERS_REQUESTED, Topics.ORDERS_DECISIONS})
class OrderGatewayIntegrationTest {
    static final String BODY = """
            {"accountId":"ACC-10042","instrument":"GB0002634946","side":"BUY",
             "quantity":1500,"limitPrice":101.25,"currency":"GBP"}""";

    @Autowired MockMvc mvc;
    @Autowired EmbeddedKafkaBroker broker;
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ObjectMapper json;
    @Autowired OutboxRepository outbox;

    Consumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> props = KafkaTestUtils.consumerProps("it-" + UUID.randomUUID(), "false", broker);
        props.put("auto.offset.reset", "latest");
        consumer = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer())
                .createConsumer();
        broker.consumeFromAnEmbeddedTopic(consumer, Topics.ORDERS_REQUESTED);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    String submit(String key, String body, int expectedStatus) throws Exception {
        return mvc.perform(post("/api/v1/orders").header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().is(expectedStatus))
                .andReturn().getResponse().getContentAsString();
    }

    List<ConsumerRecord<String, String>> requested(int atLeast) {
        List<ConsumerRecord<String, String>> got = new ArrayList<>();
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            KafkaTestUtils.getRecords(consumer, Duration.ofMillis(300)).forEach(got::add);
            return got.size() >= atLeast;
        });
        return got;
    }

    @Test
    void submitPublishesThroughOutboxAndAppliesDecision() throws Exception {
        String key = "k-" + UUID.randomUUID();
        String created = mvc.perform(post("/api/v1/orders").header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isAccepted())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.status").value("PENDING_RISK"))
                .andReturn().getResponse().getContentAsString();
        String orderId = json.readTree(created).get("orderId").asText();

        ConsumerRecord<String, String> rec = requested(1).stream()
                .filter(r -> r.value().contains(orderId)).findFirst().orElseThrow();
        OrderRequested event = json.readValue(rec.value(), OrderRequested.class);
        assertThat(rec.key()).isEqualTo("ACC-10042"); // keyed by account for per-account ordering
        assertThat(event.quantity()).isEqualTo(1500);
        await().atMost(Duration.ofSeconds(5)).until(() -> outbox.countByPublishedAtIsNull() == 0);

        OrderDecision decision = new OrderDecision(orderId, "ACC-10042", DecisionStatus.REJECTED,
                List.of("PRICE_BAND", "ACCOUNT_LIMIT"), Instant.now());
        kafka.send(Topics.ORDERS_DECISIONS, "ACC-10042", json.writeValueAsString(decision)).get();
        kafka.send(Topics.ORDERS_DECISIONS, "ACC-10042", json.writeValueAsString(
                new OrderDecision(orderId, "ACC-10042", DecisionStatus.ACCEPTED, List.of(), Instant.now()))).get();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                mvc.perform(get("/api/v1/orders/" + orderId))
                        .andExpect(jsonPath("$.status").value("REJECTED"))
                        .andExpect(jsonPath("$.rejectReasons[0]").value("PRICE_BAND"))
                        .andExpect(jsonPath("$.rejectReasons[1]").value("ACCOUNT_LIMIT")));
        Thread.sleep(500); // the later, conflicting decision must not override the first
        mvc.perform(get("/api/v1/orders/" + orderId)).andExpect(jsonPath("$.status").value("REJECTED"));
    }

    @Test
    void retryWithSameKeyReturnsOriginalAndPublishesOnce() throws Exception {
        String key = "k-" + UUID.randomUUID();
        String first = submit(key, BODY, 202);
        // Same order, different JSON formatting and price scale: still the same request.
        String second = submit(key, BODY.replace("101.25", "101.250"), 200);
        String id = json.readTree(first).get("orderId").asText();
        assertThat(json.readTree(second).get("orderId").asText()).isEqualTo(id);
        Thread.sleep(800);
        long published = requested(1).stream().filter(r -> r.value().contains(id)).count();
        assertThat(published).isEqualTo(1);
    }

    @Test
    void sameKeyDifferentOrderIsConflict() throws Exception {
        String key = "k-" + UUID.randomUUID();
        submit(key, BODY, 202);
        String problem = submit(key, BODY.replace("1500", "1600"), 409);
        assertThat(problem).contains("already used for a different order");
    }

    @Test
    void validationErrorsAreReportedPerField() throws Exception {
        String bad = """
                {"accountId":"bob","instrument":"NOTANISIN","side":"BUY",
                 "quantity":0,"limitPrice":-1,"currency":"pounds"}""";
        mvc.perform(post("/api/v1/orders").header("Idempotency-Key", "k-" + UUID.randomUUID())
                        .contentType(MediaType.APPLICATION_JSON).content(bad))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.accountId").exists())
                .andExpect(jsonPath("$.errors.instrument").exists())
                .andExpect(jsonPath("$.errors.quantity").exists())
                .andExpect(jsonPath("$.errors.limitPrice").exists())
                .andExpect(jsonPath("$.errors.currency").exists());
    }

    @Test
    void idempotencyKeyIsRequiredAndValidated() throws Exception {
        mvc.perform(post("/api/v1/orders").contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isBadRequest());
        submit("short", BODY, 400);
    }

    @Test
    void unknownOrderIs404_andOpenApiIsPublished() throws Exception {
        mvc.perform(get("/api/v1/orders/" + UUID.randomUUID())).andExpect(status().isNotFound());
        mvc.perform(get("/v3/api-docs")).andExpect(status().isOk())
                .andExpect(jsonPath("$.paths['/api/v1/orders'].post").exists());
    }
}
