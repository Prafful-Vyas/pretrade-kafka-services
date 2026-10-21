package com.pretrade.risk;

import java.time.Clock;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.pretrade.common.Topics;

@Configuration
public class RiskConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    NewTopic ordersRequested() {
        return TopicBuilder.name(Topics.ORDERS_REQUESTED).partitions(6).build();
    }

    @Bean
    NewTopic ordersDecisions() {
        return TopicBuilder.name(Topics.ORDERS_DECISIONS).partitions(6).build();
    }

    @Bean
    NewTopic ordersRequestedDlt() {
        return TopicBuilder.name(Topics.ORDERS_REQUESTED_DLT).partitions(6).build();
    }

    @Bean
    NewTopic referencePrices() {
        // Compacted, so a restarted instance rebuilds its cache from the latest price per instrument.
        return TopicBuilder.name(Topics.REFERENCE_PRICES).partitions(3).compact().build();
    }

    /**
     * Malformed messages go straight to the dead-letter topic, because retrying cannot
     * fix them. Other failures get 3 quick retries first. The DLT keeps the original
     * partition, so per-account ordering is preserved when it is replayed.
     */
    @Bean
    CommonErrorHandler errorHandler(KafkaTemplate<String, String> template) {
        DefaultErrorHandler handler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template),
                new FixedBackOff(200L, 3L));
        handler.addNotRetryableExceptions(JsonProcessingException.class, IllegalArgumentException.class);
        return handler;
    }
}
