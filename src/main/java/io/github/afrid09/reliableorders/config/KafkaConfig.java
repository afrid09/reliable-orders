package io.github.afrid09.reliableorders.config;

import io.github.afrid09.reliableorders.fulfillment.NonRetryableEventException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaConfig {

    private static final int PARTITIONS = 3;

    @Bean
    NewTopic orderEventsTopic(AppProperties properties) {
        return TopicBuilder.name(properties.topics().orderEvents())
                .partitions(PARTITIONS)
                .replicas(1)
                .build();
    }

    /**
     * Same partition count as the main topic: the recoverer writes a failed record
     * to the same partition number on the dead-letter topic.
     */
    @Bean
    NewTopic orderEventsDeadLetterTopic(AppProperties properties) {
        return TopicBuilder.name(properties.topics().orderEventsDeadLetter())
                .partitions(PARTITIONS)
                .replicas(1)
                .build();
    }

    /**
     * Transient failures are retried in place with exponential backoff. Once retries
     * run out, or straight away for errors that retrying cannot fix, the record goes
     * to the dead-letter topic and the partition keeps moving.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template, AppProperties properties) {
        AppProperties.Consumer.Retry retry = properties.consumer().retry();

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(retry.maxRetries());
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(10_000);

        // Name the dead-letter topic explicitly, and keep the original partition
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, exception) -> new TopicPartition(
                        properties.topics().orderEventsDeadLetter(), record.partition()));

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(NonRetryableEventException.class);
        return handler;
    }
}
