package io.github.afrid09.reliableorders;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.ConsumerFactory;

final class KafkaTestSupport {

    private KafkaTestSupport() {
    }

    /** Reads a topic from the beginning with a fresh consumer group until a record with the key shows up. */
    static ConsumerRecord<String, String> awaitRecord(ConsumerFactory<String, String> factory,
                                                      String topic, String key, Duration timeout) {
        try (Consumer<String, String> consumer = factory.createConsumer("test-" + UUID.randomUUID(), null)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(timeout);
            while (Instant.now().isBefore(deadline)) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(record.key())) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("No record with key " + key + " on " + topic + " within " + timeout);
    }
}
