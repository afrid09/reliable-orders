package io.github.afrid09.reliableorders.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app")
public record AppProperties(Topics topics, Outbox outbox, Consumer consumer) {

    public record Topics(String orderEvents) {

        public String orderEventsDeadLetter() {
            return orderEvents + ".DLT";
        }
    }

    public record Outbox(long pollIntervalMs, int batchSize, Duration sendTimeout) {
    }

    public record Consumer(Retry retry) {

        public record Retry(Duration initialInterval, double multiplier, int maxRetries) {
        }
    }
}
