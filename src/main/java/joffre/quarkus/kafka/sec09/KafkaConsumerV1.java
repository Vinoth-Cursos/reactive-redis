package joffre.quarkus.kafka.sec09;


import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.Disposable;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class KafkaConsumerV1 {
    private static final Logger log =
            LoggerFactory.getLogger(KafkaConsumerV1.class);

    private Disposable subscription;

    void onStart(@Observes StartupEvent ev) {

        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );

        var options = ReceiverOptions.<String, String>create(consumerConfig)
                .subscription(List.of("order-events"));

        subscription =
                KafkaReceiver.create(options)
                        .receive()
                        .doOnSubscribe(s ->
                                log.info("Kafka consumer started"))
                        .doOnNext(r ->
                                log.info("key: {}, value: {}",
                                        r.key(),
                                        r.value().toCharArray()[15])) // demo error
                        .doOnNext(r ->
                                r.receiverOffset().acknowledge())
                        .doOnError(ex ->
                                log.error("Kafka processing error", ex))
                        .retryWhen(
                                Retry.fixedDelay(3, Duration.ofSeconds(1))
                                        .doBeforeRetry(rs ->
                                                log.warn("Retrying... attempt {}",
                                                        rs.totalRetries() + 1))
                        )
                        .subscribe();
    }

    @PreDestroy
    void shutdown() {
        log.info("Shutting down Kafka consumer");
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }
    }
}