package joffre.quarkus.kafka.sec07;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class Lec03KafkaConsumerAutoAck {

    private static final Logger log =
            LoggerFactory.getLogger(Lec03KafkaConsumerAutoAck.class);

    private Disposable subscription;

    void onStart(@Observes StartupEvent ev) {

        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3
        );

        var options = ReceiverOptions.create(consumerConfig)
                .commitInterval(Duration.ofSeconds(1))
                .subscription(List.of("order-events"));

        subscription = KafkaReceiver.create(options)
                .receiveAutoAck()
                .flatMap(this::batchProcess,256)
                .doOnSubscribe(s ->
                        log.info("Kafka consumer (auto-ack + batch) started"))
                .doOnError(e ->
                        log.error("Kafka consumer error", e))
                .subscribe();
    }

    private Mono<Void> batchProcess(Flux<ConsumerRecord<Object, Object>> flux) {
        return flux
                .doFirst(() -> log.info("----------------"))
                .doOnNext(r ->
                        log.info("key: {}, value: {}", r.key(), r.value()))
                .then(Mono.delay(Duration.ofSeconds(1)))
                .then();
    }

    @PreDestroy
    void shutdown() {
        log.info("Shutting down Kafka consumer (auto-ack)");
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }
    }
}