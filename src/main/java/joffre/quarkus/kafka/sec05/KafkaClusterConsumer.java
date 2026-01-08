package joffre.quarkus.kafka.sec05;

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

import java.util.List;
import java.util.Map;

@ApplicationScoped
public class KafkaClusterConsumer {
    private static final Logger log =
            LoggerFactory.getLogger(KafkaClusterConsumer.class);

    private Disposable subscription;

    void onStart(@Observes StartupEvent ev) {

        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8081",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );

        var options = ReceiverOptions.<String, String>create(consumerConfig)
                .subscription(List.of("order-events"));

        subscription = KafkaReceiver.create(options)
                .receive()
                .doOnSubscribe(s ->
                        log.info("Kafka cluster consumer started"))
                .doOnNext(r -> {
                    log.info(
                            "key={}, value={}, partition={}, offset={}",
                            r.key(),
                            r.value(),
                            r.partition(),
                            r.offset()
                    );
                    r.receiverOffset().acknowledge();
                })
                .doOnError(e ->
                        log.error("Kafka consumer error", e))
                .subscribe();
    }

    @PreDestroy
    void shutdown() {
        if (subscription != null && !subscription.isDisposed()) {
            log.info("Shutting down Kafka cluster consumer");
            subscription.dispose();
        }
    }
}