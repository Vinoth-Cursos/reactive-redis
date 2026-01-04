package joffre.quarkus.kafka.sec01;

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
/*
  goal: to demo a simple kafka consumer using reactor kafka
  producer ----> kafka broker <----------> consumer

  topic: order-events
  partitions: 1
  log-end-offset: 15
  current-offset: 15

 */
@ApplicationScoped
public class Lec01KafkaConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(Lec01KafkaConsumer.class);

    // 👇 ESTE CAMPO FALTABA
    private Disposable subscription;

    void onStart(@Observes StartupEvent ev) {

        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );

        var options = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events"));

        subscription = KafkaReceiver.create(options)
                .receive()
                .doOnSubscribe(s ->
                        log.info("Kafka consumer subscribed to topic order-events"))
                .doOnNext(r -> {
                    log.info("key={}, value={}", r.key(), r.value());
                    r.receiverOffset().acknowledge();
                })
                .doOnError(e ->
                        log.error("Kafka consumer error", e))
                .subscribe();
    }

    @PreDestroy
    void shutdown() {
        if (subscription != null && !subscription.isDisposed()) {
            log.info("Shutting down Kafka consumer");
            subscription.dispose();
        }
    }
}