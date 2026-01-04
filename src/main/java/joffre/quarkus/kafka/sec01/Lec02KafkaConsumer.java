package joffre.quarkus.kafka.sec01;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.core.Disposable;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import jakarta.annotation.PreDestroy;
/*
  goal: to consume from multiple topics
  producer ----> kafka broker <----------> consumer
 */
@ApplicationScoped
public class Lec02KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(Lec02KafkaConsumer.class);
    private Disposable subscription;

    void onStart(@Observes StartupEvent ev) {

        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "inventory-service-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );

        var options = ReceiverOptions.create(consumerConfig)
                .subscription(Pattern.compile("order.*"));

        subscription = KafkaReceiver.create(options)
                .receive()
                .doOnNext(r -> {
                    log.info("topic={}, value={}", r.topic(), r.value());
                    r.receiverOffset().acknowledge();
                })
                .subscribe();
    }

    @PreDestroy
    void shutdown() {
        if (subscription != null) subscription.dispose();
    }
}