package joffre.quarkus.kafka.sec04;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class KafkaConsumerGroupQuarkus {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaConsumerGroupQuarkus.class);

    private Disposable c1;
    private Disposable c2;
    private Disposable c3;

    void onStart(@Observes StartupEvent ev) {

        c1 = startConsumer("1");
        c2 = startConsumer("2");
        c3 = startConsumer("3");
    }

    private Disposable startConsumer(String instanceId) {

        var config = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "order-consumer-group",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );

        var options = ReceiverOptions.create(config)
                .subscription(List.of("order-events"));

        return KafkaReceiver.create(options)
                .receive()
                .doOnSubscribe(s ->
                        log.info("Consumer instance {} started", instanceId))
                .doOnNext(r -> {
                    log.info(
                            "[instance={}] partition={}, value={}",
                            instanceId,
                            r.partition(),
                            r.value()
                    );
                    r.receiverOffset().acknowledge();
                })
                .subscribe();
    }

    @PreDestroy
    void shutdown() {
        if (c1 != null) c1.dispose();
        if (c2 != null) c2.dispose();
        if (c3 != null) c3.dispose();
    }
}