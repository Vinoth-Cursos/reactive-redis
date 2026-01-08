package joffre.quarkus.kafka.sec04;
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
public class KafkaSeekConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaSeekConsumer.class);   
    private Disposable subscription;

    void onStart(@Observes StartupEvent ev) {

        var consumerConfig = Map.<String, Object>of(
                //    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8081",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );

        var options = ReceiverOptions.<String, String>create(consumerConfig)
                .addAssignListener(partitions -> {
                    partitions.forEach(p ->
                            log.info("assigned partition={}, offset={}",
                                    p.topicPartition(), p.position()));

                    partitions.stream()
                            .filter(p -> p.topicPartition().partition() == 2)
                            .findFirst()
                            .ifPresent(p -> {
                                long seekTo = Math.max(p.position() - 2, 0);
                                log.info("seeking partition {} to offset {}",
                                        p.topicPartition(), seekTo);
                                p.seek(seekTo);
                            });
                })
                .subscription(List.of("order-events"));

        subscription = KafkaReceiver.create(options)
                .receive()
                .doOnSubscribe(s -> log.info("Kafka seek consumer started"))
                .doOnNext(r -> {
                    log.info("key={}, value={}, partition={}, offset={}",
                            r.key(),
                            r.value(),
                            r.partition(),
                            r.offset());

                    r.receiverOffset().acknowledge();
                })
                .subscribe();
    }

    @PreDestroy
    void shutdown() {
        if (subscription != null) {
            subscription.dispose();
            log.info("Kafka seek consumer stopped");
        }
    }
}