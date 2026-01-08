package joffre.quarkus.kafka.sec05;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;

@ApplicationScoped
public class Lec01KafkaProducer {
    private static final Logger log =
            LoggerFactory.getLogger(Lec01KafkaProducer.class);

    private KafkaSender<String, String> sender;
    private Disposable subscription;

    void onStart(@Observes StartupEvent ev) {

        var producerConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8081",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var options = SenderOptions.<String, String>create(producerConfig);
        sender = KafkaSender.create(options);

        var flux = Flux.interval(Duration.ofMillis(50))
                .take(30)
                .map(i -> new ProducerRecord<>(
                        "order-events",
                        i.toString(),
                        "order-" + i
                ))
                .map(pr -> SenderRecord.create(pr, pr.key()));

        subscription = sender.send(flux)
                .doOnSubscribe(s ->
                        log.info("Kafka producer started"))
                .doOnNext(r ->
                        log.info("Message sent | correlationId={}", r.correlationMetadata()))
                .doOnError(e ->
                        log.error("Kafka producer error", e))
                .doOnComplete(() ->
                        log.info("Kafka producer finished sending messages"))
                .subscribe();
    }

    @PreDestroy
    void shutdown() {
        log.info("Shutting down Kafka producer");
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }
        if (sender != null) {
            sender.close();
        }
    }
}