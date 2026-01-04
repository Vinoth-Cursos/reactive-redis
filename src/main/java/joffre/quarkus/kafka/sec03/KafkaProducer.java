package joffre.quarkus.kafka.sec03;

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

/*
    goal: demo a simple kafka producer using reactor-kafka (Quarkus style)
 */
@ApplicationScoped
public class KafkaProducer {
    private static final Logger log =
            LoggerFactory.getLogger(KafkaProducer.class);
    private KafkaSender<String, String> sender;
    private Disposable subscription;
    private long startTime;

    void onStart(@Observes StartupEvent ev) {

        log.info("Starting Kafka backpressure producer (Quarkus)");

        var producerConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"
        );

        var options = SenderOptions.<String, String>create(producerConfig)
                .maxInFlight(19);

        sender = KafkaSender.create(options);

        var flux = Flux.range(1, 180)
                .map(i ->
                        new ProducerRecord<>(
                                "order-events",
                                i.toString(),
                                "order-" + i
                        )
                )
                .map(pr -> SenderRecord.create(pr, pr.key()));

        startTime = System.currentTimeMillis();

        subscription = sender.send(flux)
                .doOnNext(r -> {
                    if (r.correlationMetadata() != null &&
                            Integer.parseInt(r.correlationMetadata()) % 180== 0) {
                        log.info("Sent message {}", r.correlationMetadata());
                    }
                })
                .doOnError(e ->
                        log.error("Kafka producer error", e))
                .doOnComplete(() -> {
                    log.info(
                            "Finished sending messages. Total time taken: {} ms",
                            System.currentTimeMillis() - startTime
                    );
                })
                .subscribe();
    }

    @PreDestroy
    void shutdown() {

        log.info("Shutting down Kafka backpressure producer");

        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }

        if (sender != null) {
            sender.close();
        }
    }
}