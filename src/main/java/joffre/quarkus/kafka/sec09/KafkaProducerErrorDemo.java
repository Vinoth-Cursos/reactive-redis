package joffre.quarkus.kafka.sec09;



import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import org.apache.kafka.clients.producer.ProducerConfig;
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
public class KafkaProducerErrorDemo {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaProducerErrorDemo.class);

    private KafkaSender<String, String> sender;
    private Disposable subscription;

    void onStart(@Observes StartupEvent ev) {

        var producerConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var options =
                SenderOptions.<String, String>create(producerConfig);

        sender = KafkaSender.create(options);

        var flux =
                Flux.range(1, 2_000)
                        .delayElements(Duration.ofMillis(2))
                        .map(i ->
                                SenderRecord.create(
                                        "order-events",
                                        null,
                                        null,
                                        i.toString(),
                                        "order-" + i,
                                        i.toString()
                                )
                        );

        subscription =
                sender.send(flux)
                        .doOnSubscribe(s ->
                                log.info("KafkaProducerErrorDemo started"))
                        .doOnNext(r ->
                                log.info("correlation id: {}",
                                        r.correlationMetadata()))
                        .doOnComplete(() ->
                                log.info("Producer completed"))
                        .subscribe();
    }

    @PreDestroy
    void shutdown() {
        log.info("Shutting down KafkaProducerErrorDemo");
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }
        if (sender != null) {
            sender.close();
        }
    }
}