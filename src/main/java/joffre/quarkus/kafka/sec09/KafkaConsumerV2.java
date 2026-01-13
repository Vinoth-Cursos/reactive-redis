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
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@ApplicationScoped
public class KafkaConsumerV2 {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaConsumerV2.class);

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

        var options = ReceiverOptions.<Object, Object>create(consumerConfig)
                .subscription(List.of("order-events"));

        subscription =
                KafkaReceiver.create(options)
                        .receive()
                        .doOnSubscribe(s ->
                                log.info("KafkaConsumerV2 started"))
                        .concatMap(this::process)
                        .subscribe();
    }

    private Mono<Void> process(ReceiverRecord<Object, Object> record) {
        return Mono.just(record)
                .doOnNext(r -> {
                    int index = ThreadLocalRandom.current().nextInt(1, 10);
                    log.info("key: {}, index: {}, value: {}",
                            r.key(),
                            index,
                            r.value().toString().toCharArray()[index]);
                })
                .retryWhen(
                        Retry.fixedDelay(3, Duration.ofSeconds(1))
                                .onRetryExhaustedThrow((spec, signal) ->
                                        signal.failure())
                )
                .doOnError(ex ->
                        log.error("Processing error", ex))
                .doFinally(s ->
                        record.receiverOffset().acknowledge())
                .onErrorComplete()
                .then();
    }

    @PreDestroy
    void shutdown() {
        log.info("Shutting down KafkaConsumerV2");
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }
    }
}