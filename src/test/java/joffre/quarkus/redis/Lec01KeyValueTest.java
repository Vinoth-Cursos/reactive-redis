package joffre.quarkus.redis;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RBucketReactive;
import org.redisson.client.codec.StringCodec;
import reactor.test.StepVerifier;

import java.util.concurrent.TimeUnit;

@ExtendWith(MockitoExtension.class)
public class Lec01KeyValueTest extends BaseTest {

    @Test
    public void keyValueAccessTest(){
        RBucketReactive<String> bucket = this.client.getBucket("user:1:name", StringCodec.INSTANCE);
        // set should complete
        StepVerifier.create(bucket.set("sam"))
                .verifyComplete();
        // get should return the mocked value "sam"
        StepVerifier.create(bucket.get())
                .expectNext("sam")
                .verifyComplete();
    }

    @Test
    public void keyValueExpiryTest(){
        RBucketReactive<String> bucket = this.client.getBucket("user:1:name", StringCodec.INSTANCE);
        // set with TTL should complete
        StepVerifier.create(bucket.set("sam", 10, TimeUnit.SECONDS))
                .verifyComplete();
        // get should return the mocked value
        StepVerifier.create(bucket.get())
                .expectNext("sam")
                .verifyComplete();
    }

    @Test
    public void keyValueExtendExpiryTest(){
        RBucketReactive<String> bucket = this.client.getBucket("user:1:name", StringCodec.INSTANCE);
        // initial set
        StepVerifier.create(bucket.set("sam", 10, TimeUnit.SECONDS))
                .verifyComplete();
        // verify value
        StepVerifier.create(bucket.get())
                .expectNext("sam")
                .verifyComplete();
        // extend expiry (mock returns true)
        StepVerifier.create(bucket.expire(60, TimeUnit.SECONDS))
                .expectNext(true)
                .verifyComplete();
        // access expiration time (mock returns 60L)
        StepVerifier.create(bucket.remainTimeToLive())
                .expectNextCount(1)
                .verifyComplete();
    }

}
