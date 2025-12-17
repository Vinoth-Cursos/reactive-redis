package joffre.quarkus.redis.config;


import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.api.RedissonReactiveClient;

import java.util.Objects;

@ApplicationScoped
public class RedissonConfigBeta {

    @ConfigProperty(name = "redisson.address", defaultValue = "redis://127.0.0.1:6379")
    String address;

    private RedissonClient redissonClient;

    @Produces
    @ApplicationScoped
    public RedissonClient getClient() {
        if (Objects.isNull(this.redissonClient)) {
            org.redisson.config.Config config = new org.redisson.config.Config();
            config.useSingleServer()
                    .setAddress(address);
            redissonClient = Redisson.create(config);
        }
        return redissonClient;
    }

    @Produces
    public RedissonReactiveClient getReactiveClient() {
        return getClient().reactive();
    }

    @PreDestroy
    public void cleanup() {
        if (this.redissonClient != null && !this.redissonClient.isShutdown()) {
            this.redissonClient.shutdown();
        }
    }
}
