package joffre.quarkus.redis;

import joffre.quarkus.redis.config.RedissonConfig;
import joffre.quarkus.redis.dto.Student;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLocalCachedMap;
import org.redisson.api.RedissonClient;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.redisson.api.RLocalCachedMap;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

public class Lec08LocalCachedMapTest extends BaseTest {


    private RLocalCachedMap<Integer, Student> studentsMap;

    @BeforeEach
    public void setupClient(){
        // backing store simple para simular comportamiento de put/get
        Map<Integer, Student> backing = new ConcurrentHashMap<>();

        // mock que delega en el backing map para put/get
        RLocalCachedMap<Integer, Student> mapMock = Mockito.mock(RLocalCachedMap.class);

        when(mapMock.put(anyInt(), any())).thenAnswer(inv -> {
            Integer k = inv.getArgument(0);
            Student v = inv.getArgument(1);
            return backing.put(k, v);
        });

        when(mapMock.get(anyInt())).thenAnswer(inv -> backing.get(inv.getArgument(0)));

        this.studentsMap = mapMock;
    }

    @Test
    public void appServer1(){
        Student student1 = new Student("sam", 10, "atlanta", List.of(1, 2, 3));
        Student student2 = new Student("jake", 30, "miami", List.of(10, 20, 30));

        this.studentsMap.put(1, student1);
        this.studentsMap.put(2, student2);

        Flux.interval(Duration.ofSeconds(1))
                .doOnNext(i -> System.out.println(i + " ==> " + studentsMap.get(1)))
                .subscribe();

        sleep(6);
    }

    @Test
    public void appServer2(){
        Student student1 = new Student("sam-updated", 10, "atlanta", List.of(1, 2, 3));
        this.studentsMap.put(1, student1);
    }
}
