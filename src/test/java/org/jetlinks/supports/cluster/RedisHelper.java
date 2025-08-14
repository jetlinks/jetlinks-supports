package org.jetlinks.supports.cluster;

import lombok.SneakyThrows;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class RedisHelper {
    private static ReactiveRedisTemplate<Object, Object> operations;


    @SneakyThrows
    public static synchronized ReactiveRedisTemplate<Object, Object> getRedisTemplate() {
//        if (operations == null) {
//            RedisClusterConfiguration clusterConfiguration=new RedisClusterConfiguration(
//                    Arrays.asList(
//                            "ubuntu:6380",
//                            "ubuntu:6381",
//                            "ubuntu:6382"
//                    )
//            );
//
//            LettuceConnectionFactory factory = new LettuceConnectionFactory(clusterConfiguration);
//            factory.afterPropertiesSet();
//
//            operations = new ReactiveRedisTemplate<>(factory, RedisSerializationContext.java());
//        }

//        return operations;
        if (operations == null) {
            GenericContainer redis = new GenericContainer<>(DockerImageName.parse("redis:5"))
                .withEnv("TZ", "Asia/Shanghai")
                .withExposedPorts(6379)
                .waitingFor(Wait.forListeningPort());
            redis.start();

            LettuceConnectionFactory factory = new LettuceConnectionFactory(
                "localhost",
                redis.getMappedPort(6379));
            factory.afterPropertiesSet();

            operations = new ReactiveRedisTemplate<>(factory, RedisSerializationContext.java());
        }

        return operations;
    }
}
