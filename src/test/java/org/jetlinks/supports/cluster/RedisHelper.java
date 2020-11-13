package org.jetlinks.supports.cluster;

import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;

public class RedisHelper {
    private static ReactiveRedisTemplate<Object, Object> operations;


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
            LettuceConnectionFactory factory = new LettuceConnectionFactory("localhost", 6379);
            factory.setDatabase(5);
            factory.afterPropertiesSet();
            operations = new ReactiveRedisTemplate<>(factory, RedisSerializationContext.java());
        }

        return operations;
    }
}
