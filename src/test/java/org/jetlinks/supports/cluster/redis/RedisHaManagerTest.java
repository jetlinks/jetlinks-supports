package org.jetlinks.supports.cluster.redis;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@SpringBootTest(classes = TestApplication.class)
@RunWith(SpringJUnit4ClassRunner.class)
public class RedisHaManagerTest {

    @Autowired
    private ReactiveRedisTemplate<Object, Object> operations;

    @Test
    public void test(){
        //RedisHaManager haManager=new RedisHaManager("test", ServerNode.builder().id("test").build(),)
    }
}