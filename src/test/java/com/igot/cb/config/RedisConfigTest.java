package com.igot.cb.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
class RedisConfigTest {

    @InjectMocks
    private RedisConfig redisConfig;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(redisConfig, "redisHost", "localhost");
        ReflectionTestUtils.setField(redisConfig, "redisPort", 6379);
        ReflectionTestUtils.setField(redisConfig, "defaultIndex", 0);
        ReflectionTestUtils.setField(redisConfig, "redisMaxTotal", 3000);
        ReflectionTestUtils.setField(redisConfig, "redisMaxIdle", 128);
        // Deliberately 0 here - a positive minIdle makes the pool eagerly open connections on
        // construction, which would require a live Redis server in this test.
        ReflectionTestUtils.setField(redisConfig, "redisMinIdle", 0);
        ReflectionTestUtils.setField(redisConfig, "redisMaxWaitMillis", 5000L);
    }

    @Test
    void jedisPool_CreatesPoolWithConfiguredHostAndPort() {
        JedisPool jedisPool = redisConfig.jedisPool();

        assertNotNull(jedisPool);
        jedisPool.close();
    }

    @Test
    void buildPoolConfig() {
        JedisPoolConfig poolConfig = ReflectionTestUtils.invokeMethod(redisConfig, "buildPoolConfig");

        assertNotNull(poolConfig);
        assertEquals(3000, poolConfig.getMaxTotal());
        assertEquals(128, poolConfig.getMaxIdle());
        assertEquals(0, poolConfig.getMinIdle());
        assertEquals(5000L, poolConfig.getMaxWaitMillis());
    }

    @Test
    void getDefaultIndex_ReturnsConfiguredValue() {
        assertEquals(0, redisConfig.getDefaultIndex());
    }
}
