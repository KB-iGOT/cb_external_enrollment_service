package com.igot.cb.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Configuration
public class RedisConfig {

  @Value("${spring.redis.host}")
  private String redisHost;

  @Value("${spring.redis.port}")
  private int redisPort;

  @Value("${spring.redis.default.index}")
  private int defaultIndex;

  @Value("${spring.redis.pool.maxTotal:3000}")
  private int redisMaxTotal;

  @Value("${spring.redis.pool.maxIdle:128}")
  private int redisMaxIdle;

  @Value("${spring.redis.pool.minIdle:100}")
  private int redisMinIdle;

  @Value("${spring.redis.pool.maxWaitMillis:5000}")
  private long redisMaxWaitMillis;

  private final int redisTimeoutMillis = 60000;

  /**
   * Single shared JedisPool for the service - CacheService borrows a connection per call and
   * selects the target logical database on it (see CacheService#withJedis), rather than one
   * pool/connection-factory per database as the old RedisTemplate-based config did.
   */
  @Bean
  public JedisPool jedisPool() {
    return new JedisPool(buildPoolConfig(), redisHost, redisPort, redisTimeoutMillis);
  }

  private JedisPoolConfig buildPoolConfig() {
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setMaxTotal(redisMaxTotal);
    poolConfig.setMaxIdle(redisMaxIdle);
    poolConfig.setMinIdle(redisMinIdle);
    poolConfig.setMaxWaitMillis(redisMaxWaitMillis);
    return poolConfig;
  }

  public int getDefaultIndex() {
    return defaultIndex;
  }
}
