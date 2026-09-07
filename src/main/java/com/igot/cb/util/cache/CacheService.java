package com.igot.cb.util.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.config.RedisConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class CacheService {

  @Autowired
  private RedisTemplate<String, String> redisTemplate;

  @Autowired
  private RedisConfig redisConfig;

  @Autowired
  private ObjectMapper objectMapper;

  @Value("${spring.redis.cacheTtl}")
  private long cacheTtl;

  @Value("${spring.redis.database:0}")
  private int defaultDatabase;

  private final ConcurrentHashMap<Integer, RedisTemplate<String, String>> templateCache = new ConcurrentHashMap<>();

  private RedisTemplate<String, String> getTemplate(int dbIndex) {
    if (dbIndex == defaultDatabase) {
      return redisTemplate;
    }

    return templateCache.computeIfAbsent(dbIndex, db -> {
      log.info("Creating new RedisTemplate for database: {}", db);
      return redisConfig.createRedisTemplate(redisConfig.createConnectionFactory(db));
    });
  }

  public void putCache(String key, int dbIndex, Object object) {
    putCache(key, dbIndex, object, cacheTtl);
  }

  public void putCache(String key, int dbIndex, Object object, long ttlSeconds) {
    try {
      RedisTemplate<String, String> template = getTemplate(dbIndex);
      String data = objectMapper.writeValueAsString(object);
      template.opsForValue().set(key, data, ttlSeconds, TimeUnit.SECONDS);
      log.debug("Data saved to database {} with key: {}", dbIndex, key);
    } catch (Exception e) {
      log.error("Error while putting data in Redis cache: {} ", e.getMessage());
    }
  }

  public String getCache(String key, int dbIndex) {
    try {
      RedisTemplate<String, String> template = getTemplate(dbIndex);
      return template.opsForValue().get(key);
    } catch (Exception e) {
      log.error("Error while getting data from Redis cache: {} ", e.getMessage());
      return null;
    }
  }

  public Boolean deleteCache(String key, int dbIndex) {
    try {
      RedisTemplate<String, String> template = getTemplate(dbIndex);
      boolean result = template.delete(key);
      if(result) {
        log.info("Key {} deleted successfully from database {}.", key, dbIndex);
      } else {
        log.warn("Key {} not found in database {}.", key, dbIndex);
      }
      return result;
    } catch (Exception e) {
      log.error("Error while deleting key from Redis cache: {} ", e.getMessage());
      return false;
    }
  }

  /**
   * Bulk-writes every field of a Redis hash in a single round trip (HMSET) - used to
   * populate a per-user "map" of minimal enrolment info (courseId -> {partnerId, status})
   * in one shot the first time it's needed, rather than one write per field.
   */
  public void putAllHashFields(String key, int dbIndex, Map<String, String> fieldValueMap) {
    try {
      RedisTemplate<String, String> template = getTemplate(dbIndex);
      template.opsForHash().putAll(key, fieldValueMap);
      log.debug("Hash with {} fields saved to database {} under key: {}", fieldValueMap.size(), dbIndex, key);
    } catch (Exception e) {
      log.error("Error while putting hash fields in Redis cache: {} ", e.getMessage());
    }
  }

  public Map<Object, Object> getAllHashFields(String key, int dbIndex) {
    try {
      RedisTemplate<String, String> template = getTemplate(dbIndex);
      return template.opsForHash().entries(key);
    } catch (Exception e) {
      log.error("Error while getting hash fields from Redis cache: {} ", e.getMessage());
      return Collections.emptyMap();
    }
  }

  public Long incrementIfExists(String key, long delta, int dbIndex) {
    try {
      RedisTemplate<String, String> template = getTemplate(dbIndex);
      Boolean exists = template.hasKey(key);
      if (Boolean.TRUE.equals(exists)) {
        return template.opsForValue().increment(key, delta);
      } else {
        log.warn("Key {} does not exist in database {}, increment skipped.", key, dbIndex);
        return null;
      }
    } catch (Exception e) {
      log.error("Error while incrementing key in Redis cache: {} ", e.getMessage());
      return null;
    }
  }
}