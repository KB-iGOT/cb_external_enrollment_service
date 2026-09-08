package com.igot.cb.util.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class CacheService {

  @Autowired
  private JedisPool jedisPool;

  @Autowired
  private ObjectMapper objectMapper;

  @Value("${spring.redis.cacheTtl}")
  private long cacheTtl;

  public void putCache(String key, int dbIndex, Object object) {
    putCache(key, dbIndex, object, cacheTtl);
  }

  public void putCache(String key, int dbIndex, Object object, long ttlSeconds) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.select(dbIndex);
      String data = objectMapper.writeValueAsString(object);
      jedis.setex(key, (int) ttlSeconds, data);
      log.debug("Data saved to database {} with key: {}", dbIndex, key);
    } catch (Exception e) {
      log.error("Error while putting data in Redis cache: {} ", e.getMessage());
    }
  }

  public String getCache(String key, int dbIndex) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.select(dbIndex);
      return jedis.get(key);
    } catch (Exception e) {
      log.error("Error while getting data from Redis cache: {} ", e.getMessage());
      return null;
    }
  }

  public Boolean deleteCache(String key, int dbIndex) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.select(dbIndex);
      long result = jedis.del(key);
      if (result > 0) {
        log.info("Key {} deleted successfully from database {}.", key, dbIndex);
      } else {
        log.warn("Key {} not found in database {}.", key, dbIndex);
      }
      return result > 0;
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
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.select(dbIndex);
      jedis.hset(key, fieldValueMap);
      log.debug("Hash with {} fields saved to database {} under key: {}", fieldValueMap.size(), dbIndex, key);
    } catch (Exception e) {
      log.error("Error while putting hash fields in Redis cache: {} ", e.getMessage());
    }
  }

  public Map<Object, Object> getAllHashFields(String key, int dbIndex) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.select(dbIndex);
      Map<String, String> fields = jedis.hgetAll(key);
      return new HashMap<>(fields);
    } catch (Exception e) {
      log.error("Error while getting hash fields from Redis cache: {} ", e.getMessage());
      return Collections.emptyMap();
    }
  }

  public Long incrementIfExists(String key, long delta, int dbIndex) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.select(dbIndex);
      if (jedis.exists(key)) {
        return jedis.incrBy(key, delta);
      }
      log.warn("Key {} does not exist in database {}, increment skipped.", key, dbIndex);
      return null;
    } catch (Exception e) {
      log.error("Error while incrementing key in Redis cache: {} ", e.getMessage());
      return null;
    }
  }
}
