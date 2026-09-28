package com.igot.cb.util.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

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

  public void putCacheWithoutTtl(String key, int dbIndex, Object object) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.select(dbIndex);
      String data = objectMapper.writeValueAsString(object);
      jedis.set(key, data);
      log.debug("Data saved to database {} with key: {} (no ttl)", dbIndex, key);
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

  /**
   * Seeds a numeric key only if it doesn't already exist (atomic SET...NX EX) - used to
   * initialize a per-user cache value (e.g. karma coin balance) from its source of truth on
   * first use, without a race between concurrent callers each trying to seed it: whichever
   * request's SET NX lands first wins, every other concurrent caller's seed attempt is a
   * silent no-op, and all of them proceed against whatever value is now in Redis.
   *
   * <p>A genuine Redis error throws rather than returning false, deliberately - a caller that
   * treats "false" as just "someone else already seeded it" would otherwise silently proceed
   * to operate (e.g. decrement) against a key that was never actually seeded, misreading a
   * connection failure as a real balance of zero.
   */
  public boolean setIfAbsentWithTtl(String key, int dbIndex, long value, long ttlSeconds) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.select(dbIndex);
      String result = jedis.set(key, String.valueOf(value), SetParams.setParams().nx().ex(ttlSeconds));
      return "OK".equals(result);
    } catch (Exception e) {
      log.error("Error while seeding key in Redis cache: {} ", e.getMessage());
      throw new RuntimeException("Failed to seed Redis key " + key, e);
    }
  }

  /**
   * Atomic decrement (Redis DECRBY) - concurrent callers are serialized by Redis itself, so
   * each sees the post-decrement result of every caller ahead of it, never a stale
   * pre-decrement value. Used to enforce a balance under concurrent requests: the caller
   * must check the returned value and undo (incrementBy) if it went negative.
   */
  public long decrementBy(String key, int dbIndex, long amount) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.select(dbIndex);
      return jedis.decrBy(key, amount);
    } catch (Exception e) {
      log.error("Error while decrementing key in Redis cache: {} ", e.getMessage());
      throw new RuntimeException("Failed to decrement Redis key " + key, e);
    }
  }
}
