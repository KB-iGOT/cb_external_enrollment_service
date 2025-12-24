package com.igot.cb.util.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class CacheService {

  @Autowired
  private RedisTemplate<String, String> redisTemplate;

  @Autowired
  private ObjectMapper objectMapper;

  @Value("${spring.redis.cacheTtl}")
  private long cacheTtl;

  // ---------- PUT (INDEX) ----------
  public void putCache(String key, int index, Object object) {
    try {
      String data = objectMapper.writeValueAsString(object);

      Long size = redisTemplate.opsForList().size(key);
      if (size == null || size <= index) {
        // pad list with nulls if index does not exist
        for (long i = size == null ? 0 : size; i <= index; i++) {
          redisTemplate.opsForList().rightPush(key, null);
        }
      }

      redisTemplate.opsForList().set(key, index, data);
      redisTemplate.expire(key, cacheTtl, TimeUnit.SECONDS);

    } catch (Exception e) {
      log.error("Error while putting data in Redis cache for key {} at index {} : {}",
              key, index, e.getMessage());
    }
  }

  // ---------- GET (INDEX) ----------
  public String getCache(String key, int index) {
    try {
      return redisTemplate.opsForList().index(key, index);
    } catch (Exception e) {
      log.error("Error while getting data from Redis cache for key {} at index {} : {}",
              key, index, e.getMessage());
      return null;
    }
  }

  // ---------- UPDATE (INDEX) ----------
  public void updateCache(String key, int index, Object object) {
    try {
      String data = objectMapper.writeValueAsString(object);
      redisTemplate.opsForList().set(key, index, data);
      redisTemplate.expire(key, cacheTtl, TimeUnit.SECONDS);
    } catch (Exception e) {
      log.error("Error while updating Redis cache for key {} at index {} : {}",
              key, index, e.getMessage());
    }
  }

  // ---------- DELETE (INDEX) ----------
  public Boolean deleteCache(String key, int index) {
    try {
      String value = redisTemplate.opsForList().index(key, index);
      if (value == null) {
        log.warn("No value found at key {} index {}", key, index);
        return false;
      }

      Long removed = redisTemplate.opsForList().remove(key, 1, value);
      return removed != null && removed > 0;

    } catch (Exception e) {
      log.error("Error while deleting Redis cache for key {} at index {} : {}",
              key, index, e.getMessage());
      return false;
    }
  }

  // ---------- INCREMENT (INDEX) ----------
  public Long incrementIfExists(String key, int index) {
    try {
      String value = redisTemplate.opsForList().index(key, index);
      if (value == null) {
        log.debug("Redis key {} index {} does not exist. Skipping increment.", key, index);
        return null;
      }

      Long incrementedValue = Long.parseLong(value) + 1;
      redisTemplate.opsForList().set(key, index, String.valueOf(incrementedValue));
      redisTemplate.expire(key, cacheTtl, TimeUnit.SECONDS);

      return incrementedValue;

    } catch (Exception e) {
      log.error("Error while incrementing Redis key {} at index {} : {}",
              key, index, e.getMessage());
      return null;
    }
  }
}
