package com.igot.cb.util.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CacheServiceTest {

    @InjectMocks
    private CacheService cacheService;

    @Mock
    private JedisPool jedisPool;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private Jedis jedis;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(cacheService, "cacheTtl", 3600L);
        lenient().when(jedisPool.getResource()).thenReturn(jedis);
    }

    @Test
    void putCache_Success() throws Exception {
        String key = "testKey";
        Map<String, String> object = new HashMap<>();
        object.put("field", "value");
        String jsonString = "{\"field\":\"value\"}";

        when(objectMapper.writeValueAsString(object)).thenReturn(jsonString);

        cacheService.putCache(key, 2, object);

        verify(jedis).select(2);
        verify(jedis).setex(key, 3600, jsonString);
        verify(jedis).close();
    }

    @Test
    void putCache_CustomTtl() throws Exception {
        String key = "testKey";
        Map<String, String> object = new HashMap<>();
        object.put("field", "value");
        String jsonString = "{\"field\":\"value\"}";

        when(objectMapper.writeValueAsString(object)).thenReturn(jsonString);

        cacheService.putCache(key, 0, object, 120L);

        verify(jedis).select(0);
        verify(jedis).setex(key, 120, jsonString);
    }

    @Test
    void putCache_Exception() throws Exception {
        String key = "testKey";
        Object object = new Object();

        when(objectMapper.writeValueAsString(object)).thenThrow(new RuntimeException("Test exception"));

        assertDoesNotThrow(() -> cacheService.putCache(key, 0, object));
        verify(jedis, never()).setex(anyString(), anyInt(), anyString());
    }

    @Test
    void putCacheWithoutTtl_Success() throws Exception {
        String key = "testKey";
        Map<String, String> object = new HashMap<>();
        object.put("field", "value");
        String jsonString = "{\"field\":\"value\"}";

        when(objectMapper.writeValueAsString(object)).thenReturn(jsonString);

        cacheService.putCacheWithoutTtl(key, 2, object);

        verify(jedis).select(2);
        verify(jedis).set(key, jsonString);
        verify(jedis, never()).setex(anyString(), anyInt(), anyString());
    }

    @Test
    void putCacheWithoutTtl_Exception() throws Exception {
        String key = "testKey";
        Object object = new Object();

        when(objectMapper.writeValueAsString(object)).thenThrow(new RuntimeException("Test exception"));

        assertDoesNotThrow(() -> cacheService.putCacheWithoutTtl(key, 0, object));
        verify(jedis, never()).set(anyString(), anyString());
    }

    @Test
    void getCache_Success() {
        String key = "testKey";
        String cachedValue = "{\"field\":\"value\"}";

        when(jedis.get(key)).thenReturn(cachedValue);

        String result = cacheService.getCache(key, 1);

        assertEquals(cachedValue, result);
        verify(jedis).select(1);
        verify(jedis).get(key);
    }

    @Test
    void getCache_ReturnsNull() {
        String key = "nonExistentKey";

        when(jedis.get(key)).thenReturn(null);

        String result = cacheService.getCache(key, 0);

        assertNull(result);
    }

    @Test
    void getCache_Exception() {
        String key = "testKey";

        when(jedisPool.getResource()).thenThrow(new RuntimeException("Test exception"));

        String result = cacheService.getCache(key, 0);

        assertNull(result);
    }

    @Test
    void deleteCache_Success() {
        String key = "testKey";

        when(jedis.del(key)).thenReturn(1L);

        Boolean result = cacheService.deleteCache(key, 0);

        assertTrue(result);
        verify(jedis).del(key);
    }

    @Test
    void deleteCache_KeyNotFound() {
        String key = "nonExistentKey";

        when(jedis.del(key)).thenReturn(0L);

        Boolean result = cacheService.deleteCache(key, 0);

        assertFalse(result);
    }

    @Test
    void deleteCache_Exception() {
        String key = "testKey";

        when(jedis.del(key)).thenThrow(new RuntimeException("Test exception"));

        Boolean result = cacheService.deleteCache(key, 0);

        assertFalse(result);
    }

    @Test
    void putAllHashFields_Success() {
        String key = "hashKey";
        Map<String, String> fields = new HashMap<>();
        fields.put("f1", "v1");

        cacheService.putAllHashFields(key, 0, fields);

        verify(jedis).select(0);
        verify(jedis).hset(key, fields);
    }

    @Test
    void getAllHashFields_Success() {
        String key = "hashKey";
        Map<String, String> fields = new HashMap<>();
        fields.put("f1", "v1");

        when(jedis.hgetAll(key)).thenReturn(fields);

        Map<Object, Object> result = cacheService.getAllHashFields(key, 0);

        assertEquals(1, result.size());
        assertEquals("v1", result.get("f1"));
    }

    @Test
    void getAllHashFields_Exception() {
        String key = "hashKey";

        when(jedis.hgetAll(key)).thenThrow(new RuntimeException("Test exception"));

        Map<Object, Object> result = cacheService.getAllHashFields(key, 0);

        assertTrue(result.isEmpty());
    }

    @Test
    void incrementIfExists_Success() {
        String key = "counter";
        long delta = 1L;

        when(jedis.exists(key)).thenReturn(true);
        when(jedis.incrBy(key, delta)).thenReturn(6L);

        Long result = cacheService.incrementIfExists(key, delta, 0);

        assertEquals(6L, result);
    }

    @Test
    void incrementIfExists_KeyNotFound() {
        String key = "counter";
        long delta = 1L;

        when(jedis.exists(key)).thenReturn(false);

        Long result = cacheService.incrementIfExists(key, delta, 0);

        assertNull(result);
        verify(jedis, never()).incrBy(anyString(), anyLong());
    }

    @Test
    void incrementIfExists_Exception() {
        String key = "counter";
        long delta = 1L;

        when(jedis.exists(key)).thenThrow(new RuntimeException("Test exception"));

        Long result = cacheService.incrementIfExists(key, delta, 0);

        assertNull(result);
    }

    @Test
    void setIfAbsentWithTtl_Success() {
        String key = "karmaWalletBalance_user1";

        when(jedis.set(eq(key), eq("100"), any(redis.clients.jedis.params.SetParams.class))).thenReturn("OK");

        boolean result = cacheService.setIfAbsentWithTtl(key, 1, 100L, 300L);

        assertTrue(result);
        verify(jedis).select(1);
    }

    @Test
    void setIfAbsentWithTtl_KeyAlreadyExists_ReturnsFalse() {
        String key = "karmaWalletBalance_user1";

        // NX conflict - Redis returns a null bulk reply rather than "OK", not an exception.
        when(jedis.set(eq(key), anyString(), any(redis.clients.jedis.params.SetParams.class))).thenReturn(null);

        boolean result = cacheService.setIfAbsentWithTtl(key, 1, 100L, 300L);

        assertFalse(result);
    }

    @Test
    void setIfAbsentWithTtl_Exception_Throws() {
        String key = "karmaWalletBalance_user1";

        when(jedis.set(eq(key), anyString(), any(redis.clients.jedis.params.SetParams.class)))
                .thenThrow(new RuntimeException("Connection reset"));

        // A genuine Redis error must propagate rather than be swallowed into a false that looks
        // identical to "someone else already seeded it" - a caller that can't tell the two apart
        // could otherwise proceed to decrement a key that was never actually seeded.
        assertThrows(RuntimeException.class, () -> cacheService.setIfAbsentWithTtl(key, 1, 100L, 300L));
    }

    @Test
    void decrementBy_Success() {
        String key = "karmaWalletBalance_user1";

        when(jedis.decrBy(key, 50L)).thenReturn(50L);

        long result = cacheService.decrementBy(key, 1, 50L);

        assertEquals(50L, result);
        verify(jedis).select(1);
    }

    @Test
    void decrementBy_Exception_Throws() {
        String key = "karmaWalletBalance_user1";

        when(jedis.decrBy(key, 50L)).thenThrow(new RuntimeException("Connection reset"));

        assertThrows(RuntimeException.class, () -> cacheService.decrementBy(key, 1, 50L));
    }

    @Test
    void hsetIfAbsentWithTtl_Claimed_SetsTtl() {
        when(jedis.hsetnx("k", "f", "1")).thenReturn(1L);

        assertTrue(cacheService.hsetIfAbsentWithTtl("k", 1, "f", "1", 300L));
        verify(jedis).expire("k", 300L);
    }

    @Test
    void hsetIfAbsentWithTtl_AlreadyExists_ReturnsFalse() {
        when(jedis.hsetnx("k", "f", "1")).thenReturn(0L);

        assertFalse(cacheService.hsetIfAbsentWithTtl("k", 1, "f", "1", 300L));
        verify(jedis, never()).expire(anyString(), anyLong());
    }

    @Test
    void hsetIfAbsentWithTtl_Exception_Throws() {
        when(jedis.hsetnx("k", "f", "1")).thenThrow(new RuntimeException("boom"));

        assertThrows(RuntimeException.class, () -> cacheService.hsetIfAbsentWithTtl("k", 1, "f", "1", 300L));
    }

    @Test
    void hdelWithRetry_Success() {
        when(jedis.hdel("k", "f")).thenReturn(1L);

        assertTrue(cacheService.hdelWithRetry("k", 1, "f"));
        verify(jedis, times(1)).hdel("k", "f");
    }

    @Test
    void hdelWithRetry_FirstAttemptFails_RetriesAndSucceeds() {
        when(jedis.hdel("k", "f")).thenThrow(new RuntimeException("conn")).thenReturn(1L);

        assertTrue(cacheService.hdelWithRetry("k", 1, "f"));
        verify(jedis, times(2)).hdel("k", "f");
    }

    @Test
    void hdelWithRetry_BothAttemptsFail_ReturnsFalse() {
        when(jedis.hdel("k", "f")).thenThrow(new RuntimeException("conn"));

        assertFalse(cacheService.hdelWithRetry("k", 1, "f"));
        verify(jedis, times(2)).hdel("k", "f");
    }
}
