
package com.igot.cb.util.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

class CacheServiceTest {

    @InjectMocks
    private CacheService cacheService;

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private final long CACHE_TTL = 100L;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        ReflectionTestUtils.setField(cacheService, "cacheTtl", CACHE_TTL);
    }

    @Test
    void testPutCache_Success() throws Exception {
        String key = "test-key";
        SomeObject obj = new SomeObject("val1");
        String objAsString = "{\"field\":\"val1\"}";

        when(objectMapper.writeValueAsString(obj)).thenReturn(objAsString);

        cacheService.putCache(key, obj);

        verify(objectMapper, times(1)).writeValueAsString(obj);
        verify(valueOperations, times(1)).set(key, objAsString, CACHE_TTL, java.util.concurrent.TimeUnit.SECONDS);
    }

    @Test
    void testPutCache_Exception() throws Exception {
        String key = "test-key";
        SomeObject obj = new SomeObject("val1");

        when(objectMapper.writeValueAsString(obj)).thenThrow(new RuntimeException("serialize error"));

        // Should not throw, should log error
        assertDoesNotThrow(() -> cacheService.putCache(key, obj));
        verify(objectMapper, times(1)).writeValueAsString(obj);
        // set should not be called
        verify(valueOperations, times(0)).set(anyString(), anyString(), anyLong(), any());
    }

    @Test
    void testGetCache_Success() {
        String key = "mykey";
        String value = "json-string-value";
        when(valueOperations.get(key)).thenReturn(value);

        String result = cacheService.getCache(key);

        assertEquals(value, result);
        verify(valueOperations, times(1)).get(key);
    }

    @Test
    void testGetCache_Exception() {
        String key = "mykey";
        when(valueOperations.get(key)).thenThrow(new RuntimeException("redis error"));

        String result = cacheService.getCache(key);

        assertNull(result);
    }

    @Test
    void testDeleteCache_Success() {
        String key = "mykey";
        when(redisTemplate.delete(key)).thenReturn(true);

        Boolean result = cacheService.deleteCache(key);

        // As per current implementation, should always return null
        assertNull(result);

        verify(redisTemplate, times(1)).delete(key);
    }

    @Test
    void testDeleteCache_NotFound() {
        String key = "mykey";
        when(redisTemplate.delete(key)).thenReturn(false);

        Boolean result = cacheService.deleteCache(key);

        assertNull(result);
        verify(redisTemplate, times(1)).delete(key);
    }

    // Helper POJO for serialization/deserialization in test
    static class SomeObject {
        public String field;

        public SomeObject() {}

        public SomeObject(String field) {
            this.field = field;
        }

        // equals, hashCode, toString can be added if needed
    }
}
