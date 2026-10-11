package com.igot.cb.util.dto;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SBApiResponseTest {

    @Test
    void testIdConstructor() {
        SBApiResponse response = new SBApiResponse("api1");
        assertEquals("api1", response.getId());
        assertEquals("v1", response.getVer());
        assertNotNull(response.getTs());
        assertNotNull(response.getParams());
    }

    @Test
    void testResultMapOperations() {
        SBApiResponse response = new SBApiResponse("api1");
        response.setResponseCode(HttpStatus.OK);

        response.put("key1", "value1");
        assertEquals("value1", response.get("key1"));
        assertTrue(response.containsKey("key1"));
        assertFalse(response.containsKey("missingKey"));

        response.putAll(Map.of("key2", "value2"));
        assertEquals("value2", response.get("key2"));

        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    @Test
    void testSetResult() {
        SBApiResponse response = new SBApiResponse("api1");
        Map<String, Object> result = Map.of("a", 1);
        response.setResult(result);
        assertEquals(result, response.getResult());
    }
}
