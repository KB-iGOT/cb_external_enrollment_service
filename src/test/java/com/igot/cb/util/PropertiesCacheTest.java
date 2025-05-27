package com.igot.cb.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import static org.junit.jupiter.api.Assertions.*;

class PropertiesCacheTest {

    private PropertiesCache propertiesCache;

    @BeforeEach
    void setUp() {
        propertiesCache = PropertiesCache.getInstance();
    }

    // @Test
    // void testGetProperty_ExistingKey() {
    //     propertiesCache.setProperty("testKey", "testValue");
    //     String value = propertiesCache.getProperty("testKey");
    //     assertEquals("testValue", value);
    // }

    // @Test
    // void testGetProperty_NonExistingKey() {
    //     String value = propertiesCache.getProperty("nonExistentKey");
    //     assertNull(value, "Expected null for a non-existing key");
    // }

    // @Test
    // void testSetProperty() {
    //     // Updated to use a valid setProperty method
    //     propertiesCache.setProperty("newKey", "newValue");
    //     String value = propertiesCache.getProperty("newKey");
    //     assertEquals("newValue", value);
    // }
}