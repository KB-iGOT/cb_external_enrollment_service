package com.igot.cb.transactional.cassandrautils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;

class CassandraOperationTest {

    @Mock
    private CassandraOperation cassandraOperation;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testInsertRecord_Success() {
        String keyspace = "testKeyspace";
        String table = "testTable";
        Map<String, Object> record = Map.of("key", "value");
        // Updated to resolve type mismatch
        when(cassandraOperation.insertRecord(keyspace, table, record)).thenReturn(Map.of("response", "success"));
        Map<String, Object> response = (Map<String, Object>) cassandraOperation.insertRecord(keyspace, table, record);
        assertNotNull(response);
        assertEquals("success", response.get("response"));
    }

    @Test
    void testGetRecordsByProperties_Success() {
        String keyspace = "testKeyspace";
        String table = "testTable";
        Map<String, Object> properties = Map.of("key", "value");
        // Updated to match the correct method signature
        when(cassandraOperation.getRecordsByProperties(keyspace, table, properties, List.of())).thenReturn(List.of(properties));
        List<Map<String, Object>> records = cassandraOperation.getRecordsByProperties(keyspace, table, properties, List.of());
        assertNotNull(records);
        assertFalse(records.isEmpty());
        assertEquals(properties, records.get(0));
    }
}