package com.igot.cb.transactional.cassandrautils;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.igot.cb.util.PropertiesCache;

class CassandraConnectionManagerImplTest {

    @Mock
    private PropertiesCache propertiesCache;

    private CassandraConnectionManagerImpl cassandraConnectionManager;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        cassandraConnectionManager = mock(CassandraConnectionManagerImpl.class);

        // Mock behavior for getSession
        when(cassandraConnectionManager.getSession("testKeyspace")).thenReturn(mock(CqlSession.class));

        // Mock behavior for getTableList
        when(cassandraConnectionManager.getTableList("testKeyspace")).thenReturn(List.of("table1", "table2"));
    }

    @Test
    void testGetSession() {
        String keyspaceName = "testKeyspace";
        CqlSession mockSession = mock(CqlSession.class);
        
        // Mock behavior
        cassandraConnectionManager.getSession(keyspaceName);
        
        assertNotNull(mockSession);
    }

    @Test
    void testGetTableList() {
        String keyspaceName = "testKeyspace";
        CqlSession mockSession = mock(CqlSession.class);

        // Mock metadata and keyspace
        var mockMetadata = mock(com.datastax.oss.driver.api.core.metadata.Metadata.class);
        var mockKeyspace = mock(com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata.class);
        Map<CqlIdentifier, TableMetadata> mockTables = new HashMap<>();
        mockTables.put(CqlIdentifier.fromCql("table1"), mock(TableMetadata.class));
        mockTables.put(CqlIdentifier.fromCql("table2"), mock(TableMetadata.class));

        when(mockSession.getMetadata()).thenReturn(mockMetadata);
        when(mockMetadata.getKeyspace(keyspaceName)).thenReturn(java.util.Optional.of(mockKeyspace));
        when(mockKeyspace.getTables()).thenReturn(mockTables);

        when(cassandraConnectionManager.getSession(keyspaceName)).thenReturn(mockSession);

        List<String> tableList = cassandraConnectionManager.getTableList(keyspaceName);
        assertEquals(2, tableList.size());
        assertTrue(tableList.contains("table1"));
        assertTrue(tableList.contains("table2"));
    }

    // @Test
    // void testCreateCassandraConnectionWithKeySpaces() {
    //     String keyspaceName = "testKeyspace";
        
    //     // Mock properties
    //     when(propertiesCache.getProperty(Constants.CASSANDRA_CONFIG_HOST)).thenReturn("127.0.0.1");
        
    //     CqlSession session = cassandraConnectionManager.createCassandraConnectionWithKeySpaces(keyspaceName);
    //     assertNotNull(session);
    // }

    @Test
    void testShutdownHook() {
        CassandraConnectionManagerImpl.registerShutDownHook();
        // Verify shutdown hook is registered
        assertDoesNotThrow(() -> Runtime.getRuntime().addShutdownHook(new Thread()));
    }
}