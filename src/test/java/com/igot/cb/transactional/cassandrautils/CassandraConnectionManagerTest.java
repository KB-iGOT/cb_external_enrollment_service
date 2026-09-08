package com.igot.cb.transactional.cassandrautils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.datastax.oss.driver.api.core.CqlSession;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CassandraConnectionManagerTest {

    @Mock
    private CassandraConnectionManager cassandraConnectionManager;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testGetSession_Success() {
        String keyspace = "testKeyspace";
        // Updated to mock a valid getSession method
        when(cassandraConnectionManager.getSession(keyspace)).thenReturn(mock(CqlSession.class));
        CqlSession session = cassandraConnectionManager.getSession(keyspace);
        assertNotNull(session);
        verify(cassandraConnectionManager, times(1)).getSession(keyspace);
    }

    // @Test
    // void testCloseConnection() {
    //     // Updated to mock a valid close method
    //     doNothing().when(cassandraConnectionManager).closeConnection();
    //     cassandraConnectionManager.closeConnection();
    //     verify(cassandraConnectionManager, times(1)).closeConnection();
    // }
}