package com.igot.cb.transactional.cassandrautils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CassandraConfigTest {

    @Mock
    private CassandraConfig cassandraConfig;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testGetContactPoints() {
        when(cassandraConfig.getContactPoints()).thenReturn("127.0.0.1");

        String contactPoints = cassandraConfig.getContactPoints();

        assertNotNull(contactPoints);
        assertEquals("127.0.0.1", contactPoints);
    }

    @Test
    void testGetPort() {
        when(cassandraConfig.getPort()).thenReturn(9042);

        int port = cassandraConfig.getPort();

        assertEquals(9042, port);
    }
}