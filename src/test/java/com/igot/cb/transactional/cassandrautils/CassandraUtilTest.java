package com.igot.cb.transactional.cassandrautils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CassandraUtilTest {

    @Mock
    private CassandraUtil cassandraUtil;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    // @Test
    // void testIsValidKeyspace_ValidKeyspace() {
    //     String keyspace = "validKeyspace";
    //     // Updated to mock a valid isValidKeyspace method
    //     when(cassandraUtil.isValidKeyspace(keyspace)).thenReturn(true);
    //     boolean isValid = cassandraUtil.isValidKeyspace(keyspace);
    //     assertTrue(isValid);
    // }

    // @Test
    // void testIsValidKeyspace_InvalidKeyspace() {
    //     String keyspace = "invalidKeyspace";
    //     // Updated to mock a valid isValidKeyspace method
    //     when(cassandraUtil.isValidKeyspace(keyspace)).thenReturn(false);
    //     boolean isValid = cassandraUtil.isValidKeyspace(keyspace);
    //     assertFalse(isValid);
    // }
}