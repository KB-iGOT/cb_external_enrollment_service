package com.igot.cb.transactional.cassandrautils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.test.util.ReflectionTestUtils;

import com.datastax.oss.driver.api.core.CqlSession;

@ExtendWith(MockitoExtension.class)
class SunbirdConfigTest {

    @InjectMocks
    private SunbirdConfig sunbirdConfig;

    @Mock
    private CqlSession mockSession;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(sunbirdConfig, "contactPoints", "localhost");
        ReflectionTestUtils.setField(sunbirdConfig, "port", 9042);
        ReflectionTestUtils.setField(sunbirdConfig, "keyspaceName", "sunbird");
        ReflectionTestUtils.setField(sunbirdConfig, "sunbirdUser", "user");
        ReflectionTestUtils.setField(sunbirdConfig, "sunbirdPassword", "password");
    }

    @Test
    void cassandraTemplate() {
        // Arrange
        SunbirdConfig spyConfig = spy(sunbirdConfig);

        // Mock dependencies
        CassandraConverter mockConverter = mock(CassandraConverter.class);
        CassandraMappingContext mockMappingContext = mock(CassandraMappingContext.class);
        SpelAwareProxyProjectionFactory mockProjectionFactory = new SpelAwareProxyProjectionFactory();

        // Return the mocks when required
        when(mockConverter.getMappingContext()).thenReturn(mockMappingContext);
        when(mockConverter.getProjectionFactory()).thenReturn(mockProjectionFactory); // ✅ fix

        // Spy config to return our mocked converter
        doReturn(mockConverter).when(spyConfig).cassandraConverter();

        // Act
        CassandraAdminTemplate template = spyConfig.cassandraTemplate(mockSession);

        // Assert
        assertNotNull(template);
    }

    @Test
    void getKeyspaceName() {
        // Act
        String keyspaceName = sunbirdConfig.getKeyspaceName();

        // Assert
        assertEquals("sunbird", keyspaceName);
    }

    @Test
    void getPort() {
        // Act
        int port = sunbirdConfig.getPort();

        // Assert
        assertEquals(9042, port);
    }

    @Test
    void getContactPoints() {
        // Act
        String contactPoints = sunbirdConfig.getContactPoints();

        // Assert
        assertEquals("localhost", contactPoints);
    }
}