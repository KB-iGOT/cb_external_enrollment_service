package com.igot.cb.transactional.cassandrautils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.igot.cb.util.ApiResponse;
import com.igot.cb.util.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.MockedStatic;

@ExtendWith(MockitoExtension.class)
class CassandraOperationImplTest {

    @InjectMocks
    private CassandraOperationImpl cassandraOperation;

    @Mock
    private CassandraConnectionManager connectionManager;

    @Mock
    private CqlSession session;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private BoundStatement boundStatement;

    @Mock
    private ResultSet resultSet;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testInsertRecord_success() {
        String keyspace = "test_keyspace";
        String table = "test_table";
        Map<String, Object> request = new HashMap<>();
        request.put("id", "123");
        request.put("name", "Test");

        String insertQuery = "INSERT INTO test_table (id, name) VALUES (?, ?)";

        try (MockedStatic<CassandraUtil> cassandraUtilMockedStatic = mockStatic(CassandraUtil.class)) {
            cassandraUtilMockedStatic.when(() -> CassandraUtil.getPreparedStatement(keyspace, table, request)).thenReturn(insertQuery);

            when(connectionManager.getSession(keyspace)).thenReturn(session);
            when(session.prepare(insertQuery)).thenReturn(preparedStatement);
            when(preparedStatement.bind(request.values().toArray())).thenReturn(boundStatement);

            ApiResponse response = cassandraOperation.insertRecord(keyspace, table, request);

            assertEquals(Constants.SUCCESS, response.get(Constants.RESPONSE));
        }
    }

    @Test
    void testGetRecordsByProperties_success() {
        String keyspace = "test_keyspace";
        String table = "test_table";
        Map<String, Object> propertyMap = Map.of("id", "123");
        List<String> fields = Arrays.asList("id", "name");

        when(connectionManager.getSession(keyspace)).thenReturn(session);
        when(session.execute(any(SimpleStatement.class))).thenReturn(resultSet);

        try (MockedStatic<CassandraUtil> cassandraUtilMockedStatic = mockStatic(CassandraUtil.class)) {
            cassandraUtilMockedStatic.when(() -> CassandraUtil.createResponse(resultSet))
                    .thenReturn(List.of(Map.of("id", "123", "name", "Test")));

            List<Map<String, Object>> result = cassandraOperation.getRecordsByProperties(keyspace, table, propertyMap, fields);

            assertEquals(1, result.size());
            assertEquals("123", result.get(0).get("id"));
        }
    }

    @Test
    void testGetRecordsByPropertiesWithoutFiltering_withLimit() {
        String keyspace = "test_keyspace";
        String table = "test_table";
        Map<String, Object> propertyMap = Map.of("id", "123");
        List<String> fields = Arrays.asList("id", "name");
        int limit = 10;

        when(connectionManager.getSession(keyspace)).thenReturn(session);
        when(session.execute(any(SimpleStatement.class))).thenReturn(resultSet);

        try (MockedStatic<CassandraUtil> cassandraUtilMockedStatic = mockStatic(CassandraUtil.class)) {
            cassandraUtilMockedStatic.when(() -> CassandraUtil.createResponse(resultSet))
                    .thenReturn(List.of(Map.of("id", "123", "name", "Test")));

            List<Map<String, Object>> result = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    keyspace, table, propertyMap, fields, limit
            );

            assertEquals(1, result.size());
        }
    }

    @Test
    void testUpdateRecord_success() {
        String keyspace = "test_keyspace";
        String table = "test_table";
        Map<String, Object> updateAttributes = Map.of("name", "Updated");
        Map<String, Object> compositeKey = Map.of("id", "123");

        when(connectionManager.getSession(keyspace)).thenReturn(session);

        Map<String, Object> response = cassandraOperation.updateRecord(keyspace, table, updateAttributes, compositeKey);

        assertEquals(Constants.SUCCESS, response.get(Constants.RESPONSE));
    }
}
