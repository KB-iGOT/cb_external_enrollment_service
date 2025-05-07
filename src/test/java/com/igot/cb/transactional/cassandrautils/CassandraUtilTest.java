package com.igot.cb.transactional.cassandrautils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Constructor;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CassandraUtilTest {

    @Mock
    private ResultSet mockResultSet;
    
    @Mock
    private ColumnDefinitions mockColumnDefinitions;
    
    @Mock
    private ColumnDefinition mockColumnDefinition;
    
    @Mock
    private Row mockRow;
    
    @Mock
    private CassandraPropertyReader mockPropertyReader;

    @BeforeEach
    void setUp() {
        // No setup needed
    }

    @Test
    void getPreparedStatement() {
        // Arrange
        String keyspaceName = "testKeyspace";
        String tableName = "testTable";
        Map<String, Object> map = new LinkedHashMap<>(); // LinkedHashMap to preserve order
        map.put("id", "123");
        map.put("name", "Test");
        
        // Act
        String query = CassandraUtil.getPreparedStatement(keyspaceName, tableName, map);
        
        // Assert - match the actual implementation which includes spaces and semicolon
        assertEquals("INSERT INTO testKeyspace.testTable(id,name) VALUES (?,?);", query);
    }
    
    @Test
    void getPreparedStatement_EmptyMap() {
        // Arrange
        String keyspaceName = "testKeyspace";
        String tableName = "testTable";
        Map<String, Object> map = new LinkedHashMap<>(); // Empty map
        
        // Act
        String query = CassandraUtil.getPreparedStatement(keyspaceName, tableName, map);
        
        // Assert - match the actual implementation which includes spaces and semicolon
        assertEquals("INSERT INTO testKeyspace.testTable() VALUES ();", query);
    }

    @Test
    void getPreparedStatement_SingleEntry() {
        // Arrange
        String keyspaceName = "testKeyspace";
        String tableName = "testTable";
        Map<String, Object> map = new LinkedHashMap<>(); // Single entry
        map.put("id", "123");
        
        // Act
        String query = CassandraUtil.getPreparedStatement(keyspaceName, tableName, map);
        
        // Assert
        assertEquals("INSERT INTO testKeyspace.testTable(id) VALUES (?);", query);
    }

    @Test
    void createResponse_List() {
        // Arrange
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(mockColumnDefinition);
        
        when(mockColumnDefinition.getName()).thenReturn(CqlIdentifier.fromCql("id"));
        
        try (MockedStatic<CassandraPropertyReader> propertyReaderMockedStatic = Mockito.mockStatic(CassandraPropertyReader.class)) {
            propertyReaderMockedStatic.when(CassandraPropertyReader::getInstance).thenReturn(mockPropertyReader);
            when(mockPropertyReader.readProperty("id")).thenReturn("userId");
            
            // Force the forEach to execute by mocking the behavior
            doAnswer(invocation -> {
                java.util.function.Consumer<ColumnDefinition> consumer = invocation.getArgument(0);
                for (ColumnDefinition def : columnDefinitions) {
                    consumer.accept(def);
                }
                return null;
            }).when(mockColumnDefinitions).forEach(any());
            
            List<Row> rows = new ArrayList<>();
            rows.add(mockRow);
            when(mockResultSet.iterator()).thenReturn(rows.iterator());
            
            // Return 123 as the value for the id column
            when(mockRow.getObject("id")).thenReturn(123);
            
            // Act
            List<Map<String, Object>> response = CassandraUtil.createResponse(mockResultSet);
            
            // Assert
            assertEquals(1, response.size());
            assertEquals(123, response.get(0).get("userId"));
        }
    }
    
    @Test
    void createResponse_List_EmptyResults() {
        // Arrange
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(mockColumnDefinition);
        when(mockColumnDefinition.getName()).thenReturn(CqlIdentifier.fromCql("id"));

        try (MockedStatic<CassandraPropertyReader> propertyReaderMockedStatic = Mockito.mockStatic(CassandraPropertyReader.class)) {
            propertyReaderMockedStatic.when(CassandraPropertyReader::getInstance).thenReturn(mockPropertyReader);
            //when(mockPropertyReader.readProperty("id")).thenReturn("userId");

            // Force the forEach to execute by mocking the behavior
            doAnswer(invocation -> {
                java.util.function.Consumer<ColumnDefinition> consumer = invocation.getArgument(0);
                for (ColumnDefinition def : columnDefinitions) {
                    consumer.accept(def);
                }
                return null;
            }).when(mockColumnDefinitions).forEach(any());

            // Empty list of rows
            List<Row> rows = new ArrayList<>();
            when(mockResultSet.iterator()).thenReturn(rows.iterator());

            // Act
            List<Map<String, Object>> response = CassandraUtil.createResponse(mockResultSet);

            // Assert
            assertTrue(response.isEmpty());
        }
    }

    @Test
    void createResponse_List_MultipleRows() {
        // Arrange
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(mockColumnDefinition);
        
        when(mockColumnDefinition.getName()).thenReturn(CqlIdentifier.fromCql("id"));
        
        try (MockedStatic<CassandraPropertyReader> propertyReaderMockedStatic = Mockito.mockStatic(CassandraPropertyReader.class)) {
            propertyReaderMockedStatic.when(CassandraPropertyReader::getInstance).thenReturn(mockPropertyReader);
            when(mockPropertyReader.readProperty("id")).thenReturn("userId");
            
            // Force the forEach to execute by mocking the behavior
            doAnswer(invocation -> {
                java.util.function.Consumer<ColumnDefinition> consumer = invocation.getArgument(0);
                for (ColumnDefinition def : columnDefinitions) {
                    consumer.accept(def);
                }
                return null;
            }).when(mockColumnDefinitions).forEach(any());
            
            // Create multiple rows
            Row mockRow2 = mock(Row.class);
            List<Row> rows = new ArrayList<>();
            rows.add(mockRow);
            rows.add(mockRow2);
            when(mockResultSet.iterator()).thenReturn(rows.iterator());
            
            // Return different values for each row
            when(mockRow.getObject("id")).thenReturn(123);
            when(mockRow2.getObject("id")).thenReturn(456);
            
            // Act
            List<Map<String, Object>> response = CassandraUtil.createResponse(mockResultSet);
            
            // Assert
            assertEquals(2, response.size());
            assertEquals(123, response.get(0).get("userId"));
            assertEquals(456, response.get(1).get("userId"));
        }
    }

    @Test
    void createResponse_List_MultipleColumns() {
        // Arrange
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        
        ColumnDefinition mockColumnDefinition2 = mock(ColumnDefinition.class);
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(mockColumnDefinition);
        columnDefinitions.add(mockColumnDefinition2);
        
        when(mockColumnDefinition.getName()).thenReturn(CqlIdentifier.fromCql("id"));
        when(mockColumnDefinition2.getName()).thenReturn(CqlIdentifier.fromCql("name"));
        
        try (MockedStatic<CassandraPropertyReader> propertyReaderMockedStatic = Mockito.mockStatic(CassandraPropertyReader.class)) {
            propertyReaderMockedStatic.when(CassandraPropertyReader::getInstance).thenReturn(mockPropertyReader);
            when(mockPropertyReader.readProperty("id")).thenReturn("userId");
            when(mockPropertyReader.readProperty("name")).thenReturn("userName");
            
            // Force the forEach to execute by mocking the behavior
            doAnswer(invocation -> {
                java.util.function.Consumer<ColumnDefinition> consumer = invocation.getArgument(0);
                for (ColumnDefinition def : columnDefinitions) {
                    consumer.accept(def);
                }
                return null;
            }).when(mockColumnDefinitions).forEach(any());
            
            List<Row> rows = new ArrayList<>();
            rows.add(mockRow);
            when(mockResultSet.iterator()).thenReturn(rows.iterator());
            
            // Return values for both columns
            when(mockRow.getObject("id")).thenReturn(123);
            when(mockRow.getObject("name")).thenReturn("John Doe");
            
            // Act
            List<Map<String, Object>> response = CassandraUtil.createResponse(mockResultSet);
            
            // Assert
            assertEquals(1, response.size());
            Map<String, Object> rowMap = response.get(0);
            assertEquals(2, rowMap.size());
            assertEquals(123, rowMap.get("userId"));
            assertEquals("John Doe", rowMap.get("userName"));
        }
    }

    @Test
    void createResponse_Map() {
        // Arrange
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(mockColumnDefinition);
        
        when(mockColumnDefinition.getName()).thenReturn(CqlIdentifier.fromCql("id"));
        
        try (MockedStatic<CassandraPropertyReader> propertyReaderMockedStatic = Mockito.mockStatic(CassandraPropertyReader.class)) {
            propertyReaderMockedStatic.when(CassandraPropertyReader::getInstance).thenReturn(mockPropertyReader);
            when(mockPropertyReader.readProperty("id")).thenReturn("userId");
            
            // Force the forEach to execute by mocking the behavior
            doAnswer(invocation -> {
                java.util.function.Consumer<ColumnDefinition> consumer = invocation.getArgument(0);
                for (ColumnDefinition def : columnDefinitions) {
                    consumer.accept(def);
                }
                return null;
            }).when(mockColumnDefinitions).forEach(any());
            
            List<Row> rows = new ArrayList<>();
            rows.add(mockRow);
            when(mockResultSet.iterator()).thenReturn(rows.iterator());
            
            // Return "123" as the value for the id column
            when(mockRow.getObject("id")).thenReturn("123");
            
            // Act
            Map<String, Object> response = CassandraUtil.createResponse(mockResultSet, "userId");
            
            // Assert
            assertEquals(1, response.size());
            assertTrue(response.containsKey("123"));
            Map<String, Object> rowMap = (Map<String, Object>) response.get("123");
            assertEquals("123", rowMap.get("userId"));
        }
    }
    
    @Test
    void createResponse_Map_EmptyResults() {
        // Arrange
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(mockColumnDefinition);
        
        when(mockColumnDefinition.getName()).thenReturn(CqlIdentifier.fromCql("id"));
        
        try (MockedStatic<CassandraPropertyReader> propertyReaderMockedStatic = Mockito.mockStatic(CassandraPropertyReader.class)) {
            propertyReaderMockedStatic.when(CassandraPropertyReader::getInstance).thenReturn(mockPropertyReader);
            lenient().when(mockPropertyReader.readProperty("id")).thenReturn("userId");
            
            // Force the forEach to execute by mocking the behavior
            doAnswer(invocation -> {
                java.util.function.Consumer<ColumnDefinition> consumer = invocation.getArgument(0);
                for (ColumnDefinition def : columnDefinitions) {
                    consumer.accept(def);
                }
                return null;
            }).when(mockColumnDefinitions).forEach(any());
            
            // Empty list of rows
            List<Row> rows = new ArrayList<>();
            when(mockResultSet.iterator()).thenReturn(rows.iterator());
            
            // Act
            Map<String, Object> response = CassandraUtil.createResponse(mockResultSet, "userId");
            
            // Assert
            assertTrue(response.isEmpty());
        }
    }
    
    @Test
    void createResponse_Map_NullKey() {
        // Arrange
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(mockColumnDefinition);
        
        when(mockColumnDefinition.getName()).thenReturn(CqlIdentifier.fromCql("id"));
        
        try (MockedStatic<CassandraPropertyReader> propertyReaderMockedStatic = Mockito.mockStatic(CassandraPropertyReader.class)) {
            propertyReaderMockedStatic.when(CassandraPropertyReader::getInstance).thenReturn(mockPropertyReader);
            lenient().when(mockPropertyReader.readProperty("id")).thenReturn("userId");
            
            // Force the forEach to execute by mocking the behavior
            doAnswer(invocation -> {
                java.util.function.Consumer<ColumnDefinition> consumer = invocation.getArgument(0);
                for (ColumnDefinition def : columnDefinitions) {
                    consumer.accept(def);
                }
                return null;
            }).when(mockColumnDefinitions).forEach(any());
            
            List<Row> rows = new ArrayList<>();
            rows.add(mockRow);
            when(mockResultSet.iterator()).thenReturn(rows.iterator());
            
            // Return null as the value for the id column
            when(mockRow.getObject("id")).thenReturn(null);
            
            // Act
            Map<String, Object> response = CassandraUtil.createResponse(mockResultSet, "userId");
            
            // Assert - the implementation actually adds a null key entry
            assertEquals(1, response.size());
            assertTrue(response.containsKey(null));
            Map<String, Object> rowMap = (Map<String, Object>) response.get(null);
            assertNotNull(rowMap);
            assertNull(rowMap.get("userId"));
        }
    }

    @Test
    void createResponse_Map_MultipleRows() {
        // Arrange
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(mockColumnDefinition);
        
        when(mockColumnDefinition.getName()).thenReturn(CqlIdentifier.fromCql("id"));
        
        try (MockedStatic<CassandraPropertyReader> propertyReaderMockedStatic = Mockito.mockStatic(CassandraPropertyReader.class)) {
            propertyReaderMockedStatic.when(CassandraPropertyReader::getInstance).thenReturn(mockPropertyReader);
            when(mockPropertyReader.readProperty("id")).thenReturn("userId");
            
            // Force the forEach to execute by mocking the behavior
            doAnswer(invocation -> {
                java.util.function.Consumer<ColumnDefinition> consumer = invocation.getArgument(0);
                for (ColumnDefinition def : columnDefinitions) {
                    consumer.accept(def);
                }
                return null;
            }).when(mockColumnDefinitions).forEach(any());
            
            // Create multiple rows
            Row mockRow2 = mock(Row.class);
            List<Row> rows = new ArrayList<>();
            rows.add(mockRow);
            rows.add(mockRow2);
            when(mockResultSet.iterator()).thenReturn(rows.iterator());
            
            // Return different values for each row
            when(mockRow.getObject("id")).thenReturn("123");
            when(mockRow2.getObject("id")).thenReturn("456");
            
            // Act
            Map<String, Object> response = CassandraUtil.createResponse(mockResultSet, "userId");
            
            // Assert
            assertEquals(2, response.size());
            assertTrue(response.containsKey("123"));
            assertTrue(response.containsKey("456"));
            assertEquals("123", ((Map<String, Object>)response.get("123")).get("userId"));
            assertEquals("456", ((Map<String, Object>)response.get("456")).get("userId"));
        }
    }

    @Test
    void fetchColumnsMapping() {
        // Arrange
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(mockColumnDefinition);
        
        when(mockColumnDefinition.getName()).thenReturn(CqlIdentifier.fromCql("id"));
        
        try (MockedStatic<CassandraPropertyReader> propertyReaderMockedStatic = Mockito.mockStatic(CassandraPropertyReader.class)) {
            propertyReaderMockedStatic.when(CassandraPropertyReader::getInstance).thenReturn(mockPropertyReader);
            // Make sure the property reader returns a non-null value
            when(mockPropertyReader.readProperty("id")).thenReturn("userId");
            
            // Force the forEach to execute by mocking the behavior
            doAnswer(invocation -> {
                java.util.function.Consumer<ColumnDefinition> consumer = invocation.getArgument(0);
                for (ColumnDefinition def : columnDefinitions) {
                    consumer.accept(def);
                }
                return null;
            }).when(mockColumnDefinitions).forEach(any());
            
            // Act
            Map<String, String> columnsMapping = CassandraUtil.fetchColumnsMapping(mockResultSet);
            
            // Assert - verify the map contains the expected entry
            assertNotNull(columnsMapping);
            assertEquals(1, columnsMapping.size());
            assertTrue(columnsMapping.containsKey("userId"));
            assertEquals("id", columnsMapping.get("userId"));
        }
    }
    
    @Test
    void fetchColumnsMapping_MultipleColumns() {
        // Arrange
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        
        ColumnDefinition mockColumnDefinition2 = mock(ColumnDefinition.class);
        
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(mockColumnDefinition);
        columnDefinitions.add(mockColumnDefinition2);
        
        when(mockColumnDefinition.getName()).thenReturn(CqlIdentifier.fromCql("id"));
        when(mockColumnDefinition2.getName()).thenReturn(CqlIdentifier.fromCql("name"));
        
        try (MockedStatic<CassandraPropertyReader> propertyReaderMockedStatic = Mockito.mockStatic(CassandraPropertyReader.class)) {
            propertyReaderMockedStatic.when(CassandraPropertyReader::getInstance).thenReturn(mockPropertyReader);
            // Return different values for different columns
            when(mockPropertyReader.readProperty("id")).thenReturn("userId");
            when(mockPropertyReader.readProperty("name")).thenReturn("userName");
            
            // Force the forEach to execute by mocking the behavior
            doAnswer(invocation -> {
                java.util.function.Consumer<ColumnDefinition> consumer = invocation.getArgument(0);
                for (ColumnDefinition def : columnDefinitions) {
                    consumer.accept(def);
                }
                return null;
            }).when(mockColumnDefinitions).forEach(any());
            
            // Act
            Map<String, String> columnsMapping = CassandraUtil.fetchColumnsMapping(mockResultSet);
            
            // Assert - verify the map contains the expected entries
            assertNotNull(columnsMapping);
            assertEquals(2, columnsMapping.size());
            assertTrue(columnsMapping.containsKey("userId"));
            assertTrue(columnsMapping.containsKey("userName"));
            assertEquals("id", columnsMapping.get("userId"));
            assertEquals("name", columnsMapping.get("userName"));
        }
    }
    
    @Test
    void fetchColumnsMapping_EmptyResults() {
        // Arrange
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        
        // Empty list of column definitions
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        
        try (MockedStatic<CassandraPropertyReader> propertyReaderMockedStatic = Mockito.mockStatic(CassandraPropertyReader.class)) {
            propertyReaderMockedStatic.when(CassandraPropertyReader::getInstance).thenReturn(mockPropertyReader);
            
            // Force the forEach to execute with empty list
            doAnswer(invocation -> {
                java.util.function.Consumer<ColumnDefinition> consumer = invocation.getArgument(0);
                for (ColumnDefinition def : columnDefinitions) {
                    consumer.accept(def);
                }
                return null;
            }).when(mockColumnDefinitions).forEach(any());
            
            // Act
            Map<String, String> columnsMapping = CassandraUtil.fetchColumnsMapping(mockResultSet);
            
            // Assert
            assertNotNull(columnsMapping);
            assertTrue(columnsMapping.isEmpty());
        }
    }

    @Test
    void fetchColumnsMapping_TrimPropertyValue() {
        // Arrange
        when(mockResultSet.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        
        List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        columnDefinitions.add(mockColumnDefinition);
        
        when(mockColumnDefinition.getName()).thenReturn(CqlIdentifier.fromCql("id"));
        
        try (MockedStatic<CassandraPropertyReader> propertyReaderMockedStatic = Mockito.mockStatic(CassandraPropertyReader.class)) {
            propertyReaderMockedStatic.when(CassandraPropertyReader::getInstance).thenReturn(mockPropertyReader);
            // Return a value with whitespace to test trim()
            when(mockPropertyReader.readProperty("id")).thenReturn("  userId  ");
            
            // Force the forEach to execute by mocking the behavior
            doAnswer(invocation -> {
                java.util.function.Consumer<ColumnDefinition> consumer = invocation.getArgument(0);
                for (ColumnDefinition def : columnDefinitions) {
                    consumer.accept(def);
                }
                return null;
            }).when(mockColumnDefinitions).forEach(any());
            
            // Act
            Map<String, String> columnsMapping = CassandraUtil.fetchColumnsMapping(mockResultSet);
            
            // Assert - verify the map contains the expected entry with trimmed key
            assertNotNull(columnsMapping);
            assertEquals(1, columnsMapping.size());
            assertTrue(columnsMapping.containsKey("userId"));
            assertEquals("id", columnsMapping.get("userId"));
        }
    }

    @Test
    void testPrivateConstructor() throws Exception {
        // Test the private constructor for code coverage
        Constructor<CassandraUtil> constructor = CassandraUtil.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        constructor.newInstance();
        constructor.setAccessible(false);
        
        // No assertion needed, just verifying it doesn't throw an exception
    }
}