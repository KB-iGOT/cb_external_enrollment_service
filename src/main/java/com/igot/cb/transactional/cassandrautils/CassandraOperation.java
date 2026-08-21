package com.igot.cb.transactional.cassandrautils;

import com.igot.cb.util.ApiResponse;

import java.util.List;
import java.util.Map;

/**
 * @author Mahesh RV
 * @author Ruksana
 * Interface defining Cassandra operations for querying records.
 */

public interface CassandraOperation {
    /**
     * Inserts a record into Cassandra.
     *
     * @param keyspaceName The name of the keyspace containing the table.
     * @param tableName    The name of the table into which to insert the record.
     * @param request      A map representing the record to insert.
     * @return An object representing the result of the insertion operation.
     */
     ApiResponse insertRecord(String keyspaceName, String tableName, Map<String, Object> request);

     List<Map<String, Object>> getRecordsByPropertiesWithoutFiltering(String keyspaceName, String tableName,
                                                                            Map<String, Object> propertyMap, List<String> fields, Integer limit);

     Map<String, Object> updateRecord(String keyspaceName, String tableName, Map<String, Object> updateAttributes,
        Map<String, Object> compositeKey);

    /**
     * Retrieves records from Cassandra based on specified properties and key.
     *
     * @param keyspaceName The name of the keyspace containing the table.
     * @param tableName    The name of the table from which to retrieve records.
     * @param propertyMap  A map representing properties to filter records.
     * @param fields       A list of fields to include in the retrieved records.
     * @return A list of maps representing the retrieved records.
     */

     List<Map<String, Object>> getRecordsByProperties(String keyspaceName, String tableName,
                                                     Map<String, Object> propertyMap, List<String> fields);

    /**
     * Inserts a record only if a row with the same primary key does not already exist
     * (a Cassandra lightweight transaction / IF NOT EXISTS). Used as the dedup gate for the
     * authoritative enrolment record, so a retried/duplicate request cannot be applied twice.
     *
     * @return a map containing a boolean "applied" key - true only if this call actually inserted
     *         the row; false if a row with that key already existed (nothing was changed).
     */
    Map<String, Object> insertRecordIfNotExists(String keyspaceName, String tableName, Map<String, Object> request);

    /**
     * Increments one or more counter columns on a counter table by the given deltas
     * (e.g. {"value": 1L}) for the row identified by compositeKey. Counter columns cannot be
     * set to a literal value - only incremented/decremented - so this is separate from updateRecord.
     */
    void incrementCounter(String keyspaceName, String tableName, Map<String, Object> compositeKey,
                           Map<String, Long> counterDeltas);

}
