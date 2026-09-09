package com.igot.cb.transactional.cassandrautils;


import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ApiResponse;

import java.util.*;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


/**
 * @author Mahesh RV
 * @author Ruksana
 */
@Component
@Slf4j
public class CassandraOperationImpl implements CassandraOperation {

    @Autowired
    CassandraConnectionManager connectionManager;

    @Override
    public ApiResponse insertRecord(String keyspaceName, String tableName, Map<String, Object> request) {
        ApiResponse response = new ApiResponse();
        CqlSession session = null;
        try {
            session = connectionManager.getSession(keyspaceName);
            String query = CassandraUtil.getPreparedStatement(keyspaceName, tableName, request);
            PreparedStatement statement = session.prepare(query);
            BoundStatement boundStatement = statement.bind(request.values().toArray());
            session.execute(boundStatement);
            response.put(Constants.RESPONSE, Constants.SUCCESS);
        } catch (Exception e) {
            log.error("Error inserting record into {}: {}", tableName, e.getMessage());
            response.put(Constants.RESPONSE, Constants.FAILED);
            response.put(Constants.ERROR_MESSAGE, e.getMessage());
        }
        return response;
    }

    @Override
    public List<Map<String, Object>> getRecordsByPropertiesWithoutFiltering(String keyspaceName, String tableName, Map<String, Object> propertyMap, List<String> fields, Integer limit) {
        List<Map<String, Object>> response = new ArrayList<>();
        CqlSession session = null;
        try {
            session = connectionManager.getSession(keyspaceName);
            Select selectQuery = null;
            selectQuery = processQuery(keyspaceName, tableName, propertyMap, fields);

            if (limit != null) selectQuery = selectQuery.limit(limit);
            String queryString = selectQuery.toString();
            SimpleStatement statement = SimpleStatement.newInstance(queryString);
            if (requiresQuorum(propertyMap)) {
                statement = statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
            }
            ResultSet results = session.execute(statement);
            response = CassandraUtil.createResponse(results);
        } catch (Exception e) {
            log.error("Error fetching records from {}: {}", tableName, e.getMessage());
        }
        return response;
    }

    private boolean requiresQuorum(Map<String, Object> propertyMap) {
        if (propertyMap == null) {
            return false;
        }
        Object scopeType = propertyMap.get(Constants.SCOPE_TYPE);
        return Constants.SCOPE_TYPE_TOTAL_ENROLMENTS.equals(scopeType)
                || Constants.SCOPE_TYPE_COURSE_ENROLMENTS.equals(scopeType);
    }

    @Override
    public List<Map<String, Object>> getRecordsByProperties(String keyspaceName, String tableName, Map<String, Object> propertyMap, List<String> fields) {
        List<Map<String, Object>> response = new ArrayList<>();
        CqlSession session = null;
        try {
            session = connectionManager.getSession(keyspaceName);
            Select selectQuery = null;
            selectQuery = processQuery(keyspaceName, tableName, propertyMap, fields);
            String queryString = selectQuery.toString();
            SimpleStatement statement = SimpleStatement.newInstance(queryString);
            ResultSet results = session.execute(statement);
            response = CassandraUtil.createResponse(results);
        } catch (Exception e) {
            log.error("Error fetching records from {}: {}", tableName, e.getMessage());
        }
        return response;
    }

    @Override
    public Map<String, Object> updateRecord(String keyspaceName, String tableName, Map<String, Object> updateAttributes,
                                            Map<String, Object> compositeKey) {
        Map<String, Object> response = new HashMap<>();
        CqlSession session = null;
        try {
            session = connectionManager.getSession(keyspaceName);
            UpdateStart updateStart = QueryBuilder.update(keyspaceName, tableName);
            UpdateWithAssignments updateWithAssignments = updateStart.set(
                    updateAttributes.entrySet().stream()
                            .map(entry -> Assignment.setColumn(entry.getKey(), QueryBuilder.literal(entry.getValue())))
                            .toArray(Assignment[]::new)
            );
            Update update = updateWithAssignments.where(
                    compositeKey.entrySet().stream()
                            .map(entry -> Relation.column(entry.getKey()).isEqualTo(QueryBuilder.literal(entry.getValue())))
                            .toArray(Relation[]::new)
            );
            session.execute(update.build());
            response.put(Constants.RESPONSE, Constants.SUCCESS);
        } catch (Exception e) {
            String errMsg = String.format("Exception occurred while updating record to %s %s", tableName, e.getMessage());
            log.error(errMsg);
            response.put(Constants.RESPONSE, Constants.FAILED);
            response.put(Constants.ERROR_MESSAGE, errMsg);
            throw e;
        }
        return response;
    }

    @Override
    public Map<String, Object> insertRecordIfNotExists(String keyspaceName, String tableName, Map<String, Object> request) {
        Map<String, Object> response = new HashMap<>();
        CqlSession session = null;
        try {
            session = connectionManager.getSession(keyspaceName);
            Iterator<Map.Entry<String, Object>> iterator = request.entrySet().iterator();
            Map.Entry<String, Object> first = iterator.next();
            RegularInsert regularInsert = QueryBuilder.insertInto(keyspaceName, tableName)
                    .value(first.getKey(), QueryBuilder.literal(first.getValue()));
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                regularInsert = regularInsert.value(entry.getKey(), QueryBuilder.literal(entry.getValue()));
            }
            SimpleStatement statement = regularInsert.ifNotExists().build();
            ResultSet resultSet = session.execute(statement);
            boolean applied = resultSet.wasApplied();
            response.put(Constants.RESPONSE, applied ? Constants.SUCCESS : Constants.FAILED);
            response.put("applied", applied);
        } catch (Exception e) {
            log.error("Error inserting record (if not exists) into {}: {}", tableName, e.getMessage());
            response.put(Constants.RESPONSE, Constants.FAILED);
            response.put("applied", false);
            response.put(Constants.ERROR_MESSAGE, e.getMessage());
        }
        return response;
    }

    @Override
    public void incrementCounter(String keyspaceName, String tableName, Map<String, Object> compositeKey,
                                  Map<String, Long> counterDeltas) {
        CqlSession session = null;
        try {
            session = connectionManager.getSession(keyspaceName);
            UpdateStart updateStart = QueryBuilder.update(keyspaceName, tableName);
            UpdateWithAssignments updateWithAssignments = updateStart.set(
                    counterDeltas.entrySet().stream()
                            .map(entry -> Assignment.increment(entry.getKey(), QueryBuilder.literal(entry.getValue())))
                            .toArray(Assignment[]::new)
            );
            Update update = updateWithAssignments.where(
                    compositeKey.entrySet().stream()
                            .map(entry -> Relation.column(entry.getKey()).isEqualTo(QueryBuilder.literal(entry.getValue())))
                            .toArray(Relation[]::new)
            );
            session.execute(update.build());
        } catch (Exception e) {
            String errMsg = String.format("Exception occurred while incrementing counter in %s %s", tableName, e.getMessage());
            log.error(errMsg);
            throw e;
        }
    }

    @Override
    public void incrementCounters(String keyspaceName, String tableName, List<CounterIncrement> increments) {
        if (CollectionUtils.isEmpty(increments)) {
            return;
        }
        CqlSession session = null;
        try {
            session = connectionManager.getSession(keyspaceName);
            BatchStatementBuilder batchBuilder = BatchStatement.builder(DefaultBatchType.COUNTER);
            boolean quorum = false;
            for (CounterIncrement increment : increments) {
                if (requiresQuorum(increment.getCompositeKey())) {
                    quorum = true;
                }
                UpdateStart updateStart = QueryBuilder.update(keyspaceName, tableName);
                UpdateWithAssignments updateWithAssignments = updateStart.set(
                        increment.getCounterDeltas().entrySet().stream()
                                .map(entry -> Assignment.increment(entry.getKey(), QueryBuilder.literal(entry.getValue())))
                                .toArray(Assignment[]::new)
                );
                Update update = updateWithAssignments.where(
                        increment.getCompositeKey().entrySet().stream()
                                .map(entry -> Relation.column(entry.getKey()).isEqualTo(QueryBuilder.literal(entry.getValue())))
                                .toArray(Relation[]::new)
                );
                batchBuilder.addStatement(update.build());
            }
            // A batch's consistency level is uniform across every statement in it - if any
            // increment in this batch is totalEnrolments/courseEnrolments, the whole batch runs
            // at QUORUM (see EnrollmentServiceImpl#updateEnrolmentCountersImmediately, which
            // never mixes those with userEnrolments in the same batch any more).
            if (quorum) {
                batchBuilder.setConsistencyLevel(ConsistencyLevel.QUORUM);
            }
            session.execute(batchBuilder.build());
        } catch (Exception e) {
            String errMsg = String.format("Exception occurred while batch incrementing counters in %s %s", tableName, e.getMessage());
            log.error(errMsg);
            throw e;
        }
    }

    private Select processQuery(String keyspaceName, String tableName, Map<String, Object> propertyMap,
                               List<String> fields) {
        Select select;
        if (CollectionUtils.isNotEmpty(fields)) {
            select = QueryBuilder.selectFrom(keyspaceName, tableName).columns(fields);
        } else {

            select = QueryBuilder.selectFrom(keyspaceName, tableName).all();
        }
        if (MapUtils.isEmpty(propertyMap)) {
            return select; // Build and return the query
        }
        for (Map.Entry<String, Object> entry : propertyMap.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof List) {
                List<?> valueList = (List<?>) value;
                if (CollectionUtils.isNotEmpty(valueList)) {
                    List<Term> terms = valueList.stream()
                            .map(QueryBuilder::literal)
                            .collect(Collectors.toList());
                    select = select.whereColumn(columnName).in(terms);
                }
            } else {
                select = select.whereColumn(columnName).isEqualTo(QueryBuilder.literal(value));
            }
        }
        return select;
    }
}
