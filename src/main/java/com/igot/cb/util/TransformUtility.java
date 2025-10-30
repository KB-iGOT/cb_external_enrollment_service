package com.igot.cb.util;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.igot.cb.enrollment.model.AccessControl;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.util.dto.SBApiResponse;
import com.igot.cb.util.dto.SunbirdApiRespParam;
import com.igot.cb.util.exceptions.CustomException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.*;

import static com.igot.cb.util.Constants.KEYSPACE_SUNBIRD_COURSES;
import static org.jclouds.cloudwatch.domain.DynamoDBConstants.Dimension.TABLE_NAME;

@Component
@Slf4j
public class TransformUtility {

    @Autowired
    private CbServerProperties cbServerProperties;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private CassandraOperation cassandraOperation;

    public JsonNode callCiosReadAPi(String extCourseId, String partnerId) {
        log.info("KafkaConsumer :: callCiosReadAPi");
        String url = cbServerProperties.getBaseUrl() + cbServerProperties.getCiosReadApiUrl() + extCourseId + "/" + partnerId;
        HttpHeaders headers = new HttpHeaders();
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<Object> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                entity,
                Object.class
        );
        if (response.getStatusCode().is2xxSuccessful()) {
            Object body = response.getBody();
            JsonNode jsonNode = body == null ? null : mapper.valueToTree(body);
            if (jsonNode != null) {
                return jsonNode;
            } else {
                log.error("CIOS read API returned null body");
                throw new CustomException(Constants.ERROR, "Received null response body from CIOS read API", HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } else {
            throw new CustomException(Constants.ERROR, "Failed to retrieve externalId. Status code: "
                    + response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public JsonNode callCiosContentReadAPi(String contentId) {
        log.info("KafkaConsumer :: callCiosContentReadAPi");
        String url = cbServerProperties.getBaseUrl() + cbServerProperties.getCiosContentReadApiUrl() + contentId;
        HttpHeaders headers = new HttpHeaders();
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<Object> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                entity,
                Object.class
        );
        if (response.getStatusCode().is2xxSuccessful()) {
            JsonNode jsonNode = mapper.valueToTree(response.getBody());
            if (jsonNode.has(Constants.CONTENT)) {
                log.warn("Successfully retrieved content for ID: {}", contentId);
                return jsonNode.get("content");
            } else {
                log.error("CIOS read API returned null body");
                throw new CustomException(Constants.ERROR, "Received null response body from CIOS read API", HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } else {
            throw new CustomException(Constants.ERROR, "Failed to retrieve contentId. Status code: "
                    + response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public JsonNode callContentPartnerReadApi(String partnerId) {
        log.info("KafkaConsumer :: callExtApi");
        String url = cbServerProperties.getBaseUrl() + cbServerProperties.getContentPartnerReadApiUrl() + partnerId;
        HttpHeaders headers = new HttpHeaders();
        headers.set("Accept", "application/json"); // Indicate JSON response
        headers.set("Content-Type", "application/json");
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<JsonNode> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                entity,
                JsonNode.class
        );
        if (response.getStatusCode().is2xxSuccessful()) {
            JsonNode body = response.getBody();
            if (body != null && body.has("result")) {
                return body.path("result");
            } else {
                log.error("CIOS read API returned null or missing 'result' field");
                throw new CustomException(Constants.ERROR, "Invalid response body from CIOS read API", HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } else {
            log.error("Failed to retrieve externalId. Status code: {}", response.getStatusCode());
            throw new CustomException(Constants.ERROR, "Failed to retrieve externalId", HttpStatus.BAD_REQUEST);
        }
    }

    public JsonNode callContentPartnerReadByPartnerCodeApi(String partnerCode) {
        log.info("KafkaConsumer :: callContentPartnerReadByPartnerCodeApi");
        String url = cbServerProperties.getBaseUrl() + cbServerProperties.getContentPartnerReadbyPartnerCodeApiUrl() + partnerCode;
        HttpHeaders headers = new HttpHeaders();
        headers.set("Accept", "application/json"); // Indicate JSON response
        headers.set("Content-Type", "application/json");
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<JsonNode> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                entity,
                JsonNode.class
        );
        if (response.getStatusCode().is2xxSuccessful()) {
            JsonNode jsonNode = response.getBody();
            if (jsonNode != null && jsonNode.has("result")) {
                return jsonNode.path("result");
            } else {
                log.error("Response body is null or missing 'result' field");
                throw new CustomException(Constants.ERROR, "Invalid or null response body", HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } else {
            log.error("Failed to retrieve externalId. Status code: {}", response.getStatusCode());
            throw new CustomException(Constants.ERROR, "Failed to retrieve externalId", HttpStatus.BAD_REQUEST);
        }

    }

    public JsonNode transformData(Object jsonNode, List<Object> contentJson) {
        log.debug("TransformUtility::transformData");
        try {
            String inputJson = mapper.writeValueAsString(jsonNode);
            Chainr chainr = Chainr.fromSpec(contentJson);
            Object transformedOutput = chainr.transform(JsonUtils.jsonToObject(inputJson));
            return mapper.convertValue(transformedOutput, JsonNode.class);
        } catch (JsonProcessingException e) {
            log.error("Error transforming data", e);
            return null;
        }
    }

    public SBApiResponse createDefaultResponse(String api) {
        SBApiResponse response = new SBApiResponse();
        response.setId(api);
        response.setVer(Constants.API_VERSION_1);
        response.setParams(new SunbirdApiRespParam(UUID.randomUUID().toString()));
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
        response.setTs(DateTime.now().toString());
        return response;
    }

    public Map<String, Object> readUserDetails(String userid) {
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("id", userid);
        List<Map<String, Object>> userEnrollmentList = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_USER,
                propertyMap,
                null
        );
        if (CollectionUtils.isNotEmpty(userEnrollmentList))
            return userEnrollmentList.get(0);
        return Map.of();
    }

    public AccessControl readAccessSettings(String courseId) {
        Map<String, Object> primaryKey = new HashMap<>();
        primaryKey.put(Constants.CONTEXT_ID, courseId);
        List<Map<String, Object>> accessSettingsList = cassandraOperation.getRecordsByProperties(KEYSPACE_SUNBIRD_COURSES, Constants.ACCESS_SETTINGS_RULE_TABLE,
                primaryKey, null);
        try {
            if (CollectionUtils.isNotEmpty(accessSettingsList)) {
                Map<String, Object> dbRecord = accessSettingsList.get(0);
                Map<String, Object> contextData = mapper
                        .readValue((String) dbRecord.get(Constants.CONTEXT_DATA), Map.class);
                if (contextData.containsKey(Constants.ACCESS_CONTROL)) {
                    return mapper.convertValue(contextData.get(Constants.ACCESS_CONTROL), AccessControl.class);
                }

            }
        } catch (Exception e) {
            log.error("Failed to read access settings for courseId: {} {}", courseId, e);
        }
        return null;
    }
}
