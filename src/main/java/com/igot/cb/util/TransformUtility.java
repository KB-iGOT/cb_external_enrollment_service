package com.igot.cb.util;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.enrollment.model.AccessControl;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.util.cache.CacheService;
import com.igot.cb.util.dto.SBApiResponse;
import com.igot.cb.util.dto.SunbirdApiRespParam;
import com.igot.cb.util.exceptions.CustomException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.util.bcel.Const;
import org.joda.time.DateTime;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.util.*;

import static com.igot.cb.util.Constants.KEYSPACE_SUNBIRD_COURSES;

@Component
@Slf4j
public class TransformUtility {

    private final CbServerProperties cbServerProperties;
    private final RestTemplate restTemplate;
    private final ObjectMapper mapper;
    private final CassandraOperation cassandraOperation;
    private final CacheService cacheService;
    private final AccessTokenValidator accessTokenValidator;

    public TransformUtility(
            CbServerProperties cbServerProperties,
            RestTemplate restTemplate,
            ObjectMapper mapper,
            CassandraOperation cassandraOperation,
            CacheService cacheService,
            AccessTokenValidator accessTokenValidator) {

        this.cbServerProperties = cbServerProperties;
        this.restTemplate = restTemplate;
        this.mapper = mapper;
        this.cassandraOperation = cassandraOperation;
        this.cacheService = cacheService;
        this.accessTokenValidator = accessTokenValidator;
    }

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
        try {
            log.info("TransformUtility :: callCiosContentReadAPi");
            JsonNode jsonNode;
            String cachedJson = cacheService.getCache(contentId, cbServerProperties.getDefaultIndex());
            if (StringUtils.isNotEmpty(cachedJson)) {
                jsonNode = mapper.readTree(cachedJson);
                return jsonNode.get("content");
            }
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
                jsonNode = mapper.valueToTree(response.getBody());
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
        } catch (Exception e) {
            log.error("error while processing", e);
            throw new CustomException(Constants.ERROR, "Failed to retrieve contentId.", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public JsonNode callContentPartnerReadApi(String partnerId) {
        try {
            log.info("TransformUtility :: callContentPartnerReadApi");
            String cachedJson = cacheService.getCache(partnerId, cbServerProperties.getDefaultIndex());
            if (StringUtils.isNotEmpty(cachedJson)) {
                return mapper.readTree(cachedJson);
            }
            String url = cbServerProperties.getBaseUrl() + cbServerProperties.getContentPartnerReadApiUrl() + partnerId;
            HttpHeaders headers = new HttpHeaders();
            headers.setAccept(List.of(MediaType.APPLICATION_JSON));
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> entity = new HttpEntity<>(headers);
            ResponseEntity<JsonNode> response = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    entity,
                    JsonNode.class
            );
            if (response.getStatusCode().is2xxSuccessful()) {
                JsonNode body = response.getBody();
                if (body != null && body.has(Constants.RESULT)) {
                    return body.path(Constants.RESULT);
                } else {
                    log.error("CIOS read API returned null or missing 'result' field");
                    throw new CustomException(Constants.ERROR, "Invalid response body from CIOS read API", HttpStatus.INTERNAL_SERVER_ERROR);
                }
            } else {
                log.error("Failed to retrieve partnerId. Status code: {}", response.getStatusCode());
                throw new CustomException(Constants.ERROR, "Failed to retrieve partnerId", HttpStatus.BAD_REQUEST);
            }
        } catch (Exception e) {
            log.error("error while processing", e);
            throw new CustomException(Constants.ERROR, "Failed to retrieve externalId.", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public JsonNode callContentPartnerReadByPartnerCodeApi(String partnerCode) {
        log.info("KafkaConsumer :: callContentPartnerReadByPartnerCodeApi");
        String url = cbServerProperties.getBaseUrl() + cbServerProperties.getContentPartnerReadbyPartnerCodeApiUrl() + partnerCode;
        HttpHeaders headers = new HttpHeaders();
        headers.set(Constants.ACCEPT, Constants.APPLICATION_JSON); // Indicate JSON response
        headers.set(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<JsonNode> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                entity,
                JsonNode.class
        );
        if (response.getStatusCode().is2xxSuccessful()) {
            JsonNode jsonNode = response.getBody();
            if (jsonNode != null && jsonNode.has(Constants.RESULT)) {
                return jsonNode.path(Constants.RESULT);
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
        log.info("TransformUtility :: readUserDetails");
        try {
            String cachedJson = cacheService.getCache(Constants.UAER_DETAILS + userid, cbServerProperties.getRedisIndex());
            if (StringUtils.isNotBlank(cachedJson)) {
                return mapper.readValue(cachedJson, new TypeReference<Map<String, Object>>() {
                });
            }
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put("id", userid);
            List<Map<String, Object>> userEnrollmentList = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_USER,
                    propertyMap,
                    null
            );
            if (CollectionUtils.isNotEmpty(userEnrollmentList)) {
                cacheService.putCache(Constants.UAER_DETAILS + userid, cbServerProperties.getRedisIndex(), userEnrollmentList.get(0));
                return userEnrollmentList.get(0);
            }
            return Map.of();
        } catch (Exception e) {
            log.error("Error while fetching user details for userId: {} {}", userid, e);
            return Map.of();
        }
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

    public Long readUserKarmaPoints(String userId, String token) {
        log.info("TransformUtility :: readUserKarmaPoints");
        String cachedJson = cacheService.getCache(Constants.USER_KARMA_POINTS + userId, cbServerProperties.getDefaultIndex());
        if (StringUtils.isNotEmpty(cachedJson)) {
            log.info("TransformUtility::readUserKarmaPoints:Record coming from redis cache");
            return Long.valueOf(cachedJson);
        } else {
            String url = cbServerProperties.getLmsEnrolmentSummaryBaseUrl() + cbServerProperties.getLmsEnrolmentSummaryFixedUrl() + userId;
            HttpHeaders headers = new HttpHeaders();
            headers.set("Accept", "application/json");
            headers.set("Content-Type", "application/json");
            headers.set(Constants.X_AUTH_TOKEN, token);
            HttpEntity<String> entity = new HttpEntity<>(headers);
            ResponseEntity<JsonNode> response = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    entity,
                    JsonNode.class
            );
            if (response.getStatusCode().is2xxSuccessful()) {
                JsonNode jsonNode = response.getBody();
                if (jsonNode != null && jsonNode.has(Constants.RESULT)) {
                    return jsonNode.path(Constants.RESULT).path("userCourseEnrolmentInfo").path("karmaPoints").asLong(0);
                } else {
                    log.error("Response body is null or missing 'result' field");
                    throw new CustomException(Constants.ERROR, "Invalid or null response body", HttpStatus.INTERNAL_SERVER_ERROR);
                }
            } else {
                log.error("Failed to retrieve externalId. Status code: {}", response.getStatusCode());
                throw new CustomException(Constants.ERROR, "Failed to retrieve externalId", HttpStatus.BAD_REQUEST);
            }
        }
    }

    public boolean callCourseraInviteApi(JsonNode contentResponse, Map<String, Object> userProfile) {
        String userId = String.valueOf(userProfile.get(Constants.ID));
        try {
            log.info("TransformUtility :: callCourseraInviteApi");
            String url = cbServerProperties.getServiceRegistryApiBaseUrl() + cbServerProperties.getServiceRegistryApiFixedUrl();
            HttpHeaders headers = new HttpHeaders();
            headers.setAccept(List.of(MediaType.APPLICATION_JSON));
            headers.setContentType(MediaType.APPLICATION_JSON);
            String programId = contentResponse.path(Constants.PROGRAM_ID).asText("");

            if (StringUtils.isBlank(programId)) {
                log.error("ProgramId missing in content response");
                return false;
            }

            Map<String, Object> urlMap = new HashMap<>();
            urlMap.put(Constants.ORG_ID, cbServerProperties.getCourseraOrgId());
            urlMap.put(Constants.PROGRAM_ID, programId);

            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put(Constants.EXTERNAL_ID, userId);
            requestBody.put(Constants.FULLNAME, userProfile.get(Constants.FIRST_NAME));
            requestBody.put(Constants.EMAIL, userId + "@karmayogi.com");
            requestBody.put(Constants.SEND_EMAIL, Boolean.FALSE);

            Map<String, Object> payload = new HashMap<>();
            payload.put(Constants.URLMAP, urlMap);
            payload.put(Constants.REQUEST_BODY, requestBody);
            payload.put(Constants.SERVICE_CODE, cbServerProperties.getCourseraServiceCode());
            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(payload, headers);
            ResponseEntity<JsonNode> response = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    entity,
                    JsonNode.class
            );
            if (response.getStatusCode().is2xxSuccessful()) {
                log.warn("Successfully called Coursera Invite API for externalId: {}");
                return true;
            }
        } catch (HttpStatusCodeException ex) {
            String responseBody = ex.getResponseBodyAsString();
            log.error("Coursera Invite API error response: {}", responseBody);
            if (responseBody != null &&
                    responseBody.contains("PROGRAM_INVITEE_ERROR_EXISTING_INVITATION_FOR_EMAIL")) {
                log.info("Coursera invite already exists for email: {} and programId: {}",
                        userId,
                        contentResponse.get("programId"));
                return true;
            }
            throw new CustomException(
                    Constants.ERROR,
                    "Coursera Invite API failed: " + responseBody,
                    HttpStatus.BAD_GATEWAY
            );

        } catch (Exception e) {
            log.error("Unexpected error while calling Coursera Invite API", e);
            throw new CustomException(
                    Constants.ERROR,
                    "Failed to call Coursera Invite API",
                    HttpStatus.INTERNAL_SERVER_ERROR
            );
        }
        return false;
    }

    public String validateAndGetUserId(String token, SBApiResponse response) {
        String userId = accessTokenValidator.verifyUserToken(token);

        if (StringUtils.isBlank(userId)
                || Constants.UNAUTHORIZED.equalsIgnoreCase(userId)) {
            buildFailedResponse(
                    response,
                    Constants.USER_ID_DOESNT_EXIST,
                    HttpStatus.BAD_REQUEST
            );
            return null;
        }
        return userId;
    }

    public SBApiResponse buildFailedResponse(SBApiResponse response, String message, HttpStatus status) {
        response.getParams().setMsg(message);
        response.getParams().setStatus(Constants.FAILED);
        response.setResponseCode(status);
        return response;
    }

    public SBApiResponse buildSuccessResponse(SBApiResponse response, String message, HttpStatus status) {
        response.getParams().setMsg(message);
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(status);
        return response;
    }

    public JsonNode searchContentByExternalId(String externalId, String partnerCode) {
        try {
            ObjectNode filterCriteriaMap = mapper.createObjectNode();
            filterCriteriaMap.put("contentPartner.partnerCode", partnerCode);
            filterCriteriaMap.put(Constants.EXTERNAL_ID, externalId);

            ObjectNode requestBody = mapper.createObjectNode();
            requestBody.set(Constants.FILTER_CRITERIA_MAP, filterCriteriaMap);
            requestBody.putArray(Constants.REQUESTED_FIELDS).add(Constants.CONTENT_ID);
            requestBody.put(Constants.PAGE_NUMBER, 0);
            requestBody.put(Constants.PAGE_SIZE, 1);
            return postApiCall(requestBody);
        } catch (Exception e) {
            log.error("Error searching CIOS content for externalId: {}, partnerCode: {}", externalId, partnerCode, e);
            throw new CustomException(Constants.ERROR, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Resolves contentId for an externalId + partnerCode, redis-cache first, then CIOS search API.
     */
    public String getContentIdByExternalId(String externalId, String partnerCode) {
        String cacheKey = Constants.EXTERNAL_ID + externalId + Constants.PARTNER_CODE + partnerCode;

        String cachedContentId = cacheService.getCache(cacheKey, cbServerProperties.getRedisIndex());
        if (StringUtils.isNotBlank(cachedContentId)) {
            log.info("contentId for externalId: {}, partnerCode: {} served from redis cache", externalId, partnerCode);
            try {
                return mapper.readValue(cachedContentId, String.class);
            } catch (JsonProcessingException e) {
                throw new CustomException(Constants.ERROR, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }

        JsonNode searchResponse = searchContentByExternalId(externalId, partnerCode);
        if (searchResponse == null
                || !searchResponse.has(Constants.DATA)
                || searchResponse.get(Constants.DATA).isEmpty()) {
            log.error("No content found in CIOS for externalId: {}, partnerCode: {}", externalId, partnerCode);
            return null;
        }

        String contentId = searchResponse.get(Constants.DATA).path(0).path(Constants.CONTENT_ID).asText("");
        if (StringUtils.isNotBlank(contentId)) {
            cacheService.putCache(cacheKey, cbServerProperties.getRedisIndex(), contentId);
        }
        return contentId;
    }

    private JsonNode postApiCall(JsonNode requestBody) {
        try {
            String url = cbServerProperties.getBaseUrl() + cbServerProperties.getCiosSearchContentApiEndPoint();
            HttpHeaders headers = new HttpHeaders();
            headers.set(Constants.ACCEPT, Constants.APPLICATION_JSON);
            headers.set(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
            HttpEntity<JsonNode> entity = new HttpEntity<>(requestBody, headers);
            ResponseEntity<JsonNode> response = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    entity,
                    JsonNode.class
            );
            JsonNode responseBody = response.getBody();
            if (responseBody == null) {
                log.error("CIOS search content API returned null response body. Status: {}", response.getStatusCode());
                throw new CustomException(Constants.ERROR, "Empty response received from CIOS search content API", HttpStatus.INTERNAL_SERVER_ERROR);
            }
            return responseBody;
        } catch (Exception e) {
            log.error("Error calling CIOS search content API", e);
            throw new CustomException(Constants.ERROR, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
