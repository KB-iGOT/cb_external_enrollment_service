package com.igot.cb.enrollment.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.enrollment.entity.CiosContentEntity;
import com.igot.cb.enrollment.entity.CiosEnrolmentStatus;
import com.igot.cb.enrollment.model.AccessControl;
import com.igot.cb.enrollment.model.KarmaValidationResult;
import com.igot.cb.enrollment.model.UserGroup;
import com.igot.cb.enrollment.model.UserGroupCriteria;
import com.igot.cb.enrollment.repository.CiosContentRepository;
import com.igot.cb.enrollment.service.EnrollmentService;
import com.igot.cb.producer.Producer;
import com.igot.cb.util.*;
import com.igot.cb.util.cache.CacheService;
import com.igot.cb.util.dto.*;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;


import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Supplier;

import com.igot.cb.util.exceptions.CustomException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;


@Service
@Slf4j
@RequiredArgsConstructor
public class EnrollmentServiceImpl implements EnrollmentService {

    private final AccessTokenValidator accessTokenValidator;

    private final CassandraOperation cassandraOperation;

    private final ObjectMapper objectMapper;

    private final CacheService cacheService;

    private final CbServerProperties cbServerProperties;

    private final CiosContentRepository contentRepository;

    private final TransformUtility transformUtility;

    private final Producer producer;

    private final PayloadValidation payloadValidation;

    private final Map<String, Integer> statusMap = CiosEnrolmentStatus.toMap();

    private KarmaValidationResult karmaValidationResult;

    @Override
    public SBApiResponse enrollUser(JsonNode userCourseEnroll, String token) {
        log.info("EnrollmentService::enrollUser:inside the method");
        SBApiResponse response = transformUtility.createDefaultResponse(Constants.CIOS_ENROLLMENT_CREATE);
        if (!validateRequest(userCourseEnroll, response)) {
            return response;
        }
        String partnerId = userCourseEnroll.get(Constants.PARTNER_ID).asText();
        String courseId = userCourseEnroll.get(Constants.COURSE_ID_RQST).asText();
        try {
            String userId = transformUtility.validateAndGetUserId(token,response);
            if(StringUtils.isBlank(userId)){
                return response;
            }

            if(isUserEnrolled(response, userId, courseId)){
                return response;
            }
            processEnrolment(response, userId, courseId, partnerId, token);

        } catch (Exception e) {
            String errMsg = Constants.ENROLLMENT_ERROR + e.getMessage();
            log.error(errMsg, e);
            return transformUtility.buildFailedResponse(response, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    @Override
    public SBApiResponse readByUserId(Map<String, Object> searchRequest, String token) {
        log.info("EnrollmentService::readByUserId:inside the method");
        SBApiResponse response = transformUtility.createDefaultResponse(Constants.CIOS_ENROLLMENT_READ_COURSELIST);
        try {
            Map<String, Object> request = (Map<String, Object>)searchRequest.get(Constants.REQUEST);
            if (MapUtils.isEmpty(request)) {
                response.getParams().setMsg("Request is not proper, please include request body.");
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            String status = (String) request.get(Constants.STATUS);
            if (StringUtils.isEmpty(status)) {
                response.getParams().setMsg("Request is not proper, please provide status in request body.");
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            if (statusMap.get(status) == null) {
                response.getParams().setMsg("Request is not proper, please provide proper value of status.");
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            String userId = accessTokenValidator.verifyUserToken(token);
            log.info("UserId from auth token {}", userId);
            if (StringUtils.isBlank(userId) || userId.equalsIgnoreCase(Constants.UNAUTHORIZED)) {
                response.getParams().setMsg(Constants.USER_ID_DOESNT_EXIST);
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.USER_ID, userId);
            List<Map<String, Object>> userEnrollmentList = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    Constants.KEYSPACE_SUNBIRD_COURSES,
                    Constants.TABLE_USER_EXTERNAL_ENROLMENTS,
                    propertyMap,
                    null,
                    null
            );
            if (request.get(Constants.LIMIT) != null) {
                int limit = (int)request.get(Constants.LIMIT);
                if (cbServerProperties.getMaximumAllowedLimit() < limit) {
                    limit = cbServerProperties.getMaximumAllowedLimit();
                }
                userEnrollmentList = userEnrollmentList.stream()
                        .filter(enrollment -> {
                            Instant updatedOn = (Instant) enrollment.get(Constants.UPDATED_ON); // Cast to Date
                            if (updatedOn == null) {
                                log.error("Invalid or missing updatedOn value: " + updatedOn);
                                return false;
                            }
                            return true;
                        })
                        .sorted(Comparator.comparing(enrollment -> ((Instant) (((Map<String, Object>) enrollment).get(Constants.UPDATED_ON)))).reversed())
                        .toList();
                if (CollectionUtils.isNotEmpty(userEnrollmentList) && userEnrollmentList.size() > limit) {
                    userEnrollmentList = userEnrollmentList.subList(0, limit);
                }
            }

            Integer statusValue = statusMap.get(status);
            if (statusValue != -1) {
                userEnrollmentList = userEnrollmentList.stream().filter(enrolment -> (int) enrolment.get(Constants.STATUS) == statusValue).toList();
            }

            List<Map<String, Object>> courses = new ArrayList<>();
            if (!userEnrollmentList.isEmpty()) {
                for (Map<String, Object> enrollment : userEnrollmentList) {
                    String courseId = (String) enrollment.get(Constants.COURSE_ID);
                    Map<String, Object> data = fetchDataByContentId(courseId);
                    enrollment.put(Constants.CONTENT, data.get(Constants.CONTENT));
                    courses.add(enrollment);
                    response.put(Constants.COURSES, courses);
                }
                response.setResponseCode(HttpStatus.OK);
                response.setResult(response.getResult());
            } else {
                response.getParams().setMsg("User is not enrolled into any courses");
                response.getParams().setStatus(Constants.SUCCESS);
                response.setResponseCode(HttpStatus.OK);
                return response;
            }
            return response;
        } catch (Exception e) {
            String errMsg = "Error while performing operation." + e.getMessage();
            log.error(errMsg, e);
            response.getParams().setMsg(errMsg);
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    @Override
    public SBApiResponse readByUserIdAndPartnerId(Map<String, Object> searchRequest, String token) {
        log.info("EnrollmentService::readByUserIdAndPartnerId:inside the method");
        SBApiResponse response = transformUtility.createDefaultResponse(Constants.CIOS_ENROLLMENT_READ_COURSELIST_BY_PARTNER);
        try {
            if (MapUtils.isEmpty(searchRequest)) {
                response.getParams().setMsg("Request is not proper, please include request body.");
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            String partnerId = (String) searchRequest.get(Constants.PARTNER_ID);
            if (StringUtils.isEmpty(partnerId)) {
                response.getParams().setMsg("Request is not proper, please provide partnerId in request body.");
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            // status must be one of the CiosEnrolmentStatus labels (same values accepted by the
            // existing /v1/courselist/byuserid API): "In-Progress", "Completed", "All".
            String status = (String) searchRequest.get(Constants.STATUS);
            if (StringUtils.isEmpty(status)) {
                response.getParams().setMsg("Request is not proper, please provide status in request body.");
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            if (statusMap.get(status) == null) {
                response.getParams().setMsg("Request is not proper, please provide proper value of status (In-Progress, Completed, All).");
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            String userId = accessTokenValidator.verifyUserToken(token);
            log.info("UserId from auth token {}", userId);
            if (StringUtils.isBlank(userId) || userId.equalsIgnoreCase(Constants.UNAUTHORIZED)) {
                response.getParams().setMsg(Constants.USER_ID_DOESNT_EXIST);
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            // Check Redis first for the minimal per-user enrolment map (courseId -> {partnerId, status}).
            // Only if it isn't present there do we fall back to Cassandra - once - and then
            // populate Redis so every subsequent call for this user is served from Redis alone.
            // No TTL is set: once populated, the entry is not re-queried again.
            Integer statusValue = statusMap.get(status);
            String enrolmentRedisKey = Constants.USER_ENROLMENTS_PREFIX + userId;
            int redisDbIndex = cbServerProperties.getRedisIndex();
            Map<Object, Object> enrolmentHash = cacheService.getAllHashFields(enrolmentRedisKey, redisDbIndex);

            if (MapUtils.isEmpty(enrolmentHash)) {
                log.info("No enrolment map found in Redis for user {}, building it from Cassandra", userId);
                Map<String, Object> propertyMap = new HashMap<>();
                propertyMap.put(Constants.USER_ID, userId);
                List<Map<String, Object>> userEnrollmentList = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                        Constants.KEYSPACE_SUNBIRD_COURSES,
                        Constants.TABLE_USER_EXTERNAL_ENROLMENTS,
                        propertyMap,
                        null,
                        null
                );

                Map<String, String> hashToCache = new HashMap<>();
                Map<Object, Object> rebuiltHash = new HashMap<>();
                for (Map<String, Object> enrolment : userEnrollmentList) {
                    String cId = (String) enrolment.get(Constants.COURSE_ID);
                    Map<String, Object> minimalInfo = new HashMap<>();
                    minimalInfo.put(Constants.PARTNER_ID_REQ, enrolment.get(Constants.PARTNER_ID_REQ));
                    minimalInfo.put(Constants.STATUS, enrolment.get(Constants.STATUS));
                    String json = objectMapper.writeValueAsString(minimalInfo);
                    hashToCache.put(cId, json);
                    rebuiltHash.put(cId, json);
                }
                if (!hashToCache.isEmpty()) {
                    cacheService.putAllHashFields(enrolmentRedisKey, redisDbIndex, hashToCache);
                }
                enrolmentHash = rebuiltHash;
            } else {
                log.info("Enrolment map for user {} fetched from Redis", userId);
            }

            List<Map<String, Object>> courses = new ArrayList<>();
            for (Map.Entry<Object, Object> entry : enrolmentHash.entrySet()) {
                String courseId = String.valueOf(entry.getKey());
                Map<String, Object> enrolmentInfo;
                try {
                    enrolmentInfo = objectMapper.readValue((String) entry.getValue(), new TypeReference<Map<String, Object>>() {});
                } catch (JsonProcessingException e) {
                    log.error("Error while parsing cached enrolment info for user {} course {}: {}", userId, courseId, e.getMessage());
                    continue;
                }

                String cachedPartnerId = (String) enrolmentInfo.get(Constants.PARTNER_ID_REQ);
                if (!partnerId.equalsIgnoreCase(cachedPartnerId)) {
                    continue;
                }
                if (statusValue != -1) {
                    int cachedStatus = ((Number) enrolmentInfo.get(Constants.STATUS)).intValue();
                    if (cachedStatus != statusValue) {
                        continue;
                    }
                }

                Map<String, Object> data = fetchDataByContentId(courseId);
                Map<String, Object> course = new HashMap<>((Map<String, Object>) data.get(Constants.CONTENT));
                course.put(Constants.COURSE_ID_RQST, courseId);
                course.put(Constants.PARTNER_ID, cachedPartnerId);
                course.put(Constants.STATUS, enrolmentInfo.get(Constants.STATUS));
                courses.add(course);
            }

            if (!courses.isEmpty()) {
                response.put(Constants.COURSES, courses);
                response.setResponseCode(HttpStatus.OK);
            } else {
                response.getParams().setMsg("User has no courses with this provider matching the given status");
                response.getParams().setStatus(Constants.SUCCESS);
                response.setResponseCode(HttpStatus.OK);
            }
            return response;
        } catch (Exception e) {
            String errMsg = "Error while performing operation. " + e.getMessage();
            log.error("Error while performing operation. {}", e.getMessage(), e);
            response.getParams().setMsg(errMsg);
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    @Override
    public SBApiResponse readByUserIdAndCourseId(String courseid, String token) {
        log.info("EnrollmentService::readByUserIdAndCourseId:inside the method");
        SBApiResponse response = transformUtility.createDefaultResponse(Constants.CIOS_ENROLLMENT_READ_COURSEID);
        try {
            String userId = accessTokenValidator.verifyUserToken(token);
            if (StringUtils.isBlank(userId) || userId.equalsIgnoreCase(Constants.UNAUTHORIZED)) {
                response.getParams().setMsg(Constants.USER_ID_DOESNT_EXIST);
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            //List<String> fields = Arrays.asList("userid", "courseid", "completedon", "updatedon", "completionpercentage", "enrolled_date", "issued_certificates", "progress", "status"); // Assuming user_id is the column name in your table
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put("userid", userId);
            propertyMap.put("courseid", courseid);
            List<Map<String, Object>> userEnrollmentList = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    Constants.KEYSPACE_SUNBIRD_COURSES,
                    Constants.TABLE_USER_EXTERNAL_ENROLMENTS,
                    propertyMap,
                    null,
                    1
            );
            if (!userEnrollmentList.isEmpty()) {
                for (Map<String, Object> enrollment : userEnrollmentList) {
                    if (!enrollment.isEmpty()) {
                        response.setResponseCode(HttpStatus.OK);
                        response.setResult(enrollment);
                    } else {
                        response.getParams().setMsg("courseId is not matching");
                        response.getParams().setStatus(Constants.FAILED);
                        response.setResponseCode(HttpStatus.BAD_REQUEST);
                        return response;
                    }
                }
            } else {
                response.getParams().setMsg("User not enrolled into the course");
                response.getParams().setStatus(Constants.SUCCESS);
                response.setResponseCode(HttpStatus.OK);
                return response;
            }
            return response;
        } catch (Exception e) {
            log.error("error while processing", e);
            throw new CustomException(Constants.ERROR, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public SBApiResponse userProgressUpdate(JsonNode jsonNode, String partnercode) {
        try {
            SBApiResponse response = transformUtility.createDefaultResponse(Constants.CIOS_ENROLLMENT_PREGRESS_UPDATE);
            log.info("Payload received for userProgressUpdate: {} and partnerCode: {}", jsonNode.toString(),partnercode);
            String inputDate=jsonNode.get("completion_date").asText();
            String formatedDate=updateDateFormatFromInputDate(inputDate);
            ((ObjectNode)jsonNode).put("completion_date",formatedDate);
            ((ObjectNode)jsonNode).put("partnerCode",partnercode);
            JsonNode additionalProps = jsonNode.path("additionalProperties");
            if (!additionalProps.isMissingNode() && !additionalProps.isNull()) {
                payloadValidation.validatePayload(Constants.PAYLOAD_VALIDATION_FILE_CONTENT_PROVIDER, additionalProps);
            }
            producer.push(cbServerProperties.getUserProgressSendFromPartner(), jsonNode);
            Map<String, Object> result = new HashMap<>();
            result.put("response", "Progress report sent successfully");
            response.setResult(result);
            return response;
        }catch (Exception e) {
           throw new CustomException(Constants.ERROR,e.getMessage(),HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private String updateDateFormatFromInputDate(String inputDate) {
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDateTime = LocalDateTime.parse(inputDate, inputFormatter);

        ZonedDateTime utcZonedDateTime = localDateTime
                .atZone(ZoneId.of("Asia/Kolkata"))
                .withZoneSameInstant(ZoneId.of("UTC"));

        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                .withZone(ZoneId.of("UTC"));
        return outputFormatter.format(utcZonedDateTime);
    }
    public Map<String, Object> fetchDataByContentId(String contentId) {
        log.debug("getting content by id: " + contentId);
        if (StringUtils.isEmpty(contentId)) {
            log.error("CiosContentServiceImpl::read:Id not found");
            throw new CustomException(Constants.ERROR, "contentId is mandatory", HttpStatus.BAD_REQUEST);
        }
        String cachedJson = cacheService.getCache(contentId,cbServerProperties.getDefaultIndex());
        Map<String, Object> response = new HashMap<>();
        if (StringUtils.isNotEmpty(cachedJson)) {
            log.info("CiosContentServiceImpl::read:Record coming from redis cache");
            try {
               return objectMapper.readValue(cachedJson, new TypeReference<Map<String, Object>>() {});
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        } else {
            Optional<CiosContentEntity> optionalJsonNodeEntity = contentRepository.findByContentIdAndIsActive(contentId, true);
            if (optionalJsonNodeEntity.isPresent()) {
                CiosContentEntity ciosContentEntity = optionalJsonNodeEntity.get();
                cacheService.putCache(contentId, cbServerProperties.getDefaultIndex(), ciosContentEntity.getCiosData());
                log.info("CiosContentServiceImpl::read:Record coming from postgres db");
                return objectMapper.convertValue(ciosContentEntity.getCiosData(), new TypeReference<Map<String, Object>>() {});
            }
        }
    return response;
    }

    private Map<String, String> getUserAttributes(Map<String, Object> userProfileMap) {
        Map<String, String> userAttributes = new HashMap<>();
        userAttributes.put(Constants.USER, (String) userProfileMap.get(Constants.ID));
        userAttributes.put(Constants.ROOT_ORG_ID.toLowerCase(), (String) userProfileMap.get(Constants.ROOT_ORG_ID_REQ));

        String profileDetailsStr = (String) userProfileMap.get(Constants.PROFILE_DETAILS);
        if (StringUtils.isBlank(profileDetailsStr)) {
            return userAttributes;
        }

        try {
            Map<String, Object> profileDetails = objectMapper.readValue(profileDetailsStr, new TypeReference<Map<String, Object>>() {});
            if (MapUtils.isEmpty(profileDetails)) {
                return userAttributes;
            }

            userAttributes.put(Constants.PROFILE_STATUS.toLowerCase(),
                    (String) profileDetails.get(Constants.PROFILE_STATUS));

            populateProfessionalDetails(userAttributes, profileDetails);
            populateCadreDetails(userAttributes, profileDetails);

        } catch (Exception e) {
            throw new CustomException(Constants.USER_NOT_FOUND, e.getMessage(), HttpStatus.NOT_FOUND);
        }
        return userAttributes;
    }

    private void populateProfessionalDetails(Map<String, String> userAttributes, Map<String, Object> profileDetails) {
        Object professionalObj = profileDetails.get(Constants.PROFESSIONAL_DETAILS);
        if (!(professionalObj instanceof List)) {
            return;
        }
        List<?> profList = (List<?>) professionalObj;
        if (profList.isEmpty()) {
            return;
        }
        Object first = profList.get(0);
        if (!(first instanceof Map)) {
            return;
        }
        Map<String, Object> professionalDetails = (Map<String, Object>) first;
        if (MapUtils.isEmpty(professionalDetails)) {
            return;
        }
        if (professionalDetails.get(Constants.DESIGNATION) != null) {
            userAttributes.put(Constants.DESIGNATION, (String) professionalDetails.get(Constants.DESIGNATION));
        }
        if (professionalDetails.get(Constants.GROUP) != null) {
            userAttributes.put(Constants.GROUP, (String) professionalDetails.get(Constants.GROUP));
        }
    }

    @SuppressWarnings("unchecked")
    private void populateCadreDetails(Map<String, String> userAttributes, Map<String, Object> profileDetails) {
        Object cadreObj = profileDetails.get(Constants.CADRE_DETAILS);
        if (!(cadreObj instanceof Map)) {
            return;
        }
        Map<String, Object> cadreDetails = (Map<String, Object>) cadreObj;
        if (MapUtils.isEmpty(cadreDetails)) {
            return;
        }
        if (StringUtils.isNotBlank(MapUtils.getString(cadreDetails, Constants.CADRE_NAME))) {
            userAttributes.put(Constants.CADRE, (String) cadreDetails.get(Constants.CADRE_NAME));
        }
        if (StringUtils.isNotBlank(MapUtils.getString(cadreDetails,Constants.CIVIL_SERVICE_NAME))) {
            userAttributes.put(Constants.SERVICE, (String) cadreDetails.get(Constants.CIVIL_SERVICE_NAME));
        }
        if (StringUtils.isNotBlank(MapUtils.getString(cadreDetails, Constants.CADRE_BATCH))) {
            userAttributes.put(Constants.BATCH, String.valueOf(cadreDetails.get(Constants.CADRE_BATCH)));
        }
    }


    private boolean accessSettingsEnabled(Map<String, String> userAttributes, List<UserGroup> rules) {
        boolean isCourseAllowed = false;
        for (UserGroup rule : rules) {
            boolean isRuleSuccess = true;
            log.info("Validating rule: {}", rule.getUserGroupId());
            for (UserGroupCriteria criteria : rule.getUserGroupCriteriaList()) {
                log.info("Validating criteriaKey: {}, with Value: {}", criteria.getCriteriaKey(), criteria.getCriteriaValue());
                if (!criteria.evaluate(userAttributes)) {
                    isRuleSuccess = false;
                    break;
                }
            }
            if (isRuleSuccess) {
                isCourseAllowed = true;
                log.info("User {} successfully passed the rule using id: {}", userAttributes.get(Constants.USER), rule.getUserGroupId());
                break;
            }
            log.info("isRuleSuccess: {} is course allowed: {}", isRuleSuccess, isCourseAllowed);
        }
        return isCourseAllowed;

    }

    private boolean handleAccessControlledEnrollment(String courseId, Map<String, String> userAttributes) throws JsonProcessingException {
        AccessControl accessControl = transformUtility.readAccessSettings(courseId);
        if (accessControl == null) {
            log.error("Access control settings enabled but not found for courseId: {}", courseId);
            throw new CustomException(Constants.ERROR, Constants.ACCESS_RULES_ENABLED_BUT_NOT_FOUND_COURSE, HttpStatus.BAD_REQUEST);
        }
        if (accessSettingsEnabled(userAttributes, accessControl.getUserGroups())) {
            return true;
        }
        return false;
    }

    private boolean enrollUserInCourse(String userId, String courseId, String partnerId, JsonNode providerResponse, JsonNode contentResponse) throws JsonProcessingException {
        ZoneId zoneId = ZoneId.of(Constants.UTC);
        Instant instant = LocalDateTime.now().atZone(zoneId).toInstant();

        Map<String, Object> userCourseEnrollMap = new HashMap<>();
        userCourseEnrollMap.put(Constants.USER_ID, userId);
        userCourseEnrollMap.put(Constants.COURSE_ID, courseId);
        userCourseEnrollMap.put(Constants.PARTNER_ID_REQ, partnerId);
        userCourseEnrollMap.put(Constants.PROGRESS, 0);
        userCourseEnrollMap.put(Constants.STATUS, 0);
        userCourseEnrollMap.put(Constants.COMPLETED_ON, null);
        userCourseEnrollMap.put(Constants.COMPLETION_PERCENTAGE, 0);
        userCourseEnrollMap.put(Constants.ISSUED_CERTIFICATES, new ArrayList<>());
        userCourseEnrollMap.put(Constants.ENROLLED_DATE, instant);
        userCourseEnrollMap.put(Constants.UPDATED_ON, instant);
        userCourseEnrollMap.put(Constants.ADDITIONAL_PROPERTIES, objectMapper.writeValueAsString(new HashMap<>()));

        // Authoritative insert, gated by IF NOT EXISTS - this is what makes the enrolment
        // exactly-once. A retried/duplicate request comes back not-applied and nothing below
        // (lookup table, counters) is touched for it.
        ApiResponse insertResult = cassandraOperation.insertRecord(
                Constants.KEYSPACE_SUNBIRD_COURSES,
                Constants.TABLE_USER_EXTERNAL_ENROLMENTS,
                userCourseEnrollMap
        );

        if (!Constants.SUCCESS.equals(insertResult.get(Constants.RESPONSE))) {
            log.error("Failed to insert enrolment for user {} to course {}", userId, courseId);
            return false;
        }

        String licenseType = getLicenseType(providerResponse);
        String courseType = getCourseType(contentResponse);
        // Whether this enrolment counts toward the partner's overall licence consumption
        // (TOTAL_ENROLMENTS) depends on licenseType - course licences always count, user
        // licences only count for a genuinely new user - but that decision is deliberately not
        // made here anymore. A live "is this user new" read in this request thread can race
        // with another concurrent enrolment request for the same user and double-count a
        // licence unit, since two requests could both read "new" before either has incremented
        // anything. That decision now happens exactly once, safely, in the Kafka consumer that
        // processes this event (see KafkaConsumer.enrolmentCounterUpdateConsumer) - every event
        // for a given (partnerId, userId) is published keyed on that pair (see
        // publishCounterUpdateEvent), so Kafka guarantees the consumer processes them one at a
        // time, in order, closing the race without any new table or cache.
        publishCounterUpdateEvent(partnerId, userId, courseId, courseType, licenseType);

        cacheService.incrementIfExists(Constants.PARTNER + partnerId + Constants.COUNT, 1, cbServerProperties.getRedisIndex());
        cacheService.incrementIfExists(Constants.PARTNER + partnerId + Constants.USER_KEY + userId + Constants.COUNT, 1, cbServerProperties.getRedisIndex());
        cacheService.incrementIfExists(Constants.PARTNER + partnerId + Constants.USER_KEY + userId + Constants.ACTIVE_COUNT, 1, cbServerProperties.getRedisIndex());

        // Invalidate (rather than patch) the readByUserIdAndPartnerId cache-aside hash for
        // this user. That cache's read path treats "hash exists" as "hash is fully built" -
        // an HSET adding just this one course would risk looking complete while still
        // missing whatever hadn't been cached before. Deleting it instead means the next
        // read simply rebuilds it from Cassandra, this new enrolment included. If it was
        // never cached at all, the delete is a harmless no-op.
        cacheService.deleteCache(Constants.USER_ENROLMENTS_PREFIX + userId, cbServerProperties.getRedisIndex());

        log.info("User {} successfully enrolled to course {}", userId, courseId);
        return true;
    }

    private void publishCounterUpdateEvent(String partnerId, String userId, String courseId, String courseType, String licenseType) {
        Map<String, Object> counterEvent = new HashMap<>();
        counterEvent.put(Constants.PARTNER_ID_REQ, partnerId);
        counterEvent.put(Constants.USER_ID, userId);
        counterEvent.put(Constants.COURSE_ID, courseId);
        counterEvent.put(Constants.COURSE_TYPE_COL, courseType);
        counterEvent.put(Constants.LICENSE_TYPE, licenseType);
        // Keyed by partnerId+userId - Kafka's own partitioner then guarantees every counter
        // update event for this pair always lands on the same partition and is consumed
        // strictly in order, one at a time, regardless of how many concurrent HTTP requests
        // produced them. That ordering is what lets the consumer safely decide "is this a new
        // user" for itself instead of trusting a value read earlier in a racy request thread.
        producer.push(cbServerProperties.getEnrolmentCounterUpdateTopic(), counterEvent, partnerId + "_" + userId);
    }

    private KarmaValidationResult validatePartnerEnrollmentLimits(
            String userId,
            String partnerId,
            String courseId,
            SBApiResponse response,
            JsonNode providerResponse,
            JsonNode contentResponse,
            String token,
            Map<String, String> userAttributes) {

        // Free courses skip course-level and partner-level validation entirely
        if (isCourseFree(contentResponse)) {
            return new KarmaValidationResult(true, 0);
        }

        if (isOverallLimitExceeded(userId, partnerId, providerResponse, contentResponse, response)) {
            return new KarmaValidationResult(false, 0);
        }

        if (isUserWiseLimitExceeded(userId, partnerId, providerResponse, response)) {
            return new KarmaValidationResult(false, 0);
        }

        if (isConcurrentLimitExceeded(userId, partnerId, providerResponse, response)) {
            return new KarmaValidationResult(false, 0);
        }

        if (isCourseLevelCapExceeded(courseId, partnerId, providerResponse, contentResponse, response)) {
            return new KarmaValidationResult(false, 0);
        }

        KarmaValidationResult karmaResult = validateAndResolveKarma(userId, contentResponse, providerResponse, token, userAttributes, response);
        if (!karmaResult.isAllowed()) {
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return karmaResult;
        }

        return karmaResult;
    }

    private boolean isOverallLimitExceeded(
            String userId,
            String partnerId,
            JsonNode providerResponse,
            JsonNode contentResponse,
            SBApiResponse response) {

        int overallLimit = providerResponse.path(Constants.OVER_ALL_PROVIDER_LIMIT).asInt(0);
        if (overallLimit <= 0) return false;

        String licenseType = getLicenseType(providerResponse);

        if (Constants.LICENSE_TYPE_COURSE.equalsIgnoreCase(licenseType)) {
            // No validation at all for free courses. Every enrolment (not just new users) counts
            // against the cap here, since a course licence is consumed per-enrolment.
            if (isCourseFree(contentResponse)) {
                return false;
            }
            long totalEnrolments = getCounterValue(partnerId, Constants.SCOPE_TYPE_TOTAL_ENROLMENTS, partnerId, Constants.COURSE_TYPE_PAID);
            if (totalEnrolments >= overallLimit) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setMsg(cbServerProperties.getPartnerOverallLimitMsg());
                return true;
            }
            return false;
        }

        // licenseType == user, or not yet configured on this partner - both are now treated as
        // the user-licence model; the old lookup-table fallback is fully retired.
        // Free courses are exempt from the licence cap here too, same as the course-licence
        // model above - a partner offering a free course under the User-licence model should
        // never have that enrolment blocked (or counted) by the overall licence limit, since it
        // never consumes a licence unit.
        if (isCourseFree(contentResponse)) {
            return false;
        }
        // An already-licensed user (has at least one prior enrolment with this partner) is exempt
        // from the overall-limit check entirely - a full license should never block a learner
        // who already holds a unit. Only a genuinely new user is checked here.
        if (!isNewUserForPartner(partnerId, userId)) {
            return false;
        }
        long totalEnrolments = getCounterValue(partnerId, Constants.SCOPE_TYPE_TOTAL_ENROLMENTS, partnerId, Constants.COURSE_TYPE_PAID);
        if (totalEnrolments >= overallLimit) {
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            response.getParams().setMsg(cbServerProperties.getPartnerOverallLimitMsg());
            return true;
        }
        return false;
    }

    private boolean isUserWiseLimitExceeded(
            String userId,
            String partnerId,
            JsonNode providerResponse,
            SBApiResponse response) {

        if (!providerResponse.path(Constants.USER_WISE_LIMIT_ENABLED).asBoolean(false))
            return false;

        int userWiseLimit = providerResponse.path(Constants.USER_WISE_LIMIT).asInt(0);
        if (userWiseLimit <= 0) return false;

        // Identical regardless of licenseType (course, user, or not yet configured) - courses
        // a user holds with this partner. The old lookup-table fallback is fully retired; the
        // counter table is now the only source for this check.
        long userCourseCount = getCounterValue(partnerId, Constants.SCOPE_TYPE_USER_ENROLMENTS, userId, Constants.COURSE_TYPE_PAID);
        if (userCourseCount >= userWiseLimit) {
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            response.getParams().setMsg(cbServerProperties.getPartnerUserwiseLimitMsg());
            return true;
        }
        return false;
    }

    /**
     * Course-level cap (courseEnrolLimit): only evaluated when licenseType == course, the course
     * is paid, and courseEnrolLimit > 0. Not evaluated at all for licenseType == user, free
     * courses, or a partner without courseEnrolLimit set.
     */
    private boolean isCourseLevelCapExceeded(
            String courseId,
            String partnerId,
            JsonNode providerResponse,
            JsonNode contentResponse,
            SBApiResponse response) {

        if (!Constants.LICENSE_TYPE_COURSE.equalsIgnoreCase(getLicenseType(providerResponse))) {
            return false;
        }
        if (isCourseFree(contentResponse)) {
            return false;
        }
        int courseEnrolLimit = contentResponse.path(Constants.COURSE_ENROL_LIMIT).asInt(0);
        if (courseEnrolLimit <= 0) {
            return false;
        }

        long courseEnrolmentCount = getCounterValue(partnerId, Constants.SCOPE_TYPE_COURSE_ENROLMENTS, courseId, Constants.COURSE_TYPE_PAID);
        if (courseEnrolmentCount >= courseEnrolLimit) {
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            response.getParams().setMsg(cbServerProperties.getPartnerCourseLimitMsg());
            return true;
        }
        return false;
    }

    private String getLicenseType(JsonNode providerResponse) {
        return providerResponse.path(Constants.LICENSE_TYPE).asText("");
    }

    /**
     * courseType defaults to paid when absent, matching the publish-time default.
     */
    private String getCourseType(JsonNode contentResponse) {
        String courseType = contentResponse.path(Constants.COURSE_TYPE).asText(Constants.COURSE_TYPE_PAID);
        return Constants.COURSE_TYPE_FREE.equalsIgnoreCase(courseType) ? Constants.COURSE_TYPE_FREE : Constants.COURSE_TYPE_PAID;
    }

    private boolean isCourseFree(JsonNode contentResponse) {
        return Constants.COURSE_TYPE_FREE.equals(getCourseType(contentResponse));
    }

    /**
     * True when this user has never enrolled in any course with this partner before, i.e. the
     * USER_ENROLMENTS counter for (partnerId, userId, paid) is absent or 0. Must always be a
     * fresh read at the point of use - never reused from an earlier validation call - since it
     * can go stale between a TOC-page check and the actual enrol click.
     */
    private boolean isNewUserForPartner(String partnerId, String userId) {
        return getCounterValue(partnerId, Constants.SCOPE_TYPE_USER_ENROLMENTS, userId, Constants.COURSE_TYPE_PAID) == 0;
    }

    /**
     * Point read of a single counter row's "value" column, defaulting to 0 when the row has
     * never been written - a counter that's never been incremented has no row at all, not a
     * row with 0, so an absent result must be treated as 0 rather than skipped or errored.
     */
    private long getCounterValue(String partnerId, String scopeType, String scopeId, String courseType) {
        Map<String, Object> keyMap = new HashMap<>();
        keyMap.put(Constants.PARTNER_ID_REQ, partnerId);
        keyMap.put(Constants.SCOPE_TYPE, scopeType);
        keyMap.put(Constants.SCOPE_ID, scopeId);
        keyMap.put(Constants.COURSE_TYPE_COL, courseType);
        List<Map<String, Object>> rows = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD_COURSES,
                Constants.TABLE_USER_EXTERNAL_ENROLMENTS_COUNTER,
                keyMap,
                List.of(Constants.COUNTER_VALUE),
                1
        );
        if (CollectionUtils.isEmpty(rows)) {
            return 0L;
        }
        Object value = rows.get(0).get(Constants.COUNTER_VALUE);
        return value == null ? 0L : ((Number) value).longValue();
    }

    private boolean isConcurrentLimitExceeded(
            String userId,
            String partnerId,
            JsonNode providerResponse,
            SBApiResponse response) {

        if (!providerResponse.path(Constants.CONCURRENT_LIMIT_ENABLED).asBoolean(false))
            return false;

        int concurrentLimit = providerResponse.path(Constants.CONCURRENT_LIMIT).asInt(0);
        if (concurrentLimit <= 0) return false;

        String activeKey =
                Constants.PARTNER + partnerId + Constants.USER_KEY + userId + Constants.ACTIVE_COUNT;

        int activeCount = getCountFromCacheOrDb(
                activeKey,
                () -> {
                    List<Map<String, Object>> allUserCourses =
                            cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                                    Constants.KEYSPACE_SUNBIRD_COURSES,
                                    Constants.TABLE_USER_EXTERNAL_ENROLMENTS,
                                    Map.of(Constants.USER_ID, userId),
                                    null,
                                    null
                            );

                    return (int) allUserCourses.stream()
                            .filter(rec -> partnerId.equals(rec.get(Constants.PARTNER_ID_REQ)))
                            .filter(rec -> rec.get(Constants.STATUS) != null
                                    && ((int) rec.get(Constants.STATUS)) == 0)
                            .count();
                }
        );

        if (activeCount >= concurrentLimit) {
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            response.getParams().setMsg(cbServerProperties.getPartnerConcurrentLimitMsg());
            return true;
        }
        return false;
    }

    private KarmaValidationResult validateAndResolveKarma(
            String userId,
            JsonNode contentResponse,
            JsonNode providerResponse,
            String token,
            Map<String, String> userAttributes,
            SBApiResponse response) {

        if (!providerResponse.path(Constants.KARMA_POINTS_ENABLED).asBoolean(false)) {
            return new KarmaValidationResult(true, 0);
        }

        if (isCourseFree(contentResponse)) {
            return new KarmaValidationResult(true, 0);
        }

        int requiredKarmaPoints = contentResponse.path(Constants.REQUIRED_KARMA_POINTS).asInt(0);
        if (requiredKarmaPoints <= 0) {
            return new KarmaValidationResult(true, 0);
        }

        Long userKarmaPoints = transformUtility.readUserKarmaPoints(userId, token);
        boolean isExemptGroup = isKarmaPointsExempt(userAttributes, providerResponse.path(Constants.KARMA_POINTS_EXEMPTION));

        if (!isExemptGroup && userKarmaPoints < requiredKarmaPoints) {
            response.getParams().setMsg(String.format(cbServerProperties.getKarmaInsufficientMsg(), requiredKarmaPoints));
            return new KarmaValidationResult(false,
                    0);

        }
        return new KarmaValidationResult(true, requiredKarmaPoints);
    }

    private int getCountFromCacheOrDb(String key, Supplier<Integer> dbSupplier) {
        String cached = cacheService.getCache(key,cbServerProperties.getRedisIndex());
        if (StringUtils.isNotBlank(cached)) {
            return Integer.parseInt(cached);
        }

        int count = dbSupplier.get();
        cacheService.putCache(key, cbServerProperties.getRedisIndex(), count);
        return count;
    }

    @Override
    public SBApiResponse enrolValidation(JsonNode userCourseEnroll, String token) {
        log.info("EnrollmentService::enrolValidation:inside the method");
        SBApiResponse response = transformUtility.createDefaultResponse(Constants.CIOS_ENROLLMENT_CREATE);
        if (!userCourseEnroll.hasNonNull(Constants.COURSE_ID_RQST)) {
            return transformUtility.buildFailedResponse(response, "CourseId is mandatory", HttpStatus.BAD_REQUEST);
        }

        String partnerId = userCourseEnroll.path(Constants.PARTNER_ID).asText("");
        String courseId = userCourseEnroll.get(Constants.COURSE_ID_RQST).asText("");
        JsonNode contentResponse = transformUtility.callCiosContentReadAPi(courseId);
        if(StringUtils.isBlank(partnerId)){
            partnerId = contentResponse.path(Constants.CONTENT_PARTNER).path(Constants.ID).asText("");
        }

        if (StringUtils.isBlank(partnerId) || StringUtils.isBlank(courseId)) {
            return transformUtility.buildFailedResponse(response, "Both partnerId and CourseId cannot be empty", HttpStatus.BAD_REQUEST);
        }
        try {
            String userId = transformUtility.validateAndGetUserId(token,response);
            if(StringUtils.isBlank(userId)){
                return response;
            }

            JsonNode providerResponse = transformUtility.callContentPartnerReadApi(partnerId);

            Map<String, Object> userProfile = transformUtility.readUserDetails(userId);
            Map<String, String> userAttributes = MapUtils.isNotEmpty(userProfile)
                    ? getUserAttributes(userProfile)
                    : new HashMap<>();
            log.info("User attributes fetched for enrollment: {}", userAttributes);

            KarmaValidationResult karmaValidationResult = validatePartnerEnrollmentLimits(
                    userId,
                    partnerId,
                    courseId,
                    response,
                    providerResponse.get(Constants.DATA),
                    contentResponse,
                    token,
                    userAttributes
            );
            if (!karmaValidationResult.isAllowed()) {
                return response;
            }
            if (contentResponse.has(Constants.ACCESS_SETTINGS_ENABLED) && contentResponse.get(Constants.ACCESS_SETTINGS_ENABLED).asBoolean()) {
                if (!handleAccessControlledEnrollment(courseId, userAttributes)) {
                    return transformUtility.buildFailedResponse(response, cbServerProperties.getAccessSettingsErrorMessage(), HttpStatus.BAD_REQUEST);
                }
            }
            return transformUtility.buildSuccessResponse(response, "Enrollment validation successful", HttpStatus.OK);
        } catch (Exception e) {
            String errMsg = Constants.ENROLLMENT_ERROR + e.getMessage();
            log.error(errMsg, e);
            return transformUtility.buildFailedResponse(response, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private boolean validateRequest(JsonNode request, SBApiResponse response) {
        if (!request.hasNonNull(Constants.COURSE_ID_RQST)
                || StringUtils.isBlank(request.get(Constants.COURSE_ID_RQST).asText())) {

            transformUtility.buildFailedResponse(
                    response,
                    "CourseId is mandatory and cannot be empty",
                    HttpStatus.BAD_REQUEST
            );
            return false;
        }

        boolean isPartnerIdMissing =
                !request.hasNonNull(Constants.PARTNER_ID)
                        || StringUtils.isBlank(request.path(Constants.PARTNER_ID).asText());

        if (isPartnerIdMissing) {
            JsonNode contentResponse =
                    transformUtility.callCiosContentReadAPi(
                            request.get(Constants.COURSE_ID_RQST).asText()
                    );

            String partnerIdFromContent =
                    contentResponse
                            .path(Constants.CONTENT_PARTNER)
                            .path(Constants.ID)
                            .asText("");

            if (StringUtils.isBlank(partnerIdFromContent)) {
                transformUtility.buildFailedResponse(
                        response,
                        "PartnerId not found for given CourseId",
                        HttpStatus.BAD_REQUEST
                );
                return false;
            }
            ((ObjectNode) request).put(Constants.PARTNER_ID, partnerIdFromContent);
        }

        return true;
    }

    private boolean isUserEnrolled(SBApiResponse response, String userId, String courseId) {
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.USER_ID, userId);
        propertyMap.put(Constants.COURSE_ID, courseId);
        List<Map<String, Object>> userEnrollmentList = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD_COURSES,
                Constants.TABLE_USER_EXTERNAL_ENROLMENTS,
                propertyMap,
                null,
                1
        );
        if (!userEnrollmentList.isEmpty()) {
            transformUtility.buildFailedResponse(response, "User already enrolled to the course", HttpStatus.BAD_REQUEST);
            return true;
        }
        return false;
    }

    private SBApiResponse processEnrolment(SBApiResponse response, String userId, String courseId, String partnerId, String token) {
        try {
            JsonNode contentResponse = transformUtility.callCiosContentReadAPi(courseId);

            JsonNode providerResponse = transformUtility.callContentPartnerReadApi(partnerId);

            Map<String, Object> userProfile = transformUtility.readUserDetails(userId);
            Map<String, String> userAttributes = MapUtils.isNotEmpty(userProfile)
                    ? getUserAttributes(userProfile)
                    : new HashMap<>();
            log.info("User attributes fetched for enrollment: {}", userAttributes);

            KarmaValidationResult karmaValidationResult = validatePartnerEnrollmentLimits(
                    userId,
                    partnerId,
                    courseId,
                    response,
                    providerResponse.get(Constants.DATA),
                    contentResponse,
                    token,
                    userAttributes
            );
            if (!karmaValidationResult.isAllowed()) {
                return response;
            }

            // Check access control settings enabled and validate
            if (contentResponse.has(Constants.ACCESS_SETTINGS_ENABLED) && contentResponse.get(Constants.ACCESS_SETTINGS_ENABLED).asBoolean()) {
                if (!handleAccessControlledEnrollment(courseId, userAttributes)) {
                    return transformUtility.buildFailedResponse(response, cbServerProperties.getAccessSettingsErrorMessage(), HttpStatus.BAD_REQUEST);
                }
            }
            // Special handling for Coursera partner to invite user
            String providerCode = providerResponse.path(Constants.DATA).path(Constants.PARTNER_CODE).asText("").toLowerCase();
            if (cbServerProperties.getCourseraPartnerCode().equalsIgnoreCase(providerCode)) {
                boolean inviteSuccess = transformUtility.callCourseraInviteApi(
                        contentResponse,
                        userProfile);
                if (!inviteSuccess) {
                    return transformUtility.buildFailedResponse(response, "User invitation failed on Coursera", HttpStatus.BAD_REQUEST);
                }
            }
            // Enroll user in course
            boolean enrolled = enrollUserInCourse(userId, courseId, partnerId, providerResponse.get(Constants.DATA), contentResponse);
            if (!enrolled) {
                return transformUtility.buildFailedResponse(response, "Failed to enroll user in course", HttpStatus.BAD_REQUEST);
            }
            int redeemedPoints = karmaValidationResult.getRedeemedKarmaPoints();
            if (redeemedPoints > 0) {
                //trigger event to deduct karma points from user
                log.info("Karma points deduction event triggered for userId: {}, courseId: {}, points: {}",
                        userId, courseId, redeemedPoints);
            }
            response.setResponseCode(HttpStatus.OK);
            Map<String, Object> result = new HashMap<>();
            String courseName = contentResponse.path(Constants.NAME).asText("");
            String message;
            if (redeemedPoints > 0) {
                message = String.format(cbServerProperties.getEnrolledWithKarmaMsg(), courseName, redeemedPoints);
            } else {
                message = String.format(cbServerProperties.getEnrolledWithoutKarmaMsg(), courseName);
            }
            result.put(Constants.MESSAGE, message);
            response.setResult(result);
        } catch (Exception e) {
            String errMsg = Constants.ENROLLMENT_ERROR + e.getMessage();
            log.error(errMsg, e);
            return transformUtility.buildFailedResponse(response, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    @Override
    public SBApiResponse getUserEnrolmentByExternalId(String userId, String externalId, String partnerCode) {
        log.info("EnrollmentService::getUserEnrolmentByExternalId:inside the method");
        SBApiResponse response = transformUtility.createDefaultResponse(Constants.CIOS_ENROLLMENT_READ_BY_EXTERNAL_ID);
        try {
            if (StringUtils.isBlank(userId) || StringUtils.isBlank(externalId) || StringUtils.isBlank(partnerCode)) {
                return transformUtility.buildFailedResponse(response,
                        "userId, externalId and partnerCode are mandatory", HttpStatus.BAD_REQUEST);
            }
            String contentId = transformUtility.getContentIdByExternalId(externalId, partnerCode);
            if (StringUtils.isBlank(contentId)) {
                return transformUtility.buildFailedResponse(response,
                        "No content found for given courseId and partnerCode", HttpStatus.BAD_REQUEST);
            }

            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.USER_ID, userId);
            propertyMap.put(Constants.COURSE_ID, contentId);

            List<Map<String, Object>> userEnrollmentList = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD_COURSES,
                    Constants.TABLE_USER_EXTERNAL_ENROLMENTS,
                    propertyMap,
                    null
            );

            if (CollectionUtils.isNotEmpty(userEnrollmentList)) {
                response.getParams().setMsg("User already enrolled into the course");
                response.getParams().setStatus(Constants.SUCCESS);
            } else {
                response.getParams().setMsg("User not enrolled into the course");
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.NOT_FOUND);
            }
            return response;
        } catch (Exception e) {
            String errMsg = "Error while fetching user enrolment by externalId. " + e.getMessage();
            log.error(errMsg, e);
            return transformUtility.buildFailedResponse(response, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public SBApiResponse karmapointsDeductionRule(JsonNode userCourseEnroll, String token) {
        SBApiResponse response = transformUtility.createDefaultResponse(Constants.KARMA_POINTS_DEDUCTION_RULE);
        try {
            if (!validateRequest(userCourseEnroll, response)) {
                return response;
            }
            String userId = transformUtility.validateAndGetUserId(token, response);
            if (StringUtils.isBlank(userId)) {
                return response;
            }
            String partnerId = userCourseEnroll.get(Constants.PARTNER_ID).asText();
            String courseId = userCourseEnroll.get(Constants.COURSE_ID_RQST).asText();
            JsonNode contentResponse = transformUtility.callCiosContentReadAPi(courseId);

            JsonNode providerResponse = transformUtility.callContentPartnerReadApi(partnerId);
            Map<String, Object> userProfile = transformUtility.readUserDetails(userId);
            Map<String, String> userAttributes = MapUtils.isNotEmpty(userProfile)
                    ? getUserAttributes(userProfile)
                    : new HashMap<>();
            log.warn("User attributes fetched for enrollment: {}", userAttributes);

            karmaValidationResult = validateAndResolveKarma(
                    userId,
                    contentResponse,
                    providerResponse.path(Constants.DATA),
                    token,
                    userAttributes,
                    response
            );
            int requiredKarmaPoints = karmaValidationResult.getRedeemedKarmaPoints();
            if (!cbServerProperties.isKarmaPointsDeductionEnabled() || isCourseFree(contentResponse)) {
                requiredKarmaPoints = 0;
            }
            Map<String, Object> result = new HashMap<>();
            result.put(Constants.REQUIRED_KARMA_POINTS, requiredKarmaPoints);
            response.setResult(result);
            return response;
        } catch (Exception e) {
            String errMsg = Constants.ENROLLMENT_ERROR + e.getMessage();
            log.error(errMsg, e);
            return transformUtility.buildFailedResponse(response, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Validates whether the current user is exempt from karma points deduction, based on the
     * partner-configured karmaPointsExcemption rule (a single set of criteria, ALL of which
     * must match the user's attributes - user, rootOrgId, designation, group, cadre, service, batch).
     *
     * Example config:
     * "karmaPointsExcemption": {
     *     "GROUP": ["GROUP A", "GROUP B"]
     * }
     */
    private boolean isKarmaPointsExempt(Map<String, String> userAttributes, JsonNode karmaPointsExcemption) {
        if (karmaPointsExcemption == null || karmaPointsExcemption.isMissingNode() || karmaPointsExcemption.isNull()
                || !karmaPointsExcemption.fields().hasNext()) {
            return false;
        }
        boolean isExempt = true;
        Iterator<Map.Entry<String, JsonNode>> fields = karmaPointsExcemption.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            Set<String> criteriaValues = new HashSet<>();
            if (field.getValue().isArray()) {
                field.getValue().forEach(v -> criteriaValues.add(v.asText()));
            } else if (!field.getValue().isNull()) {
                criteriaValues.add(field.getValue().asText());
            }
            UserGroupCriteria criteria = new UserGroupCriteria();
            criteria.setCriteriaKey(field.getKey());
            criteria.setCriteriaValue(criteriaValues);
            log.info("Validating karmaPointsExcemption criteriaKey: {}, with Value: {}", criteria.getCriteriaKey(), criteria.getCriteriaValue());
            if (!criteria.evaluate(userAttributes)) {
                isExempt = false;
                break;
            }
        }
        log.info("User {} karmaPointsExcemption evaluation result: {}", userAttributes.get(Constants.USER), isExempt);
        return isExempt;
    }
}
