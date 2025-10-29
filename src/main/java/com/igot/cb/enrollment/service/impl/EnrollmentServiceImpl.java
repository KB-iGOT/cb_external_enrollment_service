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
import com.igot.cb.enrollment.model.UserGroup;
import com.igot.cb.enrollment.model.UserGroupCriteria;
import com.igot.cb.enrollment.repository.CiosContentRepository;
import com.igot.cb.enrollment.service.EnrollmentService;
import com.igot.cb.producer.Producer;
import com.igot.cb.util.CbServerProperties;
import com.igot.cb.util.PayloadValidation;
import com.igot.cb.util.TransformUtility;
import com.igot.cb.util.cache.CacheService;
import com.igot.cb.util.dto.*;
import com.igot.cb.util.Constants;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;


import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.igot.cb.util.exceptions.CustomException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;


@Service
@Slf4j
public class EnrollmentServiceImpl implements EnrollmentService {

    @Autowired
    private AccessTokenValidator accessTokenValidator;

    @Autowired
    private CassandraOperation cassandraOperation;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    CacheService cacheService;

    @Autowired
    private CbServerProperties cbServerProperties;

    @Autowired
    private CiosContentRepository contentRepository;

    @Autowired
    private TransformUtility transformUtility;

    @Autowired
    private Producer producer;

    @Autowired
    private PayloadValidation payloadValidation;

    private final Map<String, Integer> statusMap = CiosEnrolmentStatus.toMap();

    @Override
    public SBApiResponse enrollUser(JsonNode userCourseEnroll, String token) {
        log.info("EnrollmentService::enrollUser:inside the method");
        SBApiResponse response = transformUtility.createDefaultResponse(Constants.CIOS_ENROLLMENT_CREATE);
        if (!userCourseEnroll.hasNonNull(Constants.PARTNER_ID) || !userCourseEnroll.hasNonNull(Constants.COURSE_ID_RQST)) {
            return buildFailedResponse(response, "Both partnerId and CourseId is mandatory", HttpStatus.BAD_REQUEST);
        }
        String partnerId = userCourseEnroll.get(Constants.PARTNER_ID).asText("");
        String courseId = userCourseEnroll.get(Constants.COURSE_ID_RQST).asText("");
        try {
            String userId = accessTokenValidator.verifyUserToken(token);
            log.info("UserId from auth token {}", userId);
            if (StringUtils.isBlank(userId) || userId.equalsIgnoreCase(Constants.UNAUTHORIZED)) {
                return buildFailedResponse(response, Constants.USER_ID_DOESNT_EXIST, HttpStatus.BAD_REQUEST);
            }
            if (userCourseEnroll.has(Constants.COURSE_ID_RQST) && !userCourseEnroll.get(
                    Constants.COURSE_ID_RQST).isNull() && userCourseEnroll.has("partnerId") && !userCourseEnroll.get(
                    "partnerId").isNull()) {
                Map<String, Object> propertyMap = new HashMap<>();
                propertyMap.put(Constants.USER_ID, userId);
                propertyMap.put(Constants.COURSE_ID, userCourseEnroll.get(Constants.COURSE_ID_RQST).asText());
                List<Map<String, Object>> userEnrollmentList = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                        Constants.KEYSPACE_SUNBIRD_COURSES,
                        Constants.TABLE_USER_EXTERNAL_ENROLMENTS,
                        propertyMap,
                        null,
                        1
                );
                if (!userEnrollmentList.isEmpty()) {
                    return buildFailedResponse(response, "User already enrolled to the course", HttpStatus.BAD_REQUEST);
                }
            }
            JsonNode contentResponse = transformUtility.callCiosContentReadAPi(userCourseEnroll.get(Constants.COURSE_ID_RQST).asText());

            if (contentResponse.has(Constants.ACCESS_SETTINGS_ENABLED) && contentResponse.get(Constants.ACCESS_SETTINGS_ENABLED).asBoolean()) {
                if (!handleAccessControlledEnrollment(userId, courseId, partnerId, response)) {
                    return buildFailedResponse(response, Constants.ACCESS_RULES_ENABLED_BUT_NOT_FOUND_COURSE, HttpStatus.BAD_REQUEST);
                }
            } else {
                enrollUserInCourse(userId, courseId, partnerId);
                response.setResponseCode(HttpStatus.OK);
                response.setResult(Map.of("message", "User enrolled successfully"));
            }

            } catch(Exception e){
                String errMsg = "Error while performing enrollment operation: " + e.getMessage();
                log.error(errMsg, e);
                return buildFailedResponse(response, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
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
            propertyMap.put("userid", userId);
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
                    // Extract the courseId from each map
                    String courseId = (String) enrollment.get("courseid");
                    Map<String, Object> data = fetchDataByContentId(courseId);
                    enrollment.put("content", data.get("content"));
                    courses.add(enrollment);
                    response.put("courses", courses);
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
        String cachedJson = cacheService.getCache(contentId);
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
                cacheService.putCache(contentId, ciosContentEntity.getCiosData());
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
        if (cadreDetails.get(Constants.CADRE_NAME) != null) {
            userAttributes.put(Constants.CADRE, (String) cadreDetails.get(Constants.CADRE_NAME));
        }
        if (cadreDetails.get(Constants.CIVIL_SERVICE_NAME) != null) {
            userAttributes.put(Constants.SERVICE, (String) cadreDetails.get(Constants.CIVIL_SERVICE_NAME));
        }
        if (cadreDetails.containsKey(Constants.CADRE_BATCH) && cadreDetails.get(Constants.CADRE_BATCH) != null) {
            userAttributes.put(Constants.BATCH, String.valueOf(cadreDetails.get(Constants.CADRE_BATCH)));
        }
    }

    private SBApiResponse buildFailedResponse(SBApiResponse response, String message, HttpStatus status) {
        response.getParams().setMsg(message);
        response.getParams().setStatus(Constants.FAILED);
        response.setResponseCode(status);
        return response;
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

    private boolean handleAccessControlledEnrollment(String userId, String courseId, String partnerId, SBApiResponse response) throws JsonProcessingException {
        Map<String, Object> userProfile = transformUtility.readUserDetails(userId);
        Map<String, String> userAttributes = MapUtils.isNotEmpty(userProfile)
                ? getUserAttributes(userProfile)
                : new HashMap<>();

        log.info("User attributes fetched for enrollment: {}", userAttributes);

        AccessControl accessControl = transformUtility.readAccessSettings(courseId);
        if (accessControl == null) {
            throw new CustomException(Constants.ERROR, Constants.ACCESS_RULES_ENABLED_BUT_NOT_FOUND_COURSE, HttpStatus.BAD_REQUEST);
        }

        if (accessSettingsEnabled(userAttributes, accessControl.getUserGroups())) {
            enrollUserInCourse(userId, courseId, partnerId);
            response.setResponseCode(HttpStatus.OK);
            response.setResult(Map.of("message", "User enrolled successfully"));
            return true;
        }
        return false;
    }

    private void enrollUserInCourse(String userId, String courseId, String partnerId) throws JsonProcessingException {
        ZoneId zoneId = ZoneId.of("UTC");
        Instant instant = LocalDateTime.now().atZone(zoneId).toInstant();

        Map<String, Object> userCourseEnrollMap = new HashMap<>();
        userCourseEnrollMap.put(Constants.USER_ID, userId);
        userCourseEnrollMap.put(Constants.COURSE_ID, courseId);
        userCourseEnrollMap.put(Constants.PARTNER_ID_REQ, partnerId);
        userCourseEnrollMap.put("progress", 0);
        userCourseEnrollMap.put(Constants.STATUS, 0);
        userCourseEnrollMap.put(Constants.COMPLETED_ON, 0);
        userCourseEnrollMap.put(Constants.COMPLETION_PERCENTAGE, 0);
        userCourseEnrollMap.put(Constants.ISSUED_CERTIFICATES, new ArrayList<>());
        userCourseEnrollMap.put(Constants.ENROLLED_DATE, instant);
        userCourseEnrollMap.put(Constants.UPDATED_ON, instant);
        userCourseEnrollMap.put(Constants.ADDITIONAL_PROPERTIES, objectMapper.writeValueAsString(new HashMap<>()));

        cassandraOperation.insertRecord(
                Constants.KEYSPACE_SUNBIRD_COURSES,
                Constants.TABLE_USER_EXTERNAL_ENROLMENTS,
                userCourseEnrollMap
        );

        log.info("User {} successfully enrolled to course {}", userId, courseId);
    }
}
