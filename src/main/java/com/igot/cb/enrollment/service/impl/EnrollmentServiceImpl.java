package com.igot.cb.enrollment.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.enrollment.entity.CiosContentEntity;
import com.igot.cb.enrollment.entity.ContentPartnerEntity;
import com.igot.cb.enrollment.repository.CiosContentRepository;
import com.igot.cb.enrollment.repository.ContentPartnerRepository;
import com.igot.cb.enrollment.service.EnrollmentService;
import com.igot.cb.util.CbServerProperties;
import com.igot.cb.util.cache.CacheService;
import com.igot.cb.util.dto.*;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PayloadValidation;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import java.sql.Timestamp;
import java.util.*;

import com.igot.cb.util.exceptions.CustomException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class EnrollmentServiceImpl implements EnrollmentService {

    @Autowired
    private PayloadValidation payloadValidation;

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
    private ContentPartnerRepository contentPartnerRepository;


    @Override
    public SBApiResponse enrollUser(JsonNode userCourseEnroll, String token) {
        log.info("EnrollmentService::enrollUser:inside the method");
        SBApiResponse response = createDefaultResponse(Constants.CIOS_ENROLLMENT_CREATE);
        try {
            String userId = accessTokenValidator.verifyUserToken(token);
            log.info("UserId from auth token {}", userId);
            if (StringUtils.isBlank(userId) || userId.equalsIgnoreCase(Constants.UNAUTHORIZED)) {
                response.getParams().setMsg(Constants.USER_ID_DOESNT_EXIST);
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            if (userCourseEnroll.has(Constants.COURSE_ID_RQST) && !userCourseEnroll.get(
                    Constants.COURSE_ID_RQST).isNull()) {

                TimeZone timeZone = TimeZone.getTimeZone("Asia/Kolkata");
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                timestamp.setTime(timestamp.getTime() + timeZone.getOffset(timestamp.getTime()));
                Map<String, Object> userCourseEnrollMap = new HashMap<>();
                userCourseEnrollMap.put("userid", userId);
                userCourseEnrollMap.put("courseid",
                        userCourseEnroll.get("courseId").asText());
                userCourseEnrollMap.put("progress",
                        0);
                userCourseEnrollMap.put("status",
                        0);
                userCourseEnrollMap.put("completedon",
                        null);
                userCourseEnrollMap.put("completionpercentage",
                        0);
                userCourseEnrollMap.put("issued_certificates",
                        new ArrayList<>());
                userCourseEnrollMap.put(Constants.ENROLLED_DATE,
                        timestamp);
                userCourseEnrollMap.put("updatedon",
                        timestamp);
                cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD_COURSES,
                        Constants.TABLE_USER_EXTERNAL_ENROLMENTS, userCourseEnrollMap);
                response.setResponseCode(HttpStatus.OK);
                response.setResult(userCourseEnrollMap);
                return response;
            } else {
                response.getParams().setMsg("CourseId is missing");
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
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
    public SBApiResponse readByUserId(String token) {
        log.info("EnrollmentService::readByUserId:inside the method");
        SBApiResponse response = createDefaultResponse(Constants.CIOS_ENROLLMENT_READ_COURSELIST);
        try {
            String userId = accessTokenValidator.verifyUserToken(token);
            log.info("UserId from auth token {}", userId);
            if (StringUtils.isBlank(userId) || userId.equalsIgnoreCase(Constants.UNAUTHORIZED)) {
                response.getParams().setMsg(Constants.USER_ID_DOESNT_EXIST);
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
                List<String> fields = Arrays.asList("userid", "courseid", "completedon","updatedon","completionpercentage", "enrolled_date", "issued_certificates", "progress", "status"); // Assuming user_id is the column name in your table
                Map<String, Object> propertyMap = new HashMap<>();
                propertyMap.put("userid", userId);
                List<Map<String, Object>> userEnrollmentList = cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD_COURSES,
                        Constants.TABLE_USER_EXTERNAL_ENROLMENTS,
                        propertyMap,
                        fields
                );
            List<Map<String, Object>> courses=new ArrayList<>();
                if (!userEnrollmentList.isEmpty()) {
                    for (Map<String, Object> enrollment : userEnrollmentList) {
                        // Extract the courseId from each map
                        String courseId = (String) enrollment.get("courseid");
                        Map<String, Object> data= (Map<String, Object>) fetchDataByContentId(courseId);
                        enrollment.put("content",data.get("content"));
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
        SBApiResponse response = createDefaultResponse(Constants.CIOS_ENROLLMENT_READ_COURSEID);
        try {
            String userId = accessTokenValidator.verifyUserToken(token);
            if (StringUtils.isBlank(userId) || userId.equalsIgnoreCase(Constants.UNAUTHORIZED)) {
                response.getParams().setMsg(Constants.USER_ID_DOESNT_EXIST);
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

//            String cacheResponse = cacheService.getCache(userId + courseid);
//            if (cacheResponse != null) {
//                log.info("EnrollmentServiceImpl :: readByUserIdAndCourseId :: Data reading from cache");
//                response = objectMapper.readValue(cacheResponse, SBApiResponse.class);
//            } else {
                List<String> fields = Arrays.asList("userid", "courseid", "completedon","updatedon","completionpercentage", "enrolled_date", "issued_certificates", "progress", "status"); // Assuming user_id is the column name in your table
                Map<String, Object> propertyMap = new HashMap<>();
                propertyMap.put("userid", userId);
                propertyMap.put("courseid", courseid);
                List<Map<String, Object>> userEnrollmentList = cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD_COURSES,
                        Constants.TABLE_USER_EXTERNAL_ENROLMENTS,
                        propertyMap,
                        fields
                );
                if (!userEnrollmentList.isEmpty()) {
                    for (Map<String, Object> enrollment : userEnrollmentList) {
                        if (!enrollment.isEmpty()) {
                            response.setResponseCode(HttpStatus.OK);
                            response.setResult(enrollment);
                       //     cacheService.putCache(userId + courseid, response);
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
//            }
            return response;
        } catch (Exception e) {
            log.error("error while processing", e);
            throw new CustomException(Constants.ERROR,e.getMessage(),HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private SBApiResponse createDefaultResponse(String api) {
        SBApiResponse response = new SBApiResponse();
        response.setId(api);
        response.setVer(Constants.API_VERSION_1);
        response.setParams(new SunbirdApiRespParam(UUID.randomUUID().toString()));
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
        response.setTs(DateTime.now().toString());
        return response;
    }

    public Object fetchDataByContentId(String contentId) {
        log.debug("getting content by id: " + contentId);
        if (StringUtils.isEmpty(contentId)) {
            log.error("CiosContentServiceImpl::read:Id not found");
            throw new CustomException(Constants.ERROR, "contentId is mandatory", HttpStatus.BAD_REQUEST);
        }
        String cachedJson = cacheService.getCache(contentId);
        Object response = null;
        if (StringUtils.isNotEmpty(cachedJson)) {
            log.info("CiosContentServiceImpl::read:Record coming from redis cache");
            try {
                response = objectMapper.readValue(cachedJson, new TypeReference<Object>() {
                });
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        } else {
            Optional<CiosContentEntity> optionalJsonNodeEntity = contentRepository.findByContentIdAndIsActive(contentId, true);
            if (optionalJsonNodeEntity.isPresent()) {
                CiosContentEntity ciosContentEntity = optionalJsonNodeEntity.get();
                cacheService.putCache(contentId, ciosContentEntity.getCiosData());
                log.info("CiosContentServiceImpl::read:Record coming from postgres db");
                response = objectMapper.convertValue(ciosContentEntity.getCiosData(), new TypeReference<Object>() {
                });
            } else {
                log.error("Invalid Id: {}", contentId);
                throw new CustomException(Constants.ERROR, "No data found for given Id", HttpStatus.BAD_REQUEST);
            }
        }
        return response;
    }

    @Override
    public ContentPartnerEntity getContentDetailsByPartnerName(String name) {
        log.info("CiosContentService:: ContentPartnerEntity: getContentDetailsByPartnerName {}",name);
        try {
            ContentPartnerEntity entity=null;
            String cachedJson = cacheService.getCache(Constants.CBPORES_REDIS_KEY_PREFIX+name);
            if (StringUtils.isNotEmpty(cachedJson)) {
                log.info("Record coming from redis cache");
                entity=objectMapper.readValue(cachedJson, new TypeReference<ContentPartnerEntity>() {});
                return entity;
            } else {
                Optional<ContentPartnerEntity> entityOptional = contentPartnerRepository.findByContentPartnerName(name);
                if (entityOptional.isPresent()) {
                    log.info("Record coming from postgres db");
                    entity = entityOptional.get();
                    cacheService.putCache(Constants.CBPORES_REDIS_KEY_PREFIX+name,entity);
                    return entity;

                } else {
                    throw new CustomException(Constants.ERROR,"Content Partner Data not present in DB",HttpStatus.BAD_REQUEST);
                }
            }
        } catch (Exception e) {
            log.error("error while processing", e);
            throw new CustomException(Constants.ERROR,e.getMessage(),HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
