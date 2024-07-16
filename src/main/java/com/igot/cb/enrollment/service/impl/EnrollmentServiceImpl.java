package com.igot.cb.enrollment.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.enrollment.service.EnrollmentService;
import com.igot.cb.util.cache.CacheService;
import com.igot.cb.util.dto.CustomResponse;
import com.igot.cb.util.Constants;
import com.igot.cb.util.PayloadValidation;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import java.sql.Timestamp;
import java.util.*;

import com.igot.cb.util.dto.CustomResponseList;
import com.igot.cb.util.exceptions.CustomException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Override
    public CustomResponse enrollUser(JsonNode userCourseEnroll, String token) {
        log.info("EnrollmentService::enrollUser:inside the method");
        CustomResponse response = new CustomResponse();
        try {
            String userId = accessTokenValidator.verifyUserToken(token);
            log.info("UserId from auth token {}", userId);
            if (StringUtils.isBlank(userId) || userId.equalsIgnoreCase(Constants.UNAUTHORIZED)) {
                response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
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
                cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD_COURSES,
                        Constants.TABLE_USER_EXTERNAL_ENROLMENTS_T1, userCourseEnrollMap);
                response.setResponseCode(HttpStatus.OK);
                response.setResult(userCourseEnrollMap);
                CustomResponseList customResponseList=new CustomResponseList();
                customResponseList.setResponseCode(HttpStatus.OK);
                List<Object> resultList;
                String cachedData = cacheService.getCache(userId);
                if (cachedData == null) {
                    resultList = new ArrayList<>();
                } else {
                    customResponseList = objectMapper.readValue(cachedData, CustomResponseList.class);
                    resultList = customResponseList.getResult();
                }
                resultList.add(userCourseEnrollMap);
                customResponseList.setResult(resultList);
                cacheService.putCache(userId, customResponseList);
                cacheService.putCache(userId + userCourseEnroll.get("courseId").asText(), response);
                return response;
            } else {
                response.setMessage("CourseId is missing");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
        } catch (Exception e) {
            log.error("error while processing", e);
            throw new CustomException("ERROR", e.getMessage(), HttpStatus.BAD_REQUEST);
        }

    }

    @Override
    public CustomResponseList readByUserId(String token) {
        log.info("EnrollmentService::readByUserId:inside the method");
        CustomResponseList response = new CustomResponseList();
        try {
            String userId = accessTokenValidator.verifyUserToken(token);
            log.info("UserId from auth token {}", userId);
            if (StringUtils.isBlank(userId) || userId.equalsIgnoreCase(Constants.UNAUTHORIZED)) {
                response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            String cacheResponse = cacheService.getCache(userId);
            if (cacheResponse != null) {
                log.info("EnrollmentServiceImpl :: readByUserId :: Data reading from cache");
                response = objectMapper.readValue(cacheResponse, CustomResponseList.class);
            } else {
                List<String> fields = Arrays.asList("userid", "courseid", "completedon", "completionpercentage", "enrolled_date", "issued_certificates", "progress", "status"); // Assuming user_id is the column name in your table
                Map<String, Object> propertyMap = new HashMap<>();
                propertyMap.put("userid", userId);
                List<Map<String, Object>> userEnrollmentList = cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD_COURSES,
                        Constants.TABLE_USER_EXTERNAL_ENROLMENTS_T1,
                        propertyMap,
                        fields
                );
                if (!userEnrollmentList.isEmpty()) {
                    response.setResponseCode(HttpStatus.OK);
                    response.setResult(Collections.singletonList(userEnrollmentList));
                    cacheService.putCache(userId, response);
                } else {
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    response.setMessage("User is not enrolled into any courses");
                }
            }
            return response;
        } catch (Exception e) {
            log.error("error while processing", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public CustomResponse readByUserIdAndCourseId(String courseid, String token) {
        log.info("EnrollmentService::readByUserIdAndCourseId:inside the method");
        CustomResponse response = new CustomResponse();
        try {
            String userId = accessTokenValidator.verifyUserToken(token);
            if (StringUtils.isBlank(userId) || userId.equalsIgnoreCase(Constants.UNAUTHORIZED)) {
                response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            String cacheResponse = cacheService.getCache(userId + courseid);
            if (cacheResponse != null) {
                log.info("EnrollmentServiceImpl :: readByUserIdAndCourseId :: Data reading from cache");
                response = objectMapper.readValue(cacheResponse, CustomResponse.class);
            } else {
                List<String> fields = Arrays.asList("userid", "courseid", "completedon", "completionpercentage", "enrolled_date", "issued_certificates", "progress", "status"); // Assuming user_id is the column name in your table
                Map<String, Object> propertyMap = new HashMap<>();
                propertyMap.put("userid", userId);
                propertyMap.put("courseid", courseid);
                List<Map<String, Object>> userEnrollmentList = cassandraOperation.getRecordsByProperties(
                        Constants.KEYSPACE_SUNBIRD_COURSES,
                        Constants.TABLE_USER_EXTERNAL_ENROLMENTS_T1,
                        propertyMap,
                        fields
                );
                if (!userEnrollmentList.isEmpty()) {
                    for (Map<String, Object> enrollment : userEnrollmentList) {
                        if (!enrollment.isEmpty()) {
                            response.setResponseCode(HttpStatus.OK);
                            response.setResult(enrollment);
                            cacheService.putCache(userId + courseid, response);
                        } else {
                            response.setResponseCode(HttpStatus.BAD_REQUEST);
                            response.setMessage("courseId is not matching");
                        }
                    }
                } else {
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    response.setMessage("User not enrolled into the course");
                }
            }
            return response;
        } catch (Exception e) {
            log.error("error while processing", e);
            throw new RuntimeException(e);
        }
    }
}
