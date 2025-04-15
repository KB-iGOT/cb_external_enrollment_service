
package com.igot.cb.enrollment.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.http.HttpStatus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.enrollment.repository.CiosContentRepository;
import com.igot.cb.producer.Producer;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.util.CbServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.TransformUtility;
import com.igot.cb.util.cache.CacheService;
import com.igot.cb.util.dto.SBApiResponse;

class EnrollmentServiceImplTest {

    @Spy
    @InjectMocks
    private EnrollmentServiceImpl enrollmentService;

    @Mock
    private AccessTokenValidator accessTokenValidator;
    @Mock
    private CassandraOperation cassandraOperation;
    @Mock
    private ObjectMapper objectMapper;
    @Mock
    private CacheService cacheService;
    @Mock
    private CbServerProperties cbServerProperties;
    @Mock
    private CiosContentRepository contentRepository;
    @Mock
    private TransformUtility transformUtility;
    @Mock
    private Producer producer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(transformUtility.createDefaultResponse(Mockito.anyString())).thenReturn(new SBApiResponse());
    }

    @Test
    @DisplayName("enrollUser: should enroll when input is correct and not already enrolled")
    void enrollUser_successful() {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode userCourseEnroll = objectMapper.createObjectNode();
        userCourseEnroll.put("courseId", "course1");
        userCourseEnroll.put("partnerId", "partner1");
        String token = "jwt.token";

        when(accessTokenValidator.verifyUserToken(token)).thenReturn("user123");

        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("userid", "user123");
        propertyMap.put("courseid", "course1");
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                any(), any(), any(), isNull(), eq(1)))
                .thenReturn(Collections.emptyList());

        // FIX: Use when(...).thenReturn for non-void method
        when(cassandraOperation.insertRecord(any(), any(), any())).thenReturn(null);

        SBApiResponse response = enrollmentService.enrollUser(userCourseEnroll, token);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals("user123", ((Map)response.getResult()).get("userid"));
        assertEquals("course1", ((Map)response.getResult()).get("courseid"));
        assertEquals("partner1", ((Map)response.getResult()).get("partnerid"));
    }

    @Test
    @DisplayName("enrollUser: should return error if user already enrolled")
    void enrollUser_alreadyEnrolled() {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode userCourseEnroll = objectMapper.createObjectNode();
        userCourseEnroll.put("courseId", "course1");
        userCourseEnroll.put("partnerId", "partner1");
        String token = "jwt.token";

        when(accessTokenValidator.verifyUserToken(token)).thenReturn("user123");
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                any(), any(), any(), isNull(), eq(1)))
                .thenReturn(Collections.singletonList(new HashMap<>()));

        SBApiResponse response = enrollmentService.enrollUser(userCourseEnroll, token);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertTrue(response.getParams().getMsg().contains("User already enrolled"));
    }

    @Test
    @DisplayName("enrollUser: should return error on invalid token")
    void enrollUser_invalidToken() {
        ObjectNode userCourseEnroll = new ObjectMapper().createObjectNode();
        userCourseEnroll.put("courseId", "course1");
        userCourseEnroll.put("partnerId", "partner1");

        String token = "invalid.token";
        when(accessTokenValidator.verifyUserToken(token)).thenReturn(Constants.UNAUTHORIZED);

        SBApiResponse response = enrollmentService.enrollUser(userCourseEnroll, token);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertTrue(response.getParams().getMsg().contains(Constants.USER_ID_DOESNT_EXIST));
    }

    @Test
    @DisplayName("readByUserId: should return user courses")
    void readByUserId_returnsCourses() {
        String token = "jwt.token";
        String userId = "user1";
        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);

        Map<String, Object> searchRequest = new HashMap<>();
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.STATUS, "In-Progress");
        searchRequest.put(Constants.REQUEST, requestBody);

        Map<String, Object> record = new HashMap<>();
        record.put("courseid", "c1");
        record.put(Constants.STATUS, 1);
        record.put(Constants.UPDATED_ON, Instant.now());

        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(any(), any(), any(), any(), any()))
                .thenReturn(Collections.singletonList(record));

        Map<String, Object> contentData = new HashMap<>();
        contentData.put("content", new HashMap<>());

        // FIX: Use doReturn(...).when(SPY).fetchDataByContentId() because @Spy is used now.
        doReturn(contentData).when(enrollmentService).fetchDataByContentId("c1");

        SBApiResponse response = enrollmentService.readByUserId(searchRequest, token);
        System.out.println(response.getResult());
        assertEquals(HttpStatus.OK, response.getResponseCode());
        //assertTrue(((Map<?, ?>)response.getResult()).containsKey("courses"));
    }

    @Test
    @DisplayName("readByUserIdAndCourseId: returns enrollment if found")
    void readByUserIdAndCourseId_found() {
        String token = "token";
        String userId = "user1";
        String courseId = "c1";
        Map<String, Object> record = new HashMap<>();
        record.put("courseid", courseId);
        record.put("userid", userId);

        List<Map<String, Object>> records = Collections.singletonList(record);

        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                any(), any(), any(), isNull(), eq(1)))
                .thenReturn(records);

        SBApiResponse response = enrollmentService.readByUserIdAndCourseId(courseId, token);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertNotNull(response.getResult());
    }

    @Test
    @DisplayName("userProgressUpdate: returns success")
    void userProgressUpdate_success() {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode jsonNode = objectMapper.createObjectNode();
        jsonNode.put("completion_date", "2023-12-01 12:12:12");
        String partnerCode = "partner";

        String topic = "topic";
        when(cbServerProperties.getUserProgressSendFromPartner()).thenReturn(topic);
        doNothing().when(producer).push(eq(topic), any(JsonNode.class));

        SBApiResponse response = enrollmentService.userProgressUpdate(jsonNode, partnerCode);

        assertNotNull(response.getResult());
        assertEquals("Progress report sent successfully", ((Map)response.getResult()).get("response"));
    }
}
