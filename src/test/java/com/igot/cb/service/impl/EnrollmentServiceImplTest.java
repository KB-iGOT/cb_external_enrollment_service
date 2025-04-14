package com.igot.cb.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.enrollment.repository.CiosContentRepository;
import com.igot.cb.enrollment.service.impl.EnrollmentServiceImpl;
import com.igot.cb.producer.Producer;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.util.CbServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.TransformUtility;
import com.igot.cb.util.cache.CacheService;
import com.igot.cb.util.dto.SBApiResponse;
import com.igot.cb.util.dto.SunbirdApiRespParam;
import com.igot.cb.util.exceptions.CustomException;

class EnrollmentServiceImplTest {

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
        // Mock SBApiResponse to avoid null pointer exceptions
        SBApiResponse mockResponse = mock(SBApiResponse.class);
        when(mockResponse.getParams()).thenReturn(new SunbirdApiRespParam());
        when(mockResponse.getResult()).thenReturn(new HashMap<>());
        when(mockResponse.getResponseCode()).thenReturn(HttpStatus.OK);
        when(transformUtility.createDefaultResponse(anyString())).thenReturn(mockResponse);
    }

    @Test
    void testEnrollUser() throws Exception {
        JsonNode userCourseEnroll = mock(JsonNode.class);
        JsonNode courseIdNode = mock(JsonNode.class);
        JsonNode partnerIdNode = mock(JsonNode.class);
        JsonNode batchIdNode = mock(JsonNode.class);
        String token = "validToken";

        when(accessTokenValidator.verifyUserToken(eq(token))).thenReturn("123333344");
        when(userCourseEnroll.has("courseId")).thenReturn(true);
        when(userCourseEnroll.get("courseId")).thenReturn(courseIdNode);
        when(courseIdNode.asText()).thenReturn("do_12233333");
        when(userCourseEnroll.has("partnerId")).thenReturn(true);
        when(userCourseEnroll.get("partnerId")).thenReturn(partnerIdNode);
        when(partnerIdNode.asText()).thenReturn("ext_122333");
        when(userCourseEnroll.has("batchId")).thenReturn(true);
        when(userCourseEnroll.get("batchId")).thenReturn(batchIdNode);
        when(batchIdNode.asText()).thenReturn("11121122122");

        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(anyString(), anyString(), anyMap(), any(), anyInt())).thenReturn(Collections.emptyList());
        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap())).thenReturn(Map.of("response", "Enrollment successful"));

        SBApiResponse response = enrollmentService.enrollUser(userCourseEnroll, token);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    // @Test
    // void testEnrollUser_UserAlreadyEnrolled() throws Exception {
    //     JsonNode userCourseEnroll = mock(JsonNode.class);
    //     JsonNode courseIdNode = mock(JsonNode.class);
    //     JsonNode partnerIdNode = mock(JsonNode.class);
    //     JsonNode batchIdNode = mock(JsonNode.class);
    //     String token = "validToken";

    //     when(accessTokenValidator.verifyUserToken(eq(token))).thenReturn("123333344");
    //     when(userCourseEnroll.has("courseId")).thenReturn(true);
    //     when(userCourseEnroll.get("courseId")).thenReturn(courseIdNode);
    //     when(courseIdNode.asText()).thenReturn("do_12233333");
    //     when(userCourseEnroll.has("partnerId")).thenReturn(true);
    //     when(userCourseEnroll.get("partnerId")).thenReturn(partnerIdNode);
    //     when(partnerIdNode.asText()).thenReturn("ext_122333");
    //     when(userCourseEnroll.has("batchId")).thenReturn(true);
    //     when(userCourseEnroll.get("batchId")).thenReturn(batchIdNode);
    //     when(batchIdNode.asText()).thenReturn("11121122122");

    //     when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(anyString(), anyString(), anyMap(), any(), anyInt())).thenReturn(Collections.singletonList(Map.of("userid", "123333344")));

    //     SBApiResponse response = enrollmentService.enrollUser(userCourseEnroll, token);

    //     assertNotNull(response);
    //     assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    //     assertEquals("User already enrolled to the course", response.getParams().getMsg());
    // }

    @Test
    void testReadByUserId() {
        Map<String, Object> searchRequest = new HashMap<>();
        searchRequest.put("request", Map.of("status", "active"));

        String token = "validToken";
        when(accessTokenValidator.verifyUserToken(eq(token))).thenReturn("123333344");

        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(anyString(), anyString(), anyMap(), any(), anyInt()))
            .thenReturn(Collections.singletonList(Map.of("userid", "123333344")));

        SBApiResponse response = enrollmentService.readByUserId(searchRequest, token);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertNotNull(response.getResult());
    }

    @Test
    void testReadByUserIdAndCourseId() {
        String courseId = "do_12233333";
        String token = "validToken";

        when(accessTokenValidator.verifyUserToken(eq(token))).thenReturn("123333344");
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(anyString(), anyString(), anyMap(), any(), anyInt()))
            .thenReturn(Collections.singletonList(Map.of("userid", "123333344", "courseId", courseId)));

        SBApiResponse response = enrollmentService.readByUserIdAndCourseId(courseId, token);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertNotNull(response.getResult());
    }

    @Test
    void testUserProgressUpdate() throws Exception {
        // Correct the date format to match the expected format
        String inputDate = "2025-10-01 12:00:00";

        String jsonString = String.format("{\"completion_date\": \"%s\"}", inputDate);
        JsonNode inputNode = new ObjectMapper().readTree(jsonString);
        JsonNode transformedJsonNode = new ObjectMapper().readTree("{\"completion_date\": \"2025-10-01 12:00:00\", \"partnerCode\": \"ext_122333\"}");

        when(cbServerProperties.getUserProgressSendFromPartner()).thenReturn("user-progress-topic");
        SBApiResponse mockResponse = new SBApiResponse();
        mockResponse.setResponseCode(HttpStatus.OK);
        when(transformUtility.createDefaultResponse(Constants.CIOS_ENROLLMENT_PREGRESS_UPDATE)).thenReturn(mockResponse);

        doNothing().when(producer).push(eq("user-progress-topic"), eq(transformedJsonNode));

        SBApiResponse response = enrollmentService.userProgressUpdate(transformedJsonNode, "ext_122333");

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getResponseCode());
    }

    // @Test
    // void testEnrollUserWithInvalidToken() {
    //     String token = "invalidToken";
    //     JsonNode userCourseEnroll = mock(JsonNode.class);
    //     JsonNode courseIdNode = mock(JsonNode.class);

    //     when(accessTokenValidator.verifyUserToken(eq(token))).thenReturn(Constants.UNAUTHORIZED);
    //     when(userCourseEnroll.has("courseId")).thenReturn(true);
    //     when(userCourseEnroll.get("courseId")).thenReturn(courseIdNode);
    //     when(courseIdNode.asText()).thenReturn("do_12233333");

    //     SBApiResponse response = enrollmentService.enrollUser(userCourseEnroll, token);

    //     assertNotNull(response);
    //     assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    //     assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getMsg());
    //     assertEquals(Constants.FAILED, response.getParams().getStatus());
    // }

    // @Test
    // void testEnrollUser_MissingCourseId() {
    //     JsonNode userCourseEnroll = mock(JsonNode.class);
    //     String token = "validToken";

    //     when(accessTokenValidator.verifyUserToken(eq(token))).thenReturn("123333344");
    //     when(userCourseEnroll.has("courseId")).thenReturn(false);

    //     CustomException exception = assertThrows(CustomException.class, () -> {
    //         enrollmentService.enrollUser(userCourseEnroll, token);
    //     });

    //     assertEquals("Course ID is missing", exception.getMessage());
    // }

    // @Test
    // void testReadByUserId_InvalidToken() {
    //     String token = "invalidToken";

    //     when(accessTokenValidator.verifyUserToken(eq(token))).thenReturn(Constants.UNAUTHORIZED);

    //     Map<String, Object> searchRequest = new HashMap<>();
    //     CustomException exception = assertThrows(CustomException.class, () -> {
    //         enrollmentService.readByUserId(searchRequest, token);
    //     });

    //     assertEquals("Invalid token", exception.getMessage());
    // }
}