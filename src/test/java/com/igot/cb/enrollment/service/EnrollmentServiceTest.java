package com.igot.cb.enrollment.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.util.dto.SBApiResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class EnrollmentServiceTest {

    @Mock
    private EnrollmentService enrollmentService;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        objectMapper = new ObjectMapper();
    }

    @Test
    void testEnrollUser() throws Exception {
        JsonNode userCourseEnroll = objectMapper.readTree("{\"userId\":\"123\",\"courseId\":\"456\"}");
        String token = "dummyToken";
        SBApiResponse mockResponse = new SBApiResponse();
        when(enrollmentService.enrollUser(userCourseEnroll, token)).thenReturn(mockResponse);

        SBApiResponse response = enrollmentService.enrollUser(userCourseEnroll, token);

        assertNotNull(response);
        verify(enrollmentService, times(1)).enrollUser(userCourseEnroll, token);
    }

    @Test
    void testReadByUserId() {
        Map<String, Object> searchRequest = new HashMap<>();
        searchRequest.put("userId", "123");
        String token = "dummyToken";
        SBApiResponse mockResponse = new SBApiResponse();
        when(enrollmentService.readByUserId(searchRequest, token)).thenReturn(mockResponse);

        SBApiResponse response = enrollmentService.readByUserId(searchRequest, token);

        assertNotNull(response);
        verify(enrollmentService, times(1)).readByUserId(searchRequest, token);
    }

    @Test
    void testReadByUserIdAndCourseId() {
        String courseId = "456";
        String token = "dummyToken";
        SBApiResponse mockResponse = new SBApiResponse();
        when(enrollmentService.readByUserIdAndCourseId(courseId, token)).thenReturn(mockResponse);

        SBApiResponse response = enrollmentService.readByUserIdAndCourseId(courseId, token);

        assertNotNull(response);
        verify(enrollmentService, times(1)).readByUserIdAndCourseId(courseId, token);
    }

    @Test
    void testUserProgressUpdate() throws Exception {
        JsonNode jsonNode = objectMapper.readTree("{\"progress\":50}");
        String partnerId = "partner123";
        SBApiResponse mockResponse = new SBApiResponse();
        when(enrollmentService.userProgressUpdate(jsonNode, partnerId)).thenReturn(mockResponse);

        SBApiResponse response = enrollmentService.userProgressUpdate(jsonNode, partnerId);

        assertNotNull(response);
        verify(enrollmentService, times(1)).userProgressUpdate(jsonNode, partnerId);
    }
}