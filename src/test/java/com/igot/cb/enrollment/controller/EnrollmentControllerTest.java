package com.igot.cb.enrollment.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.enrollment.service.EnrollmentService;
import com.igot.cb.util.dto.SBApiResponse;

class EnrollmentControllerTest {

    @InjectMocks
    private EnrollmentController enrollmentController;

    @Mock
    private EnrollmentService enrollmentService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testCreate_Success() {
        JsonNode userCourseEnroll = mock(JsonNode.class);
        String token = "validToken";

        SBApiResponse mockResponse = new SBApiResponse();
        mockResponse.setResponseCode(HttpStatus.OK);

        when(enrollmentService.enrollUser(userCourseEnroll, token)).thenReturn(mockResponse);

        ResponseEntity<SBApiResponse> response = enrollmentController.create(userCourseEnroll, token);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockResponse, response.getBody());
    }

    @Test
    void testReadByUserId_Success() {
        Map<String, Object> searchRequest = Map.of("status", "active");
        String token = "validToken";

        SBApiResponse mockResponse = new SBApiResponse();
        mockResponse.setResponseCode(HttpStatus.OK);

        when(enrollmentService.readByUserId(searchRequest, token)).thenReturn(mockResponse);

        ResponseEntity<?> response = enrollmentController.readByUserId(searchRequest, token);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockResponse, response.getBody());
    }

    @Test
    void testReadByUserIdAndCourseId_Success() {
        String courseId = "course123";
        String token = "validToken";

        SBApiResponse mockResponse = new SBApiResponse();
        mockResponse.setResponseCode(HttpStatus.OK);

        when(enrollmentService.readByUserIdAndCourseId(courseId, token)).thenReturn(mockResponse);

        ResponseEntity<?> response = enrollmentController.readByUserIdAndCourseId(courseId, token);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockResponse, response.getBody());
    }

    @Test
    void testUserProgressUpdate_Success() {
        JsonNode jsonNode = mock(JsonNode.class);
        String partnerCode = "partner123";

        SBApiResponse mockResponse = new SBApiResponse();
        mockResponse.setResponseCode(HttpStatus.OK);

        when(enrollmentService.userProgressUpdate(jsonNode, partnerCode)).thenReturn(mockResponse);

        ResponseEntity<?> response = enrollmentController.userProgressUpdate(jsonNode, partnerCode);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockResponse, response.getBody());
    }

    @Test
    void testEnrolValidation_Success() {
        JsonNode userCourseEnroll = mock(JsonNode.class);
        String token = "validToken";

        SBApiResponse mockResponse = new SBApiResponse();
        mockResponse.setResponseCode(HttpStatus.OK);

        when(enrollmentService.enrolValidation(userCourseEnroll, token)).thenReturn(mockResponse);

        ResponseEntity<SBApiResponse> response = enrollmentController.enrolValidation(userCourseEnroll, token);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockResponse, response.getBody());
    }
}
