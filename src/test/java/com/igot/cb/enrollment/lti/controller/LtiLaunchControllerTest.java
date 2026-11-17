package com.igot.cb.enrollment.lti.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import com.igot.cb.enrollment.lti.service.LtiLaunchService;
import com.igot.cb.util.Constants;
import com.igot.cb.util.dto.SBApiResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class LtiLaunchControllerTest {

    @InjectMocks
    private LtiLaunchController ltiLaunchController;

    @Mock
    private LtiLaunchService ltiLaunchService;

    private static final String PROVIDER_ID = "MOODLE";
    private static final String COURSE_ID = "course-123";
    private static final String TOKEN = "valid-token";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    @DisplayName("Delegates to the service and relays its response body/status on success")
    void courseRedirect_success_relaysServiceResponse() {
        SBApiResponse mockResponse = new SBApiResponse();
        mockResponse.setResponseCode(HttpStatus.OK);
        Map<String, Object> result = new HashMap<>();
        result.put(Constants.LTI_LAUNCH_URL, "https://moodle.example.com/enrol/lti/login.php?iss=...");
        mockResponse.setResult(result);

        when(ltiLaunchService.buildCourseRedirectLink(PROVIDER_ID, COURSE_ID, TOKEN)).thenReturn(mockResponse);

        ResponseEntity<SBApiResponse> response = ltiLaunchController.courseRedirect(PROVIDER_ID, COURSE_ID, TOKEN);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockResponse, response.getBody());
        assertEquals("https://moodle.example.com/enrol/lti/login.php?iss=...",
                response.getBody().getResult().get(Constants.LTI_LAUNCH_URL));
    }

    @Test
    @DisplayName("Relays a failure response's status code too, not just the body")
    void courseRedirect_failure_relaysFailureStatus() {
        SBApiResponse mockResponse = new SBApiResponse();
        mockResponse.setResponseCode(HttpStatus.FORBIDDEN);
        mockResponse.getParams().setMsg(Constants.USER_NOT_ENROLLED_MESSAGE);

        when(ltiLaunchService.buildCourseRedirectLink(PROVIDER_ID, COURSE_ID, TOKEN)).thenReturn(mockResponse);

        ResponseEntity<SBApiResponse> response = ltiLaunchController.courseRedirect(PROVIDER_ID, COURSE_ID, TOKEN);

        assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
        assertEquals(Constants.USER_NOT_ENROLLED_MESSAGE, response.getBody().getParams().getMsg());
    }
}
