package com.igot.cb.enrollment.lti.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.util.Constants;
import com.igot.cb.util.TransformUtility;
import com.igot.cb.util.dto.SBApiResponse;
import com.igot.cb.util.dto.SunbirdApiRespParam;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;

class LtiLaunchServiceImplTest {

    @InjectMocks
    private LtiLaunchServiceImpl ltiLaunchService;

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private TransformUtility transformUtility;

    private static final String PROVIDER_ID = "MOODLE";
    private static final String COURSE_ID = "course-123";
    private static final String TOKEN = "valid-token";
    private static final String USER_ID = "user-abc";

    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        lenient().when(transformUtility.createDefaultResponse(anyString())).thenAnswer(invocation -> {
            SBApiResponse response = new SBApiResponse();
            response.setResponseCode(HttpStatus.OK);
            response.setParams(new SunbirdApiRespParam("test-id"));
            response.getParams().setStatus(Constants.SUCCESS);
            return response;
        });

        lenient().when(transformUtility.buildFailedResponse(any(SBApiResponse.class), anyString(), any(HttpStatus.class)))
                .thenAnswer(invocation -> {
                    SBApiResponse resp = invocation.getArgument(0);
                    HttpStatus status = invocation.getArgument(2);
                    resp.setResponseCode(status);
                    resp.getParams().setStatus(Constants.FAILED);
                    resp.getParams().setMsg(invocation.getArgument(1));
                    return resp;
                });
    }

    private void stubValidUser() {
        lenient().when(transformUtility.validateAndGetUserId(eq(TOKEN), any(SBApiResponse.class))).thenReturn(USER_ID);
    }

    private void stubInvalidUser() {
        lenient().when(transformUtility.validateAndGetUserId(eq(TOKEN), any(SBApiResponse.class)))
                .thenAnswer(invocation -> {
                    SBApiResponse response = invocation.getArgument(1);
                    transformUtility.buildFailedResponse(response, "Invalid or missing token", HttpStatus.BAD_REQUEST);
                    return null;
                });
    }

    private void stubEnrolled(boolean enrolled) {
        List<Map<String, Object>> records = enrolled
                ? Collections.singletonList(Map.of(Constants.USER_ID, USER_ID, Constants.COURSE_ID, COURSE_ID))
                : Collections.emptyList();
        lenient().when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                eq(Constants.KEYSPACE_SUNBIRD_COURSES),
                eq(Constants.TABLE_USER_EXTERNAL_ENROLMENTS),
                anyMap(),
                isNull(),
                anyInt()
        )).thenReturn(records);
    }

    private JsonNode fullLtiConfig() {
        ObjectNode ltiConfig = mapper.createObjectNode();
        ltiConfig.put(Constants.OIDC_LOGIN_INITIATION_URL, "https://moodle.example.com/enrol/lti/login.php");
        ltiConfig.put(Constants.LTI_CLIENT_ID, "portal");
        ltiConfig.put(Constants.REDIRECT_LAUNCH_URL, "https://moodle.example.com/enrol/lti/launch.php");
        ltiConfig.put(Constants.DEPLOYMENT_ID, "IGOT-TESTINGLTIVP-25344");
        ltiConfig.put(Constants.TOOL_ISSUER, "https://portal.dev.karmayogibharat.net/auth/realms/sunbird");

        ObjectNode data = mapper.createObjectNode();
        data.set(Constants.LTI_CONFIGURATION, ltiConfig);

        ObjectNode result = mapper.createObjectNode();
        result.set(Constants.DATA, data);
        return result;
    }

    private JsonNode fullLtiConfigMissing(String fieldToBlank) {
        ObjectNode result = (ObjectNode) fullLtiConfig();
        ObjectNode ltiConfig = (ObjectNode) result.path(Constants.DATA).path(Constants.LTI_CONFIGURATION);
        ltiConfig.put(fieldToBlank, "");
        return result;
    }

    @Test
    @DisplayName("Returns failure response as-is when token validation fails")
    void buildCourseRedirectLink_invalidToken_returnsFailure() {
        stubInvalidUser();

        SBApiResponse response = ltiLaunchService.buildCourseRedirectLink(PROVIDER_ID, COURSE_ID, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    @DisplayName("Rejects blank providerId")
    void buildCourseRedirectLink_blankProviderId_returnsBadRequest() {
        stubValidUser();

        SBApiResponse response = ltiLaunchService.buildCourseRedirectLink("", COURSE_ID, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    @DisplayName("Rejects blank courseId")
    void buildCourseRedirectLink_blankCourseId_returnsBadRequest() {
        stubValidUser();

        SBApiResponse response = ltiLaunchService.buildCourseRedirectLink(PROVIDER_ID, "  ", TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    @Test
    @DisplayName("Not enrolled -> BAD_REQUEST with the not-enrolled message, provider is never read")
    void buildCourseRedirectLink_notEnrolled_returnsFailureWithoutReadingProvider() {
        stubValidUser();
        stubEnrolled(false);

        SBApiResponse response = ltiLaunchService.buildCourseRedirectLink(PROVIDER_ID, COURSE_ID, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.USER_NOT_ENROLLED_MESSAGE, response.getParams().getMsg());
    }

    @Test
    @DisplayName("Enrolled but provider lookup throws -> NOT_FOUND")
    void buildCourseRedirectLink_providerLookupFails_returnsNotFound() {
        stubValidUser();
        stubEnrolled(true);
        doThrow(new RuntimeException("boom")).when(transformUtility).callContentPartnerReadByPartnerIdApi(PROVIDER_ID);

        SBApiResponse response = ltiLaunchService.buildCourseRedirectLink(PROVIDER_ID, COURSE_ID, TOKEN);

        assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
    }

    @Test
    @DisplayName("Enrolled, provider found, but ltiConfiguration missing a required field -> BAD_REQUEST")
    void buildCourseRedirectLink_incompleteLtiConfig_returnsBadRequest() {
        stubValidUser();
        stubEnrolled(true);

        ObjectNode ltiConfig = mapper.createObjectNode();
        ltiConfig.put(Constants.OIDC_LOGIN_INITIATION_URL, "https://moodle.example.com/enrol/lti/login.php");
        ltiConfig.put(Constants.LTI_CLIENT_ID, "portal");
        // redirectLaunchUrl, deploymentId, toolIssuer intentionally left out
        ObjectNode data = mapper.createObjectNode();
        data.set(Constants.LTI_CONFIGURATION, ltiConfig);
        ObjectNode result = mapper.createObjectNode();
        result.set(Constants.DATA, data);

        when(transformUtility.callContentPartnerReadByPartnerIdApi(PROVIDER_ID)).thenReturn(result);

        SBApiResponse response = ltiLaunchService.buildCourseRedirectLink(PROVIDER_ID, COURSE_ID, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    @DisplayName("Only oidcLoginInitiationUrl blank -> BAD_REQUEST")
    void buildCourseRedirectLink_onlyOidcLoginInitiationUrlBlank_returnsBadRequest() {
        stubValidUser();
        stubEnrolled(true);
        when(transformUtility.callContentPartnerReadByPartnerIdApi(PROVIDER_ID))
                .thenReturn(fullLtiConfigMissing(Constants.OIDC_LOGIN_INITIATION_URL));

        SBApiResponse response = ltiLaunchService.buildCourseRedirectLink(PROVIDER_ID, COURSE_ID, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    @DisplayName("Only clientId blank -> BAD_REQUEST")
    void buildCourseRedirectLink_onlyClientIdBlank_returnsBadRequest() {
        stubValidUser();
        stubEnrolled(true);
        when(transformUtility.callContentPartnerReadByPartnerIdApi(PROVIDER_ID))
                .thenReturn(fullLtiConfigMissing(Constants.LTI_CLIENT_ID));

        SBApiResponse response = ltiLaunchService.buildCourseRedirectLink(PROVIDER_ID, COURSE_ID, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    @DisplayName("Only targetLinkUri (redirectLaunchUrl) blank -> BAD_REQUEST")
    void buildCourseRedirectLink_onlyTargetLinkUriBlank_returnsBadRequest() {
        stubValidUser();
        stubEnrolled(true);
        when(transformUtility.callContentPartnerReadByPartnerIdApi(PROVIDER_ID))
                .thenReturn(fullLtiConfigMissing(Constants.REDIRECT_LAUNCH_URL));

        SBApiResponse response = ltiLaunchService.buildCourseRedirectLink(PROVIDER_ID, COURSE_ID, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    @DisplayName("Only deploymentId blank -> BAD_REQUEST")
    void buildCourseRedirectLink_onlyDeploymentIdBlank_returnsBadRequest() {
        stubValidUser();
        stubEnrolled(true);
        when(transformUtility.callContentPartnerReadByPartnerIdApi(PROVIDER_ID))
                .thenReturn(fullLtiConfigMissing(Constants.DEPLOYMENT_ID));

        SBApiResponse response = ltiLaunchService.buildCourseRedirectLink(PROVIDER_ID, COURSE_ID, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    @DisplayName("Only toolIssuer blank -> BAD_REQUEST")
    void buildCourseRedirectLink_onlyToolIssuerBlank_returnsBadRequest() {
        stubValidUser();
        stubEnrolled(true);
        when(transformUtility.callContentPartnerReadByPartnerIdApi(PROVIDER_ID))
                .thenReturn(fullLtiConfigMissing(Constants.TOOL_ISSUER));

        SBApiResponse response = ltiLaunchService.buildCourseRedirectLink(PROVIDER_ID, COURSE_ID, TOKEN);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    @DisplayName("Enrolled, full config present -> builds the LTI launch URL with every expected query param")
    void buildCourseRedirectLink_success_buildsLaunchUrl() {
        stubValidUser();
        stubEnrolled(true);
        when(transformUtility.callContentPartnerReadByPartnerIdApi(PROVIDER_ID)).thenReturn(fullLtiConfig());

        SBApiResponse response = ltiLaunchService.buildCourseRedirectLink(PROVIDER_ID, COURSE_ID, TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        String launchUrl = (String) response.getResult().get(Constants.LTI_LAUNCH_URL);
        assertTrue(launchUrl.startsWith("https://moodle.example.com/enrol/lti/login.php"));
        assertTrue(launchUrl.contains("iss=https"));
        assertTrue(launchUrl.contains("login_hint=" + USER_ID));
        assertTrue(launchUrl.contains("target_link_uri="));
        assertTrue(launchUrl.contains("client_id=portal"));
        assertTrue(launchUrl.contains("lti_deployment_id=IGOT-TESTINGLTIVP-25344"));
        assertTrue(launchUrl.contains("lti_message_hint=" + Constants.LTI_COURSE_LAUNCH_HINT_PREFIX + COURSE_ID)
                || launchUrl.contains("lti_message_hint=courseLaunch%3A" + COURSE_ID));
    }

    @Test
    @DisplayName("isUserEnrolled returns true when Cassandra finds an existing enrolment record")
    void isUserEnrolled_recordExists_returnsTrue() {
        stubEnrolled(true);

        boolean enrolled = ltiLaunchService.isUserEnrolled(USER_ID, COURSE_ID);

        assertTrue(enrolled);
    }

    @Test
    @DisplayName("isUserEnrolled returns false when no enrolment record is found")
    void isUserEnrolled_noRecord_returnsFalse() {
        stubEnrolled(false);

        boolean enrolled = ltiLaunchService.isUserEnrolled(USER_ID, COURSE_ID);

        assertFalse(enrolled);
    }

    @Test
    @DisplayName("isUserEnrolled queries with the expected userId/courseId property map")
    void isUserEnrolled_queriesWithExpectedProperties() {
        stubEnrolled(false);

        ltiLaunchService.isUserEnrolled(USER_ID, COURSE_ID);

        Map<String, Object> expectedProperties = new HashMap<>();
        expectedProperties.put(Constants.USER_ID, USER_ID);
        expectedProperties.put(Constants.COURSE_ID, COURSE_ID);
        verify(cassandraOperation).getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD_COURSES,
                Constants.TABLE_USER_EXTERNAL_ENROLMENTS,
                expectedProperties,
                null,
                1
        );
    }
}
