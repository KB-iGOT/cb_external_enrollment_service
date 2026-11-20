package com.igot.cb.enrollment.lti.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.enrollment.lti.service.LtiLaunchService;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.util.Constants;
import com.igot.cb.util.TransformUtility;
import com.igot.cb.util.dto.SBApiResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * New, isolated LTI package - does not touch the existing enrollment/SSO flow.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class LtiLaunchServiceImpl implements LtiLaunchService {
    private final CassandraOperation cassandraOperation;
    private final TransformUtility transformUtility;
    private final ObjectMapper objectMapper;

    @Override
    public SBApiResponse buildCourseRedirectLink(String providerId, String courseId, String token) {
        SBApiResponse response = transformUtility.createDefaultResponse(Constants.CIOS_ENROLLMENT_CREATE);
        String userId = transformUtility.validateAndGetUserId(token, response);
        if (StringUtils.isBlank(userId)) {
            return response;
        }
        if (StringUtils.isBlank(providerId) || StringUtils.isBlank(courseId)) {
            return transformUtility.buildFailedResponse(response, "providerId and courseId are mandatory", HttpStatus.BAD_REQUEST);
        }
        if (isUserEnrolled(userId, courseId)) {
            response.getParams().setStatus(Constants.SUCCESS);
            response.setResponseCode(HttpStatus.OK);
        } else {
            return transformUtility.buildFailedResponse(response, Constants.USER_NOT_ENROLLED_MESSAGE, HttpStatus.BAD_REQUEST);
        }

        JsonNode providerResult;
        try {
            providerResult = transformUtility.callContentPartnerReadByPartnerIdApi(providerId);
        } catch (Exception e) {
            log.error("LtiLaunchServiceImpl::buildCourseRedirectLink: failed to read provider {}", providerId, e);
            return transformUtility.buildFailedResponse(response, "Provider not found for id: " + providerId, HttpStatus.NOT_FOUND);
        }
        JsonNode ltiConfig = providerResult.path(Constants.DATA).path(Constants.LTI_CONFIGURATION);
        String oidcLoginInitiationUrl = ltiConfig.path(Constants.OIDC_LOGIN_INITIATION_URL).asText("");
        String clientId = ltiConfig.path(Constants.LTI_CLIENT_ID).asText("");
        String targetLinkUri = ltiConfig.path(Constants.REDIRECT_LAUNCH_URL).asText("");
        String deploymentId = ltiConfig.path(Constants.DEPLOYMENT_ID).asText("");
        String issuer = ltiConfig.path(Constants.TOOL_ISSUER).asText("");
        if (StringUtils.isBlank(oidcLoginInitiationUrl) || StringUtils.isBlank(clientId)
                || StringUtils.isBlank(targetLinkUri) || StringUtils.isBlank(deploymentId) || StringUtils.isBlank(issuer)) {
            return transformUtility.buildFailedResponse(response, "LTI configuration incomplete for provider: " + providerId, HttpStatus.BAD_REQUEST);
        }

        // The course's own providerCustomClaim (captured verbatim from the Tool at onboarding
        // time - Moodle's {"id": ...}, Coursera's {link_type, course_id, ...}, etc.) is the
        // real custom-claim shape the Tool needs - courseId itself (cb-pores-service's own
        // content identifier) is NOT that value for every provider, so it can't be reused
        // directly as "id".
        JsonNode courseContent;
        try {
            courseContent = transformUtility.callCiosContentReadAPi(courseId);
        } catch (Exception e) {
            log.error("LtiLaunchServiceImpl::buildCourseRedirectLink: failed to read course content for courseId {}", courseId, e);
            return transformUtility.buildFailedResponse(response, "Course not found for id: " + courseId, HttpStatus.NOT_FOUND);
        }
        JsonNode providerCustomClaim = courseContent != null ? courseContent.path(Constants.PROVIDER_CUSTOM_CLAIM) : null;
        if (providerCustomClaim == null || providerCustomClaim.isMissingNode() || providerCustomClaim.isNull() || !providerCustomClaim.isObject()) {
            return transformUtility.buildFailedResponse(response, "LTI custom claim configuration missing for course: " + courseId, HttpStatus.BAD_REQUEST);
        }

        Map<String, Object> messageHintFields = objectMapper.convertValue(providerCustomClaim, LinkedHashMap.class);
        messageHintFields.put("partnerCode", deploymentId);
        String ltiMessageHint;
        try {
            ltiMessageHint = objectMapper.writeValueAsString(messageHintFields);
        } catch (JsonProcessingException e) {
            log.error("LtiLaunchServiceImpl::buildCourseRedirectLink: failed to build lti_message_hint JSON for courseId {}", courseId, e);
            return transformUtility.buildFailedResponse(response, "Failed to build LTI launch link", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        String launchUrl = UriComponentsBuilder.fromUriString(oidcLoginInitiationUrl)
                .queryParam(Constants.LTI_PARAM_ISS, issuer)
                .queryParam(Constants.LTI_PARAM_LOGIN_HINT, userId)
                .queryParam(Constants.LTI_PARAM_TARGET_LINK_URI, targetLinkUri)
                .queryParam(Constants.LTI_PARAM_CLIENT_ID, clientId)
                .queryParam(Constants.LTI_PARAM_DEPLOYMENT_ID, deploymentId)
                .queryParam(Constants.LTI_PARAM_MESSAGE_HINT, ltiMessageHint)
                .build()
                .encode()
                .toUriString();

        Map<String, Object> result = new HashMap<>();
        result.put(Constants.LTI_LAUNCH_URL, launchUrl);
        response.setResult(result);
        return response;
    }

    public boolean isUserEnrolled(String userId, String courseId) {
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
        return !userEnrollmentList.isEmpty();
    }
}
