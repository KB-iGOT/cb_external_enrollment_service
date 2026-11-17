package com.igot.cb.enrollment.lti.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
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

        String launchUrl = UriComponentsBuilder.fromUriString(oidcLoginInitiationUrl)
                .queryParam(Constants.LTI_PARAM_ISS, issuer)
                .queryParam(Constants.LTI_PARAM_LOGIN_HINT, userId)
                .queryParam(Constants.LTI_PARAM_TARGET_LINK_URI, targetLinkUri)
                .queryParam(Constants.LTI_PARAM_CLIENT_ID, clientId)
                .queryParam(Constants.LTI_PARAM_DEPLOYMENT_ID, deploymentId)
                .queryParam(Constants.LTI_PARAM_MESSAGE_HINT, Constants.LTI_COURSE_LAUNCH_HINT_PREFIX + courseId)
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
