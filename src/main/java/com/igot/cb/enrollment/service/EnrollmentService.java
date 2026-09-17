package com.igot.cb.enrollment.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.util.dto.SBApiResponse;

import java.util.Map;

public interface EnrollmentService {

  SBApiResponse enrollUser(JsonNode userCourseEnroll, String token);

  SBApiResponse readByUserId(Map<String, Object> searchRequest, String token);

  SBApiResponse readByUserIdAndCourseId(String courseId,String token);

  SBApiResponse readByUserIdAndCourseIdV2(String courseId, String token);

  SBApiResponse readByUserIdAndPartnerId(Map<String, Object> searchRequest, String token);

  SBApiResponse userProgressUpdate(JsonNode jsonNode, String partnerid);

  SBApiResponse enrolValidation(JsonNode userCourseEnroll, String token);

  SBApiResponse getUserEnrolmentByExternalId(String userId, String externalId, String partnerCode) ;

  SBApiResponse karmapointsDeductionRule(JsonNode userCourseEnroll, String token);
}
