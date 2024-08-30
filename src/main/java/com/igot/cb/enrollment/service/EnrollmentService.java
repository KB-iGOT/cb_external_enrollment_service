package com.igot.cb.enrollment.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.enrollment.entity.ContentPartnerEntity;
import com.igot.cb.util.dto.SBApiResponse;

public interface EnrollmentService {

  SBApiResponse enrollUser(JsonNode userCourseEnroll, String token);

  SBApiResponse readByUserId(String token);

  SBApiResponse readByUserIdAndCourseId(String courseId,String token);

  ContentPartnerEntity getContentDetailsByPartnerName(String name);
}
