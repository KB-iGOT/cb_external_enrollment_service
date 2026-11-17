package com.igot.cb.enrollment.lti.service;

import com.igot.cb.util.dto.SBApiResponse;

public interface LtiLaunchService {

    SBApiResponse buildCourseRedirectLink(String providerId, String courseId, String token);
}
