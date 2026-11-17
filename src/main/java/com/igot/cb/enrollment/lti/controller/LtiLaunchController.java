package com.igot.cb.enrollment.lti.controller;

import com.igot.cb.enrollment.lti.service.LtiLaunchService;
import com.igot.cb.util.Constants;
import com.igot.cb.util.dto.SBApiResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * New, isolated LTI package under its own /cios-enroll/lti sub-path - does not touch the existing
 * /cios-enroll/v1/* enrollment APIs or their controller/service classes. Neither endpoint here
 * issues a browser redirect itself - the backend's job ends at handing back the LTI launch URL;
 * the Angular frontend is responsible for the actual navigation (window.location.href), since
 * these are authenticated calls whose auth header a plain redirect couldn't carry anyway.
 */
@RestController
@RequestMapping("/cios-enroll/lti")
@RequiredArgsConstructor
public class LtiLaunchController {

    private final LtiLaunchService ltiLaunchService;
    /**
     * Checks the caller is enrolled in courseId, then returns the LTI launch URL as JSON (not a
     * 302) so an Angular caller can navigate to it itself - this endpoint is reached via an
     * authenticated HttpClient call (auth header attached by the interceptor), not a plain
     * browser redirect, which couldn't carry that header.
     */
    @GetMapping("/redirect/{providerId}/{courseId}")
    public ResponseEntity<SBApiResponse> courseRedirect(@PathVariable String providerId, @PathVariable String courseId,
                                                          @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        SBApiResponse response = ltiLaunchService.buildCourseRedirectLink(providerId, courseId, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
