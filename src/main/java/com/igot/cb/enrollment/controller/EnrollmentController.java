package com.igot.cb.enrollment.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.enrollment.service.EnrollmentService;
import com.igot.cb.util.Constants;
import com.igot.cb.util.dto.SBApiResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/cios-enroll")
public class EnrollmentController {

  private final EnrollmentService enrollmentService;

  public EnrollmentController(EnrollmentService enrollmentService) {
    this.enrollmentService = enrollmentService;
  }

  @PostMapping("/v1/create")
  public ResponseEntity<SBApiResponse> create(@RequestBody JsonNode userCourseEnroll, @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
    SBApiResponse response = enrollmentService.enrollUser(userCourseEnroll, token);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @PostMapping("/v1/courselist/byuserid")
  public ResponseEntity<SBApiResponse> readByUserId(@RequestBody Map<String, Object> searchRequest, @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
    SBApiResponse response = enrollmentService.readByUserId(searchRequest, token);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @GetMapping("/v1/readby/useridcourseid/{courseid}")
  public ResponseEntity<SBApiResponse> readByUserIdAndCourseId(@PathVariable String courseid, @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
    SBApiResponse response = enrollmentService.readByUserIdAndCourseId(courseid,token);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @PostMapping("/v1/user/progressupdate")
  public ResponseEntity<SBApiResponse> userProgressUpdate(@RequestBody JsonNode jsonNode, @RequestHeader(Constants.PARTNER_CODE) String partnerCode) {
    SBApiResponse response = enrollmentService.userProgressUpdate(jsonNode,partnerCode);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @PostMapping("/v1/validation")
  public ResponseEntity<SBApiResponse> enrolValidation(@RequestBody JsonNode userCourseEnroll, @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
    SBApiResponse response = enrollmentService.enrolValidation(userCourseEnroll, token);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @GetMapping("/v1/enrollment/status")
  public ResponseEntity<SBApiResponse> getUserEnrolmentByExternalId(
          @RequestParam String userId,
          @RequestParam String courseId,
          @RequestHeader("partnerCode") String partnerCode) {
    SBApiResponse response = enrollmentService.getUserEnrolmentByExternalId(userId, courseId, partnerCode);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @PostMapping("/v1/search")
  public ResponseEntity<SBApiResponse> readByUserIdAndPartnerId(
          @RequestBody Map<String, Object> searchRequest,
          @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
    SBApiResponse response = enrollmentService.readByUserIdAndPartnerId(searchRequest, token);
    return new ResponseEntity<>(response, response.getResponseCode() != null ? response.getResponseCode() : HttpStatus.OK);
  }

  @PostMapping("/v1/karmapoints/deductionrule")
  public ResponseEntity<SBApiResponse> karmapointsDeductionRule(@RequestBody JsonNode userCourseEnroll, @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
    SBApiResponse response = enrollmentService.karmapointsDeductionRule(userCourseEnroll, token);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @GetMapping("/v2/readby/useridcourseid/{courseid}")
  public ResponseEntity<SBApiResponse> readByUserIdAndCourseIdV2(@PathVariable String courseid, @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
    SBApiResponse response = enrollmentService.readByUserIdAndCourseIdV2(courseid,token);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

}
