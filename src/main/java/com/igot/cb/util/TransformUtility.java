package com.igot.cb.util;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.igot.cb.util.dto.SBApiResponse;
import com.igot.cb.util.dto.SunbirdApiRespParam;
import com.igot.cb.util.exceptions.CustomException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Component
@Slf4j
public class TransformUtility {

    @Autowired
    private CbServerProperties cbServerProperties;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private ObjectMapper mapper;

    public JsonNode callCiosReadAPi(String extCourseId, String partnerId) {
        log.info("KafkaConsumer :: callCiosReadAPi");
        String url = cbServerProperties.getBaseUrl() + cbServerProperties.getCiosReadApiUrl() + extCourseId + "/" + partnerId;
        HttpHeaders headers = new HttpHeaders();
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<Object> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                entity,
                Object.class
        );
        if (response.getStatusCode().is2xxSuccessful()) {
            log.info("got successful response from cios read api by extCourseId and partnerId");
            return mapper.valueToTree(response.getBody());
        } else {
            throw new RuntimeException("Failed to retrieve externalId. Status code: " + response.getStatusCodeValue());
        }

    }

    public JsonNode callCiosReadAPiByContentId(String contentId) {
        log.info("KafkaConsumer :: callCiosReadAPi");
        String url = cbServerProperties.getBaseUrl() + cbServerProperties.getCiosReadApiByContentId() + contentId;
        HttpHeaders headers = new HttpHeaders();
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<Object> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                entity,
                Object.class
        );
        if (response.getStatusCode().is2xxSuccessful()) {
            log.info("got successful response from cios read api by extCourseId and partnerId");
            return mapper.valueToTree(response.getBody());
        } else {
            throw new CustomException(Constants.ERROR,"Failed to retrieve data by contentId " + contentId,HttpStatus.BAD_REQUEST);
        }

    }

    public JsonNode callContentPartnerReadApi(String partnerId) {
        log.info("KafkaConsumer :: callExtApi");
        String url = cbServerProperties.getBaseUrl() + cbServerProperties.getContentPartnerReadApiUrl() + partnerId;
        HttpHeaders headers = new HttpHeaders();
        headers.set("Accept", "application/json"); // Indicate JSON response
        headers.set("Content-Type", "application/json");
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<JsonNode> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                entity,
                JsonNode.class
        );
        if (response.getStatusCode().is2xxSuccessful()) {
            JsonNode jsonNode = response.getBody();
            return jsonNode.path("result");
        } else {
            throw new CustomException(Constants.ERROR,"Failed to retrieve externalId. Status code: " + response.getStatusCode(),HttpStatus.BAD_REQUEST);
        }
    }

    public JsonNode callContentPartnerReadByPartnerCodeApi(String partnerCode) {
        log.info("KafkaConsumer :: callContentPartnerReadByPartnerCodeApi");
        String url = cbServerProperties.getBaseUrl() + cbServerProperties.getContentPartnerReadbyPartnerCodeApiUrl() + partnerCode;
        HttpHeaders headers = new HttpHeaders();
        headers.set("Accept", "application/json"); // Indicate JSON response
        headers.set("Content-Type", "application/json");
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<JsonNode> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                entity,
                JsonNode.class
        );
        if (response.getStatusCode().is2xxSuccessful()) {
            JsonNode jsonNode = response.getBody();
            return jsonNode.path("result");
        } else {
            throw new CustomException(Constants.ERROR,"Failed to retrieve externalId. Status code: " + response.getStatusCodeValue(),HttpStatus.BAD_REQUEST);
        }
    }

    public JsonNode transformData(Object jsonNode, List<Object> contentJson) {
        log.debug("TransformUtility::transformData");
        try {
            String inputJson = mapper.writeValueAsString(jsonNode);
            Chainr chainr = Chainr.fromSpec(contentJson);
            Object transformedOutput = chainr.transform(JsonUtils.jsonToObject(inputJson));
            return mapper.convertValue(transformedOutput, JsonNode.class);
        } catch (JsonProcessingException e) {
            log.error("Error transforming data", e);
            return null;
        }
    }

    public SBApiResponse createDefaultResponse(String api) {
        SBApiResponse response = new SBApiResponse();
        response.setId(api);
        response.setVer(Constants.API_VERSION_1);
        response.setParams(new SunbirdApiRespParam(UUID.randomUUID().toString()));
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
        response.setTs(LocalDateTime.now().toString());
        return response;
    }
}
