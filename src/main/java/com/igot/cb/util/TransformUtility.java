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
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
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
        log.info("KafkaConsumer :: callExtApi");
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
            return mapper.valueToTree(response.getBody());
        } else {
            throw new RuntimeException("Failed to retrieve externalId. Status code: " + response.getStatusCodeValue());
        }

    }

    public JsonNode callContentPartnerReadApi(String partnerId) {
        log.info("KafkaConsumer :: callExtApi");
        String url = cbServerProperties.getBaseUrl() + cbServerProperties.getContentPartnerReadApiUrl() + partnerId;
        HttpHeaders headers = new HttpHeaders();
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
        response.setTs(DateTime.now().toString());
        return response;
    }
}
