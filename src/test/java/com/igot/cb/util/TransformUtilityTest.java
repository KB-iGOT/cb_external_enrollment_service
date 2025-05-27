package com.igot.cb.util;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.util.dto.SBApiResponse;
import com.igot.cb.util.exceptions.CustomException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TransformUtilityTest {

    @InjectMocks
    private TransformUtility transformUtility;

    @Mock
    private CbServerProperties cbServerProperties;

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private ObjectMapper mapper;

    private ObjectMapper realMapper = new ObjectMapper();
    private static final String DUMMY_PARTNER_ID = "partnerId";
    private static final String DUMMY_PARTNER_CODE = "partnerCode";

    @BeforeEach
    void setUp() {
        // Use real ObjectMapper for certain tests
        lenient().when(mapper.valueToTree(any())).thenAnswer(invocation -> realMapper.valueToTree(invocation.getArgument(0)));
        lenient().when(mapper.convertValue(any(), eq(JsonNode.class))).thenAnswer(invocation -> realMapper.convertValue(invocation.getArgument(0), JsonNode.class));
    }

    @Test
    void callCiosReadAPi_Success() {
        // Arrange
        String extCourseId = "course123";
        String partnerId = "partner123";
        String baseUrl = "http://example.com";
        String apiUrl = "/api/cios/read/";
        String fullUrl = baseUrl + apiUrl + extCourseId + "/" + partnerId;

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNodeBody = objectMapper.createObjectNode(); // or use actual structure

        ResponseEntity<Object> responseEntity = new ResponseEntity<>(jsonNodeBody, HttpStatus.OK);

        when(cbServerProperties.getBaseUrl()).thenReturn(baseUrl);
        when(cbServerProperties.getCiosReadApiUrl()).thenReturn(apiUrl);
        when(restTemplate.exchange(
                eq(fullUrl),
                eq(HttpMethod.GET),
                any(HttpEntity.class),
                eq(Object.class))
        ).thenReturn(responseEntity);

        // Act
        JsonNode result = transformUtility.callCiosReadAPi(extCourseId, partnerId);

        // Assert
        assertNotNull(result);
        verify(restTemplate).exchange(eq(fullUrl), eq(HttpMethod.GET), any(HttpEntity.class), eq(Object.class));
    }

    @Test
    void callCiosReadAPi_Error() {
        // Arrange
        String extCourseId = "course123";
        String partnerId = "partner123";
        String baseUrl = "http://example.com";
        String apiUrl = "/api/cios/read/";
        String fullUrl = baseUrl + apiUrl + extCourseId + "/" + partnerId;
        
        when(cbServerProperties.getBaseUrl()).thenReturn(baseUrl);
        when(cbServerProperties.getCiosReadApiUrl()).thenReturn(apiUrl);
        
        ResponseEntity<Object> responseEntity = new ResponseEntity<>(null, HttpStatus.BAD_REQUEST);
        when(restTemplate.exchange(eq(fullUrl), eq(HttpMethod.GET), any(HttpEntity.class), eq(Object.class)))
                .thenReturn(responseEntity);

        // Act & Assert
        assertThrows(RuntimeException.class, () -> {
            transformUtility.callCiosReadAPi(extCourseId, partnerId);
        });
    }

    @Test
    void callContentPartnerReadApi_Success() {
        // Arrange
        String partnerId = "partner123";
        String baseUrl = "http://example.com";
        String apiUrl = "/api/partner/read/";
        String fullUrl = baseUrl + apiUrl + partnerId;
        
        ObjectNode resultNode = realMapper.createObjectNode();
        resultNode.put("id", "partner123");
        
        ObjectNode responseNode = realMapper.createObjectNode();
        responseNode.set("result", resultNode);
        
        when(cbServerProperties.getBaseUrl()).thenReturn(baseUrl);
        when(cbServerProperties.getContentPartnerReadApiUrl()).thenReturn(apiUrl);
        
        ResponseEntity<JsonNode> responseEntity = new ResponseEntity<>(responseNode, HttpStatus.OK);
        when(restTemplate.exchange(eq(fullUrl), eq(HttpMethod.GET), any(HttpEntity.class), eq(JsonNode.class)))
                .thenReturn(responseEntity);

        // Act
        JsonNode result = transformUtility.callContentPartnerReadApi(partnerId);

        // Assert
        assertNotNull(result);
        assertEquals("partner123", result.get("id").asText());
        verify(restTemplate).exchange(eq(fullUrl), eq(HttpMethod.GET), any(HttpEntity.class), eq(JsonNode.class));
    }

    @Test
    void callContentPartnerReadApi_Error() {
        // Arrange
        String partnerId = "partner123";
        String baseUrl = "http://example.com";
        String apiUrl = "/api/partner/read/";
        String fullUrl = baseUrl + apiUrl + partnerId;
        
        when(cbServerProperties.getBaseUrl()).thenReturn(baseUrl);
        when(cbServerProperties.getContentPartnerReadApiUrl()).thenReturn(apiUrl);
        
        ResponseEntity<JsonNode> responseEntity = new ResponseEntity<>(null, HttpStatus.BAD_REQUEST);
        when(restTemplate.exchange(eq(fullUrl), eq(HttpMethod.GET), any(HttpEntity.class), eq(JsonNode.class)))
                .thenReturn(responseEntity);

        // Act & Assert
        CustomException exception = assertThrows(CustomException.class, () -> {
            transformUtility.callContentPartnerReadApi(partnerId);
        });
        
        assertEquals(Constants.ERROR, exception.getCode());
        assertTrue(exception.getMessage().contains("Failed to retrieve externalId"));
    }

    @Test
    void callContentPartnerReadByPartnerCodeApi_Success() {
        // Arrange
        String partnerCode = "code123";
        String baseUrl = "http://example.com";
        String apiUrl = "/api/partner/code/";
        String fullUrl = baseUrl + apiUrl + partnerCode;
        
        ObjectNode resultNode = realMapper.createObjectNode();
        resultNode.put("id", "partner123");
        
        ObjectNode responseNode = realMapper.createObjectNode();
        responseNode.set("result", resultNode);
        
        when(cbServerProperties.getBaseUrl()).thenReturn(baseUrl);
        when(cbServerProperties.getContentPartnerReadbyPartnerCodeApiUrl()).thenReturn(apiUrl);
        
        ResponseEntity<JsonNode> responseEntity = new ResponseEntity<>(responseNode, HttpStatus.OK);
        when(restTemplate.exchange(eq(fullUrl), eq(HttpMethod.GET), any(HttpEntity.class), eq(JsonNode.class)))
                .thenReturn(responseEntity);

        // Act
        JsonNode result = transformUtility.callContentPartnerReadByPartnerCodeApi(partnerCode);

        // Assert
        assertNotNull(result);
        assertEquals("partner123", result.get("id").asText());
        verify(restTemplate).exchange(eq(fullUrl), eq(HttpMethod.GET), any(HttpEntity.class), eq(JsonNode.class));
    }

    @Test
    void callContentPartnerReadByPartnerCodeApi_Error() {
        // Arrange
        String partnerCode = "code123";
        String baseUrl = "http://example.com";
        String apiUrl = "/api/partner/code/";
        String fullUrl = baseUrl + apiUrl + partnerCode;
        
        when(cbServerProperties.getBaseUrl()).thenReturn(baseUrl);
        when(cbServerProperties.getContentPartnerReadbyPartnerCodeApiUrl()).thenReturn(apiUrl);
        
        ResponseEntity<JsonNode> responseEntity = new ResponseEntity<>(null, HttpStatus.BAD_REQUEST);
        when(restTemplate.exchange(eq(fullUrl), eq(HttpMethod.GET), any(HttpEntity.class), eq(JsonNode.class)))
                .thenReturn(responseEntity);

        // Act & Assert
        CustomException exception = assertThrows(CustomException.class, () -> {
            transformUtility.callContentPartnerReadByPartnerCodeApi(partnerCode);
        });
        
        assertEquals(Constants.ERROR, exception.getCode());
        assertTrue(exception.getMessage().contains("Failed to retrieve externalId"));
    }

    @Test
    void transformData_Success() throws Exception {
        // Arrange
        JsonNode jsonNode = realMapper.createObjectNode();
        ((ObjectNode) jsonNode).put("field", "value");
        
        List<Object> contentJson = new ArrayList<>();
        Map<String, Object> spec = new HashMap<>();
        spec.put("operation", "shift");
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("field", "transformedField");
        spec.put("spec", mapping);
        contentJson.add(spec);
        
        JsonNode expectedOutput = realMapper.createObjectNode();
        ((ObjectNode) expectedOutput).put("transformedField", "value");
        
        Chainr mockChainr = mock(Chainr.class);
        
        try (MockedStatic<Chainr> chainrMockedStatic = Mockito.mockStatic(Chainr.class);
             MockedStatic<JsonUtils> jsonUtilsMockedStatic = Mockito.mockStatic(JsonUtils.class)) {
            
            chainrMockedStatic.when(() -> Chainr.fromSpec(any())).thenReturn(mockChainr);
            
            Map<String, Object> jsonObject = new HashMap<>();
            jsonObject.put("field", "value");
            jsonUtilsMockedStatic.when(() -> JsonUtils.jsonToObject(anyString())).thenReturn(jsonObject);
            
            Map<String, Object> transformedObject = new HashMap<>();
            transformedObject.put("transformedField", "value");
            when(mockChainr.transform(any())).thenReturn(transformedObject);
            
            when(mapper.writeValueAsString(any())).thenReturn("{\"field\":\"value\"}");
            when(mapper.convertValue(any(), eq(JsonNode.class))).thenReturn(expectedOutput);
            
            // Act
            JsonNode result = transformUtility.transformData(jsonNode, contentJson);
            
            // Assert
            assertNotNull(result);
            assertEquals("value", result.get("transformedField").asText());
        }
    }

    @Test
    void transformData_Exception() throws Exception {
        // Arrange
        JsonNode jsonNode = realMapper.createObjectNode();
        List<Object> contentJson = new ArrayList<>();
        
        when(mapper.writeValueAsString(any())).thenThrow(new RuntimeException("Test exception"));
        
        // Act & Assert - expect a RuntimeException with the message "Test exception"
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            transformUtility.transformData(jsonNode, contentJson);
        });
        assertEquals("Test exception", exception.getMessage());
    }

    @Test
    void createDefaultResponse() {
        // Arrange
        String api = "test-api";
        
        // Act
        SBApiResponse response = transformUtility.createDefaultResponse(api);
        
        // Assert
        assertNotNull(response);
        assertEquals(api, response.getId());
        assertEquals(Constants.API_VERSION_1, response.getVer());
        assertNotNull(response.getParams());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertNotNull(response.getTs());
    }

    @Test
    void testTransformData_JsonProcessingException() throws Exception {
        Object input = new Object(); // any dummy object
        when(mapper.writeValueAsString(input)).thenThrow(new JsonProcessingException("Test exception") {});

        JsonNode result = transformUtility.transformData(input, Collections.emptyList());

        assertNull(result);
        verify(mapper, times(1)).writeValueAsString(input);
    }

    @Test
    void callCiosReadAPi_ShouldThrowCustomException_WhenResponseBodyIsNull() {
        // Arrange
        String extCourseId = "course123";
        String partnerId = "partner123";
        String baseUrl = "http://example.com";
        String apiPath = "/api/cios/read/";
        String fullUrl = baseUrl + apiPath + extCourseId + "/" + partnerId;

        when(cbServerProperties.getBaseUrl()).thenReturn(baseUrl);
        when(cbServerProperties.getCiosReadApiUrl()).thenReturn(apiPath);

        ResponseEntity<Object> mockResponse = new ResponseEntity<>(null, HttpStatus.OK);
        when(restTemplate.exchange(
                eq(fullUrl),
                eq(HttpMethod.GET),
                any(HttpEntity.class),
                eq(Object.class))
        ).thenReturn(mockResponse);

        // Act & Assert
        CustomException thrown = assertThrows(CustomException.class, () ->
                transformUtility.callCiosReadAPi(extCourseId, partnerId)
        );

        assertEquals("Received null response body from CIOS read API", thrown.getMessage());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, thrown.getHttpStatusCode());
        assertEquals(Constants.ERROR, thrown.getCode());
    }

    @Test
    void testCallContentPartnerReadApi_whenResponseBodyIsNull_thenThrowCustomException() {
        ResponseEntity<JsonNode> mockResponse = new ResponseEntity<>(null, HttpStatus.OK);

        when(restTemplate.exchange(
                anyString(),
                eq(HttpMethod.GET),
                any(HttpEntity.class),
                eq(JsonNode.class))
        ).thenReturn(mockResponse);

        CustomException exception = assertThrows(CustomException.class,
                () -> transformUtility.callContentPartnerReadApi(DUMMY_PARTNER_ID));

        assertEquals("Invalid response body from CIOS read API", exception.getMessage());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, exception.getHttpStatusCode());
    }

    @Test
    void testCallContentPartnerReadByPartnerCodeApi_whenResultMissing_thenThrowCustomException() {
        JsonNode incompleteBody = mapper.createObjectNode(); // no "result" key
        ResponseEntity<JsonNode> mockResponse = new ResponseEntity<>(incompleteBody, HttpStatus.OK);

        when(restTemplate.exchange(
                anyString(),
                eq(HttpMethod.GET),
                any(HttpEntity.class),
                eq(JsonNode.class))
        ).thenReturn(mockResponse);

        CustomException exception = assertThrows(CustomException.class,
                () -> transformUtility.callContentPartnerReadByPartnerCodeApi(DUMMY_PARTNER_CODE));

        assertEquals("Invalid or null response body", exception.getMessage());
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, exception.getHttpStatusCode());
    }

}