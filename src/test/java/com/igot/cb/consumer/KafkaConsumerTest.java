
package com.igot.cb.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.producer.Producer;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.util.CbServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.TransformUtility;

@ExtendWith(MockitoExtension.class)
public class KafkaConsumerTest {

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private Producer producer;

    @Mock
    private CbServerProperties cbServerProperties;

    @Mock
    private TransformUtility transformUtility;

    @Mock
    private ResourceLoader resourceLoader;

    @Mock
    private Resource resource;

    @InjectMocks
    private KafkaConsumer kafkaConsumer;

    private ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        lenient().when(cbServerProperties.getCertificateCharLength()).thenReturn(50);
    }

    @Test
    void testEnrollUpdateConsumer_Success() throws Exception {
        // Mock data
        String userId = "user123";
        String courseId = "course123";
        String partnerId = "partner123";
        String extCourseId = "extCourse123";
        String completedOn = "15/04/2025";

        // Mock getCertificateTopic to fix the test
        when(cbServerProperties.getCertificateTopic()).thenReturn("certificate-topic");

        // Create consumer record
        Map<String, Object> enrollMap = new HashMap<>();
        enrollMap.put(Constants.USER_ID, userId);
        enrollMap.put(Constants.COURSE_ID, courseId);
        enrollMap.put("partnerId", partnerId);
        enrollMap.put("courseid", extCourseId);
        enrollMap.put("completedon", completedOn);

        String enrollMapJson = objectMapper.writeValueAsString(enrollMap);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0, "key", enrollMapJson);

        // Mock CIOS API response
        ObjectNode contentNode = objectMapper.createObjectNode();
        contentNode.put("contentId", courseId);

        ObjectNode resultNode = objectMapper.createObjectNode();
        resultNode.set("content", contentNode);

        when(transformUtility.callCiosReadAPi(eq(extCourseId), eq(partnerId))).thenReturn(resultNode);

        // Mock DB query result
        List<Map<String, Object>> dbResult = new ArrayList<>();
        Map<String, Object> dbRow = new HashMap<>();
        dbRow.put(Constants.USER_ID, userId);
        dbRow.put(Constants.COURSE_ID, courseId);
        dbResult.add(dbRow);

        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                eq(Constants.KEYSPACE_SUNBIRD_COURSES),
                eq(Constants.TABLE_USER_EXTERNAL_ENROLMENTS),
                any(Map.class),
                isNull(),
                eq(1)
        )).thenReturn(dbResult);

        // Mock content partner API response
        ObjectNode contentPartnerNode = objectMapper.createObjectNode();
        contentPartnerNode.put("contentPartnerName", "Test Partner");
        contentPartnerNode.put("id", partnerId);
        contentNode.set("contentPartner", contentPartnerNode);
        contentNode.put("name", "Test Course");
        contentNode.put("appIcon", "https://test.com/icon.png");

        // Mock certificate template
        ObjectNode partnerApiResponse = objectMapper.createObjectNode();
        partnerApiResponse.put("certificateTemplateUrl", "https://test.com/template.svg");
        when(transformUtility.callContentPartnerReadApi(eq(partnerId))).thenReturn(partnerApiResponse);

        // Mock certificate template resource
        String certificateTemplateJson = "{\"template\": \"${svgTemplate}\", \"recipient\": \"${user.name}\"}";
        InputStream inputStream = new ByteArrayInputStream(certificateTemplateJson.getBytes());
        when(resourceLoader.getResource(eq("classpath:certificateTemplate.json"))).thenReturn(resource);
        when(resource.getInputStream()).thenReturn(inputStream);

        // Mock user name lookup
        List<Map<String, Object>> userResult = new ArrayList<>();
        Map<String, Object> userRow = new HashMap<>();
        userRow.put("firstname", "John");
        userRow.put("lastname", "Doe");
        userResult.add(userRow);

        when(cassandraOperation.getRecordsByProperties(
                eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_USER),
                any(Map.class),
                anyList()
        )).thenReturn(userResult);

        // Execute
        kafkaConsumer.enrollUpdateConsumer(record);

        // Verify DB update
        ArgumentCaptor<Map<String, Object>> updateCaptor = ArgumentCaptor.forClass(Map.class);
        verify(cassandraOperation).updateRecord(
                eq(Constants.KEYSPACE_SUNBIRD_COURSES),
                eq(Constants.TABLE_USER_EXTERNAL_ENROLMENTS),
                updateCaptor.capture(),
                any(Map.class)
        );

        Map<String, Object> updateValues = updateCaptor.getValue();
        assertEquals(100, updateValues.get(Constants.PROGRESS));
        assertEquals(2, updateValues.get(Constants.STATUS));
        assertEquals(100, updateValues.get(Constants.COMPLETION_PERCENTAGE));
        assertNotNull(updateValues.get(Constants.COMPLETED_ON));
        assertNotNull(updateValues.get(Constants.UPDATED_ON));

        // Verify certificate generation
        verify(producer).push(eq("certificate-topic"), any(JsonNode.class));
    }

    @Test
    void testEnrollUpdateConsumer_NoUserInDB() throws Exception {
        // Mock data
        String userId = "user123";
        String courseId = "course123";
        String partnerId = "partner123";
        String extCourseId = "extCourse123";
        String completedOn = "15/04/2025";

        // Create consumer record
        Map<String, Object> enrollMap = new HashMap<>();
        enrollMap.put(Constants.USER_ID, userId);
        enrollMap.put(Constants.COURSE_ID, courseId);
        enrollMap.put("partnerId", partnerId);
        enrollMap.put("courseid", extCourseId);
        enrollMap.put("completedon", completedOn);

        String enrollMapJson = objectMapper.writeValueAsString(enrollMap);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0, "key", enrollMapJson);

        // Mock CIOS API response
        ObjectNode contentNode = objectMapper.createObjectNode();
        contentNode.put("contentId", courseId);

        ObjectNode resultNode = objectMapper.createObjectNode();
        resultNode.set("content", contentNode);

        when(transformUtility.callCiosReadAPi(eq(extCourseId), eq(partnerId))).thenReturn(resultNode);

        // Mock empty DB result (user not found)
        List<Map<String, Object>> emptyResult = new ArrayList<>();
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                eq(Constants.KEYSPACE_SUNBIRD_COURSES),
                eq(Constants.TABLE_USER_EXTERNAL_ENROLMENTS),
                any(Map.class),
                isNull(),
                eq(1)
        )).thenReturn(emptyResult);

        // Execute
        kafkaConsumer.enrollUpdateConsumer(record);

        // Verify no DB update or certificate generation happens
        verify(cassandraOperation, never()).updateRecord(
                eq(Constants.KEYSPACE_SUNBIRD_COURSES),
                eq(Constants.TABLE_USER_EXTERNAL_ENROLMENTS),
                any(Map.class),
                any(Map.class)
        );

        verify(producer, never()).push(any(String.class), any(JsonNode.class));
    }

    @Test
    void testReceiveProgressUpdateFromPartner_Success() throws Exception {
        // Create consumer record with partner code
        ObjectNode inputJson = objectMapper.createObjectNode();
        inputJson.put("partnerCode", "PARTNER001");

        String inputJsonString = objectMapper.writeValueAsString(inputJson);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0, "key", inputJsonString);

        // Mock partner API response
        ObjectNode partnerResponse = objectMapper.createObjectNode();
        partnerResponse.put("id", "partner123");

        List<Map<String, Object>> transformJson = new ArrayList<>();
        Map<String, Object> transformItem = new HashMap<>();
        transformItem.put("field", "value");
        transformJson.add(transformItem);

        partnerResponse.set(Constants.TRANSFORM_PROGRESS_JSON, objectMapper.valueToTree(transformJson));

        when(transformUtility.callContentPartnerReadByPartnerCodeApi(eq("PARTNER001"))).thenReturn(partnerResponse);

        // Mock transform result
        ObjectNode transformedData = objectMapper.createObjectNode();
        transformedData.put("transformed", "true");

        when(transformUtility.transformData(eq(inputJson), eq((List<Object>) (List<?>) transformJson))).thenReturn(transformedData);

        when(cbServerProperties.getUserProgressUpdateTopic()).thenReturn("progress-update-topic");

        // Execute
        kafkaConsumer.receiveProgressUpdateFromPartner(record);

        // Verify producer is called with transformed data
        ArgumentCaptor<JsonNode> nodeCaptor = ArgumentCaptor.forClass(JsonNode.class);
        verify(producer).push(eq("progress-update-topic"), nodeCaptor.capture());

        JsonNode capturedNode = nodeCaptor.getValue();
        assertEquals("true", capturedNode.get("transformed").asText());
        assertEquals("partner123", capturedNode.get(Constants.PARTNER_ID).asText());
    }

    @Test
    void testReceiveProgressUpdateFromPartner_MissingTransformJson() throws Exception {
        // Create consumer record with partner code
        ObjectNode inputJson = objectMapper.createObjectNode();
        inputJson.put("partnerCode", "PARTNER001");

        String inputJsonString = objectMapper.writeValueAsString(inputJson);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0, "key", inputJsonString);

        // Mock partner API response without transform JSON
        ObjectNode partnerResponse = objectMapper.createObjectNode();
        partnerResponse.put("id", "partner123");
        // No TRANSFORM_PROGRESS_JSON field

        when(transformUtility.callContentPartnerReadByPartnerCodeApi(eq("PARTNER001"))).thenReturn(partnerResponse);

        // Execute
        kafkaConsumer.receiveProgressUpdateFromPartner(record);

        // Verify no producer call
        verify(producer, never()).push(any(String.class), any(JsonNode.class));
    }

    @Test
    void testConvertToTimestamp() throws Exception {
        // Test the private method using reflection
        java.lang.reflect.Method method = KafkaConsumer.class.getDeclaredMethod("convertToTimestamp", String.class);
        method.setAccessible(true);

        Instant result = (Instant) method.invoke(kafkaConsumer, "15/04/2025");

        assertNotNull(result);
    }

    @Test
    void testConvertDateFormat() throws Exception {
        // Test the private method using reflection
        java.lang.reflect.Method method = KafkaConsumer.class.getDeclaredMethod("convertDateFormat", String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, "15/04/2025");

        assertEquals("2025-04-15", result);
    }
}
