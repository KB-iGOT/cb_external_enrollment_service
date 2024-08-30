package com.igot.cb.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.enrollment.entity.ContentPartnerEntity;
import com.igot.cb.enrollment.service.EnrollmentService;
import com.igot.cb.producer.Producer;
import com.igot.cb.util.CbServerProperties;
import com.igot.cb.util.cache.CacheService;
import com.igot.cb.util.Constants;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import java.io.InputStream;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.WordUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
@Slf4j
public class KafkaConsumer {
    private ObjectMapper mapper = new ObjectMapper();

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    private CassandraOperation cassandraOperation;

    @Autowired
    CacheService cacheService;

    @Autowired
    private Producer producer;

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private CbServerProperties cbServerProperties;

    @Autowired
    private EnrollmentService enrollmentService;

    @KafkaListener(topics = "${spring.kafka.cornell.topic.name}", groupId = "${spring.kafka.consumer.group.id}")
    public void enrollUpdateConsumer(ConsumerRecord<String, String> data) {
        log.info("KafkaConsumer::enrollUpdateConsumer:topic name: {} and recievedData: {}", data.topic(), data.value());
        try {
            TimeZone timeZone = TimeZone.getTimeZone("Asia/Kolkata");
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            timestamp.setTime(timestamp.getTime() + timeZone.getOffset(timestamp.getTime()));
            Map<String, Object> userCourseEnrollMap = mapper.readValue(data.value(), HashMap.class);
            if (userCourseEnrollMap.containsKey(Constants.USER_ID) && userCourseEnrollMap.get(Constants.USER_ID) instanceof String && userCourseEnrollMap.containsKey(Constants.COURSE_ID) && userCourseEnrollMap.get(Constants.COURSE_ID) instanceof String) {
                String courseId = null;
                String contentPartnerName = null;
                String courseName = null;
                String coursePosterImage = null;
//                String providerName = userCourseEnrollMap.get("providerName").toString();
                //ContentPartnerEntity entity=enrollmentService.getContentDetailsByPartnerName(providerName);
                String extCourseId = userCourseEnrollMap.get("courseid").toString();
//                JsonNode result = callExtApi(extCourseId,entity.getId());
                JsonNode result = callExtApi(extCourseId);
                JsonNode contentNode = result.path("content");
                if (!contentNode.isMissingNode()) {
                    courseId = contentNode.path("contentId").asText(null);
                    courseName = contentNode.path("name").asText(null);
                    JsonNode contentPartnerNode = contentNode.path("contentPartner");
                    if (!contentPartnerNode.isMissingNode()) {
                        contentPartnerName = contentPartnerNode.path("contentPartnerName").asText(null);
                        coursePosterImage = contentPartnerNode.path("thumbnailUrl").asText(null);
                    }
                }
                log.info("KafkaConsumer :: enrollUpdateConsumer ::courseId from cios api {} userid {}", courseId, userCourseEnrollMap.get(Constants.USER_ID));
                String[] parts = ((String) userCourseEnrollMap.get(Constants.USER_ID)).split("@");
                userCourseEnrollMap.put(Constants.USER_ID, parts[0]);
                Map<String, Object> propertyMap = new HashMap<>();
                propertyMap.put(Constants.USER_ID, userCourseEnrollMap.get(Constants.USER_ID));
                propertyMap.put(Constants.COURSE_ID, courseId);
                List<Map<String, Object>> listOfMasterData = cassandraOperation.getRecordsByPropertiesWithoutFiltering(Constants.KEYSPACE_SUNBIRD_COURSES, Constants.TABLE_USER_EXTERNAL_ENROLMENTS, propertyMap, null, 1);
                if (!CollectionUtils.isEmpty(listOfMasterData)) {
                    String Status = userCourseEnrollMap.get("status").toString();
                    log.info("status {}", Status);
                    if (Status.equalsIgnoreCase("complete")) {
                        Map<String, Object> updatedMap = new HashMap<>();
                        updatedMap.put(Constants.PROGRESS, 100);
                        updatedMap.put(Constants.STATUS, 2);
                        updatedMap.put(Constants.COMPLETED_ON, convertToTimestamp((String) userCourseEnrollMap.get("completedon")));
                        updatedMap.put(Constants.COMPLETION_PERCENTAGE, 100);
                        updatedMap.put(Constants.UPDATED_ON, timestamp);
                        cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD_COURSES, Constants.TABLE_USER_EXTERNAL_ENROLMENTS, updatedMap, propertyMap);
                        Resource resource = resourceLoader.getResource("classpath:certificateTemplate.json");
                        InputStream inputStream = resource.getInputStream();
                        JsonNode jsonNode = mapper.readTree(inputStream);
                        Map<String, Object> certificateRequest = new HashMap<>();
                        certificateRequest.put(Constants.USER_ID, userCourseEnrollMap.get(Constants.USER_ID));
                        certificateRequest.put(Constants.COURSE_ID, courseId);
                        certificateRequest.put(Constants.COMPLETION_DATE, userCourseEnrollMap.get("completedon"));
                        certificateRequest.put(Constants.PROVIDER_NAME,contentPartnerName );
                        certificateRequest.put(Constants.COURSE_NAME, courseName);
                        certificateRequest.put(Constants.COURSE_POSTER_IMAGE, coursePosterImage);
                        certificateRequest.put(Constants.RECIPIENT_NAME, readUserName(userCourseEnrollMap.get(Constants.USER_ID).toString()));
                        replacePlaceholders(jsonNode, certificateRequest);
                        producer.push(cbServerProperties.getCertificateTopic(), jsonNode);
                        inputStream.close();
                        log.info("KafkaConsumer::enrollUpdateConsumer:updated");
                    } else {
                        Map<String, Object> updatedMap = new HashMap<>();
                        updatedMap.put(Constants.PROGRESS, userCourseEnrollMap.get("progress_percetage"));
                        updatedMap.put(Constants.STATUS, 0);
                        updatedMap.put(Constants.COMPLETION_PERCENTAGE, userCourseEnrollMap.get("progress_percetage"));
                        updatedMap.put(Constants.UPDATED_ON, timestamp);
                        cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD_COURSES, Constants.TABLE_USER_EXTERNAL_ENROLMENTS, updatedMap, propertyMap);
                    }
                } else {
                    log.error("Data not present in DB");
                    //add not enrolled data to file
                }
            } else {
                log.error("Unable to get userid and courseid from kafka consumer");
            }

        } catch (Exception e) {
            log.error("Failed to read enroll Request. Message received : " + data.value(), e);
        }
    }

    private String readUserName(String userid) {
        List<String> fields = Arrays.asList("firstname", "lastname"); // Assuming user_id is the column name in your table
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put("id", userid);
        List<Map<String, Object>> userEnrollmentList = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_USER,
                propertyMap,
                fields
        );
        String firstname = (String) userEnrollmentList.stream().findFirst().get().get("firstname");
        String lastname = (String) userEnrollmentList.stream().findFirst().get().get("lastname");
        String fullname = firstname;
        if (lastname != null) {
            fullname = fullname + " " + lastname;
        }
        return fullname;
    }

    private JsonNode callExtApi(String extCourseId) {
        log.info("KafkaConsumer :: callExtApi");
        String url = cbServerProperties.getBaseUrl() + cbServerProperties.getFixedUrl() + extCourseId;
        HttpHeaders headers = new HttpHeaders();
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<Object> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                entity,
                Object.class
        );
        if (response.getStatusCode().is2xxSuccessful()) {
            JsonNode jsonNode = mapper.valueToTree(response.getBody());
            return jsonNode;
        } else {
            throw new RuntimeException("Failed to retrieve externalId. Status code: " + response.getStatusCodeValue());
        }

    }

    public static Timestamp convertToTimestamp(String dateString) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            Date parsedDate = dateFormat.parse(dateString);
            return new Timestamp(parsedDate.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void replacePlaceholders(JsonNode jsonNode, Map<String, Object> certificateRequest) {
        log.debug("KafkaConsumer :: replacePlaceholders");
        if (jsonNode.isObject()) {
            ObjectNode objectNode = (ObjectNode) jsonNode;
            objectNode.fields().forEachRemaining(entry -> {
                JsonNode value = entry.getValue();
                if (value.isTextual()) {
                    String textValue = value.asText();
                    if (textValue.startsWith("${") && textValue.endsWith("}")) {
                        String placeholder = textValue.substring(2, textValue.length() - 1);
                        String replacement = getReplacementValue(placeholder, certificateRequest);
                        objectNode.put(entry.getKey(), replacement);
                    }
                } else if (value.isArray()) {
                    value.elements().forEachRemaining(element -> {
                        if (element.isObject()) {
                            replacePlaceholders(element, certificateRequest);
                        }
                    });
                } else {
                    replacePlaceholders(value, certificateRequest);
                }
            });
        }
    }

    private String getReplacementValue(String placeholder, Map<String, Object> certificateRequest) {
        log.debug("KafkaConsumer :: getReplacementValue");
        String value = WordUtils.wrap((String) certificateRequest.get("courseName"), cbServerProperties.getCertificateCharLength(), "\n", false);
        switch (placeholder) {
            case "user.id":
                return (String) certificateRequest.get("userid");
            case "course.id":
                return (String) certificateRequest.get("courseid");
            case "today.date":
                return convertDateFormat((String) certificateRequest.get("completiondate"));
            case "time.ms":
                return String.valueOf(System.currentTimeMillis());
            case "unique.id":
                return UUID.randomUUID().toString();
            case "course.name":
                int firstNewLineIndex = value.indexOf("\n");
                if (firstNewLineIndex != -1) {
                    return value.substring(0, firstNewLineIndex).trim();
                } else {
                    return value;
                }
            case "course.name.extended":
                int firstNewLineIndexExtended = value.indexOf("\n");
                if (firstNewLineIndexExtended != -1) {
                    String textAfterFirstNewLine = value.substring(firstNewLineIndexExtended + 1).trim();
                    String secondValue = WordUtils.wrap(textAfterFirstNewLine, cbServerProperties.getCertificateCharLength(), "\n", false);
                    int secondNewLineIndex = secondValue.indexOf("\n");
                    if (secondNewLineIndex != -1) {
                        return secondValue.substring(0, secondNewLineIndex).trim();
                    } else {
                        return secondValue;
                    }
                } else {
                    return "";
                }
            case "provider.name":
                return (String) certificateRequest.get("providerName");
            case "user.name":
                return (String) certificateRequest.get("recipientName");
            case "course.poster.image":
                return (String) certificateRequest.get("coursePosterImage");
            case "svgTemplate":
                return cbServerProperties.getSvgTemplate();
            default:
                return "";
        }
    }

    private static String convertDateFormat(String originalDate) {
        DateTimeFormatter originalFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        LocalDate date = LocalDate.parse(originalDate, originalFormatter);
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return date.format(outputFormatter);
    }

}
