package com.igot.cb.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.producer.Producer;
import com.igot.cb.util.CbServerProperties;
import com.igot.cb.util.TransformUtility;
import com.igot.cb.util.Constants;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.transactional.cassandrautils.CounterIncrement;

import java.io.InputStream;


import com.igot.cb.util.cache.CacheService;
import com.igot.cb.util.exceptions.CustomException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

import org.springframework.core.io.*;


@Component
@Slf4j
public class KafkaConsumer {
    private ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private CassandraOperation cassandraOperation;

    @Autowired
    private Producer producer;

    @Autowired
    private CbServerProperties cbServerProperties;

    @Autowired
    TransformUtility transformUtility;

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private CacheService cacheService;

    @KafkaListener(topics = "${spring.kafka.enrolment.counter.update.topic.name}", groupId = "${spring.kafka.enrolment.counter.update.consumer.group.id}")
    public void enrolmentCounterUpdateConsumer(ConsumerRecord<String, String> data, Acknowledgment acknowledgment) {
        log.info("KafkaConsumer::enrolmentCounterUpdateConsumer:topic name: {} and recievedData: {}", data.topic(), data.value());
        Map<String, Object> event;
        try {
            event = mapper.readValue(data.value(), new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            log.error("Failed to parse enrolment counter update event. Message received: " + data.value(), e);
            sendToFailureTopic(data, e);
            acknowledgment.acknowledge();
            return;
        }

        // Dedup guard: reqId is minted once per event at publish time, so a Kafka redelivery of
        // this exact event still carries the same reqId. Skip re-applying it if that reqId's key
        // is still present from a prior successful run.
        String reqId = (String) event.get(Constants.REQ_ID);
        String dedupeKey = StringUtils.isNotBlank(reqId) ? Constants.ENROLMENT_COUNTER_DEDUPE_PREFIX + reqId : null;
        if (dedupeKey != null && StringUtils.isNotBlank(cacheService.getCache(dedupeKey, cbServerProperties.getRedisIndex()))) {
            log.info("Enrolment counter update event {} already processed, skipping", reqId);
            acknowledgment.acknowledge();
            return;
        }

        try {
            updateEnrolmentCounters(event);
        } catch (Exception e) {
            log.error("Failed to update enrolment counters. Message received: " + data.value(), e);
            // Do not acknowledge so the offset is not committed and Kafka redelivers this record for retry.
            return;
        }

        // Only claim the reqId once the Cassandra batch has actually succeeded - claiming it
        // earlier could mark a never-applied event as done if the write later failed.
        if (dedupeKey != null) {
            cacheService.putCache(dedupeKey, cbServerProperties.getRedisIndex(), Boolean.TRUE, Constants.ENROLMENT_COUNTER_DEDUPE_TTL_SECONDS);
        }
        acknowledgment.acknowledge();
    }

    // A payload that can never be parsed would just retry forever if left un-acked, so it's
    // preserved here (best-effort - a failure to publish this must never block the ack below)
    // instead of silently dropped, then acknowledged off the main topic.
    private void sendToFailureTopic(ConsumerRecord<String, String> data, Exception e) {
        try {
            Map<String, Object> failureEvent = new HashMap<>();
            failureEvent.put("originalTopic", data.topic());
            failureEvent.put("originalPartition", data.partition());
            failureEvent.put("originalOffset", data.offset());
            failureEvent.put("payload", data.value());
            failureEvent.put("error", e.getMessage());
            producer.push(cbServerProperties.getEnrolmentCounterUpdateFailureTopic(), failureEvent);
        } catch (Exception pushException) {
            log.error("Failed to publish enrolment counter update event to failure topic. Message received: " + data.value(), pushException);
        }
    }

    // All counter rows touched by one event are written as a single Cassandra counter batch
    // (see CassandraOperation#incrementCounters), so they are applied together - never TOTAL
    // committed while USER/COURSE fail, or vice versa. A batch failure throws, is never
    // acknowledged, and Kafka redelivers the record so the whole event is retried as a unit.
    public void updateEnrolmentCounters(Map<String, Object> event) {
        String partnerId = (String) event.get(Constants.PARTNER_ID_REQ);
        String userId = (String) event.get(Constants.USER_ID);
        String courseId = (String) event.get(Constants.COURSE_ID);
        String courseType = (String) event.get(Constants.COURSE_TYPE_COL);
        String licenseType = (String) event.get(Constants.LICENSE_TYPE);

        if (Constants.COURSE_TYPE_FREE.equalsIgnoreCase(courseType)) {
            cassandraOperation.incrementCounters(
                    Constants.KEYSPACE_SUNBIRD_COURSES,
                    Constants.TABLE_USER_EXTERNAL_ENROLMENTS_COUNTER,
                    List.of(new CounterIncrement(
                            counterKey(partnerId, Constants.SCOPE_TYPE_TOTAL_ENROLMENTS, partnerId, courseType),
                            Map.of(Constants.COUNTER_VALUE, 1L)))
            );
            return;
        }

        boolean countTowardTotal = Constants.LICENSE_TYPE_COURSE.equalsIgnoreCase(licenseType)
                || readCounterValue(partnerId, Constants.SCOPE_TYPE_USER_ENROLMENTS, userId, courseType) == 0;

        List<CounterIncrement> increments = new ArrayList<>();
        increments.add(new CounterIncrement(
                counterKey(partnerId, Constants.SCOPE_TYPE_USER_ENROLMENTS, userId, courseType),
                Map.of(Constants.COUNTER_VALUE, 1L)));
        increments.add(new CounterIncrement(
                counterKey(partnerId, Constants.SCOPE_TYPE_COURSE_ENROLMENTS, courseId, courseType),
                Map.of(Constants.COUNTER_VALUE, 1L)));
        if (countTowardTotal) {
            increments.add(new CounterIncrement(
                    counterKey(partnerId, Constants.SCOPE_TYPE_TOTAL_ENROLMENTS, partnerId, courseType),
                    Map.of(Constants.COUNTER_VALUE, 1L)));
        }

        cassandraOperation.incrementCounters(
                Constants.KEYSPACE_SUNBIRD_COURSES, Constants.TABLE_USER_EXTERNAL_ENROLMENTS_COUNTER, increments);

        // The batch above is now durably applied - safe to refresh caches and sync downstream.
        invalidateCounterCache(partnerId, Constants.SCOPE_TYPE_USER_ENROLMENTS, userId, courseType);
        invalidateCounterCache(partnerId, Constants.SCOPE_TYPE_COURSE_ENROLMENTS, courseId, courseType);
        if (countTowardTotal) {
            invalidateCounterCache(partnerId, Constants.SCOPE_TYPE_TOTAL_ENROLMENTS, partnerId, courseType);
            if (Constants.COURSE_TYPE_PAID.equalsIgnoreCase(courseType)) {
                long licenseConsumedCount = readCounterValue(partnerId, Constants.SCOPE_TYPE_TOTAL_ENROLMENTS, partnerId, courseType);
                transformUtility.updateContentPartnerLicenseConsumedCount(partnerId, licenseConsumedCount);
            }
        }
    }

    // Keeps the read-through cache in EnrollmentServiceImpl#getCounterValue from serving a
    // stale count after this consumer increments the underlying Cassandra row.
    private void invalidateCounterCache(String partnerId, String scopeType, String scopeId, String courseType) {
        String cacheKey = Constants.ENROLMENT_COUNTER_CACHE_PREFIX + partnerId + "_" + scopeType + "_" + scopeId + "_" + courseType;
        cacheService.deleteCache(cacheKey, cbServerProperties.getRedisIndex());
    }

    private Map<String, Object> counterKey(String partnerId, String scopeType, String scopeId, String courseType) {
        Map<String, Object> key = new HashMap<>();
        key.put(Constants.PARTNER_ID_REQ, partnerId);
        key.put(Constants.SCOPE_TYPE, scopeType);
        key.put(Constants.SCOPE_ID, scopeId);
        key.put(Constants.COURSE_TYPE_COL, courseType);
        return key;
    }

    /**
     * Point-read of a counter row's "value" column right after incrementing it, so the
     * partner-record sync always reflects the authoritative post-increment total rather than
     * a locally-tracked running count that could drift under concurrent consumer instances.
     */
    private long readCounterValue(String partnerId, String scopeType, String scopeId, String courseType) {
        List<Map<String, Object>> rows = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD_COURSES,
                Constants.TABLE_USER_EXTERNAL_ENROLMENTS_COUNTER,
                counterKey(partnerId, scopeType, scopeId, courseType),
                List.of(Constants.COUNTER_VALUE),
                1
        );
        if (CollectionUtils.isEmpty(rows)) {
            return 0L;
        }
        Object value = rows.get(0).get(Constants.COUNTER_VALUE);
        return value == null ? 0L : ((Number) value).longValue();
    }

    @KafkaListener(topics = "${spring.kafka.cornell.topic.name}", groupId = "${spring.kafka.consumer.group.id}")
    public void enrollUpdateConsumer(ConsumerRecord<String, String> data, Acknowledgment acknowledgment) {
        log.info("KafkaConsumer::enrollUpdateConsumer:topic name: {} and recievedData: {}", data.topic(), data.value());
        try {
            ZoneId zoneId = ZoneId.of("UTC");
            Instant instant = LocalDateTime.now().atZone(zoneId).toInstant();
            Map<String, Object> userCourseEnrollMap = mapper.readValue(data.value(), HashMap.class);
            if (userCourseEnrollMap.containsKey(Constants.USER_ID) && userCourseEnrollMap.get(Constants.USER_ID) instanceof String && userCourseEnrollMap.containsKey(Constants.COURSE_ID) && userCourseEnrollMap.get(Constants.COURSE_ID) instanceof String) {
                String courseId = "";
                String partnerId = userCourseEnrollMap.get("partnerId").toString();
                String extCourseId = userCourseEnrollMap.get("courseid").toString();
                JsonNode result = transformUtility.callCiosReadAPi(extCourseId, partnerId);
                log.debug("got result from cios read api");
                JsonNode contentNode = result.path("content");
                if (!contentNode.isMissingNode() && !contentNode.isNull()) {
                    courseId = contentNode.get("contentId").asText();
                }
                log.debug("KafkaConsumer :: enrollUpdateConsumer ::courseId from cios api {} userid {}", courseId, userCourseEnrollMap.get(Constants.USER_ID));
                String[] parts = ((String) userCourseEnrollMap.get(Constants.USER_ID)).split("@");
                userCourseEnrollMap.put(Constants.USER_ID, parts[0]);
                Map<String, Object> propertyMap = new HashMap<>();
                String userId = ((String) userCourseEnrollMap.get(Constants.USER_ID));
                propertyMap.put(Constants.USER_ID, userId);
                propertyMap.put(Constants.COURSE_ID, courseId);
                List<Map<String, Object>> listOfMasterData = cassandraOperation.getRecordsByPropertiesWithoutFiltering(Constants.KEYSPACE_SUNBIRD_COURSES, Constants.TABLE_USER_EXTERNAL_ENROLMENTS, propertyMap, null, 1);
                if (!CollectionUtils.isEmpty(listOfMasterData)) {
                    Map<String, Object> enrolledData = listOfMasterData.get(0);
                    Object statusValue = enrolledData.get(Constants.STATUS);
                    boolean alreadyCompleted = statusValue instanceof Number && ((Number) statusValue).intValue() == 2;
                    if (alreadyCompleted) {
                        log.info("User {} has already completed the course {}. No update needed.", userId, courseId);
                        acknowledgment.acknowledge();
                    } else {
                        Map<String, Object> updatedMap = new HashMap<>();
                        updatedMap.put(Constants.PROGRESS, 100);
                        updatedMap.put(Constants.STATUS, 2);
                        updatedMap.put(Constants.COMPLETED_ON, convertToTimestamp((String) userCourseEnrollMap.get("completedon")));
                        updatedMap.put(Constants.COMPLETION_PERCENTAGE, 100);
                        updatedMap.put(Constants.UPDATED_ON, instant);
                        if (userCourseEnrollMap.get(Constants.ADDITIONAL_PROPERTIES) != null) {
                            updatedMap.put(Constants.ADDITIONAL_PROPERTIES, mapper.writeValueAsString(userCourseEnrollMap.get("additional_properties")));
                        } else {
                            updatedMap.put(Constants.ADDITIONAL_PROPERTIES, mapper.writeValueAsString(new HashMap<>()));
                        }
                        cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD_COURSES, Constants.TABLE_USER_EXTERNAL_ENROLMENTS, updatedMap, propertyMap);
                        cassandraOperation.incrementCounter(
                                Constants.KEYSPACE_SUNBIRD_COURSES,
                                Constants.TABLE_USER_EXTERNAL_ENROLMENTS_COUNTER,
                                counterKey(partnerId, Constants.SCOPE_TYPE_USER_ENROLMENTS, userId, Constants.COURSE_TYPE_PAID),
                                Map.of(Constants.COMPLETED_COUNT, 1L)
                        );
                        if (StringUtils.isNotBlank(userId)) {
                            String cacheKey = Constants.ENROLMENT_COUNTER_CACHE_PREFIX + partnerId + "_" + Constants.SCOPE_TYPE_USER_ENROLMENTS + "_" + userId + "_" + Constants.COURSE_TYPE_PAID + "_" + Constants.ACTIVE_COUNT;
                            cacheService.deleteCache(cacheKey, cbServerProperties.getRedisIndex());
                        }
                        sendUpdatedRecordDataToKafkaToGenerateCertificate(userCourseEnrollMap, result);
                        acknowledgment.acknowledge();
                    }
                } else {
                    log.info("User {} is not enrolled in course {}.", userId, courseId);
                }
            } else {
                log.error("Unable to get userid and courseid from kafka consumer");
            }

        } catch (Exception e) {
            log.error("Failed to read enroll Request. Message received : " + data.value(), e);
        }
    }

    @KafkaListener(topics = "${user.progress.send.from.partner.topic.name}", groupId = "${user.progress.send.from.partner.consumer.group.id}")
    public void receiveProgressUpdateFromPartner(ConsumerRecord<String, String> data, Acknowledgment acknowledgment) {
        log.info("KafkaConsumer::receiveProgressUpdateFromPartner:topic name: {} and recievedData: {}", data.topic(), data.value());
        try {
            JsonNode jsonNode = mapper.readTree(data.value());
            JsonNode partnerReadApiResponse = transformUtility.callContentPartnerReadByPartnerCodeApi(jsonNode.get("partnerCode").asText());
            if (!partnerReadApiResponse.path(Constants.TRANSFORM_PROGRESS_JSON).isMissingNode()) {
                String partnerid = partnerReadApiResponse.get("id").asText();
                List<Object> contentJson = mapper.convertValue(partnerReadApiResponse.path(Constants.TRANSFORM_PROGRESS_JSON), new TypeReference<List<Object>>() {
                });
                JsonNode transformData = transformUtility.transformData(jsonNode, contentJson);
                ((ObjectNode) transformData).put(Constants.PARTNER_ID, partnerid);
                JsonNode additionalProps = jsonNode.path("additionalProperties");
                if (!additionalProps.isMissingNode() && !additionalProps.isNull()) {
                    ((ObjectNode) transformData).set("additional_properties", additionalProps);
                }
                producer.push(cbServerProperties.getUserProgressUpdateTopic(), transformData);
                acknowledgment.acknowledge();
            } else {
                log.error("Partner Transform progress json is missing in content partner db, please update");
            }
        } catch (Exception e) {
            log.error("Failed to read enroll Request. Message received : " + data.value(), e);
        }
    }

    private void sendUpdatedRecordDataToKafkaToGenerateCertificate(Map<String, Object> userCourseEnrollMap, JsonNode result) {
        log.info("KafkaConsumer::sendUpdatedRecordDataToKafkaToGenerateCertificate:inside method");
        try {
            String courseId = "";
            String courseName = "";
            String contentPartnerName = "";
            String coursePosterImage = "";
            String partnerId = "";
            JsonNode contentNode = result.path("content");
            if (!contentNode.isMissingNode() && !contentNode.isNull()) {
                courseId = contentNode.get("contentId").asText();
                courseName = contentNode.path("name").asText(null);
                coursePosterImage = contentNode.path("appIcon").asText(null);
                JsonNode contentPartnerNode = contentNode.path("contentPartner");
                if (!contentPartnerNode.isMissingNode() && !contentPartnerNode.isNull()) {
                    contentPartnerName = contentPartnerNode.path("contentPartnerName").asText(null);
                    partnerId = contentPartnerNode.path("id").asText(null);
                }
            }
            JsonNode partnerApiResponse = transformUtility.callContentPartnerReadApi(partnerId);
            if (!partnerApiResponse.path("certificateTemplateUrl").isMissingNode() && !partnerApiResponse.path("certificateTemplateUrl").isNull()) {
                String svgTemplate = partnerApiResponse.get("certificateTemplateUrl").asText();
                Resource resource = resourceLoader.getResource("classpath:certificateTemplate.json");
                InputStream inputStream = resource.getInputStream();
                JsonNode jsonNode = mapper.readTree(inputStream);
                Map<String, Object> certificateRequest = new HashMap<>();
                certificateRequest.put(Constants.USER_ID, userCourseEnrollMap.get(Constants.USER_ID));
                certificateRequest.put(Constants.COURSE_ID, courseId);
                certificateRequest.put(Constants.COMPLETION_DATE, userCourseEnrollMap.get("completedon"));
                certificateRequest.put(Constants.PROVIDER_NAME, contentPartnerName);
                certificateRequest.put(Constants.COURSE_NAME, courseName);
                certificateRequest.put(Constants.COURSE_POSTER_IMAGE, coursePosterImage);
                certificateRequest.put(Constants.RECIPIENT_NAME, readUserName(userCourseEnrollMap.get(Constants.USER_ID).toString()));
                certificateRequest.put(Constants.SVG_TEMPLATE, svgTemplate);
                replacePlaceholders(jsonNode, certificateRequest);
                producer.push(cbServerProperties.getCertificateTopic(), jsonNode);
                log.info("KafkaConsumer::enrollUpdateConsumer:updated");
            } else {
                throw new CustomException(Constants.ERROR, "Certificate JsonData not found in Content Partner Response", HttpStatus.INTERNAL_SERVER_ERROR);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
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
        Optional<Map<String, Object>> optionalUser = userEnrollmentList.stream().findFirst();
        if (optionalUser.isPresent()) {
            Map<String, Object> user = optionalUser.get();
            String firstname = (String) user.get("firstname");
            String lastname = (String) user.get("lastname");
            String fullname = firstname;
            if (lastname != null) {
                fullname = fullname + " " + lastname;
            }
            return fullname;
        }
        return null;
    }


    private static Instant convertToTimestamp(String dateString) {
        if (dateString == null || dateString.trim().isEmpty()) {
            return null;
        }
        try {
            return Instant.parse(dateString);
        } catch (DateTimeParseException e1) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                LocalDateTime dateTime = LocalDateTime.parse(dateString, formatter);
                return dateTime.toInstant(ZoneOffset.UTC);
            } catch (DateTimeParseException e2) {
                e2.printStackTrace();
                return null;
            }
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
        String value = WordUtils.wrap((String) certificateRequest.get(Constants.COURSE_NAME), cbServerProperties.getCertificateCharLength(), "\n", false);
        switch (placeholder) {
            case "user.id":
                return (String) certificateRequest.get(Constants.USER_ID);
            case "course.id":
                return (String) certificateRequest.get(Constants.COURSE_ID);
            case "today.date":
                return convertDateFormat((String) certificateRequest.get(Constants.COMPLETION_DATE));
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
                return (String) certificateRequest.get(Constants.PROVIDER_NAME);
            case "user.name":
                return (String) certificateRequest.get(Constants.RECIPIENT_NAME);
            case "course.poster.image":
                return (String) certificateRequest.get(Constants.COURSE_POSTER_IMAGE);
            case "svgTemplate":
                return (String) certificateRequest.get(Constants.SVG_TEMPLATE);
            default:
                return "";
        }
    }

    private static String convertDateFormat(String originalDate) {
        Instant instant = Instant.parse(originalDate); // Parse ISO 8601 format
        ZonedDateTime zonedDateTime = instant.atZone(ZoneId.of("UTC"));
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return outputFormatter.format(zonedDateTime);
    }

}
