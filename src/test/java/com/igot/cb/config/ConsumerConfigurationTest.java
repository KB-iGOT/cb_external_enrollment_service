package com.igot.cb.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.enrollment.service.impl.EnrollmentServiceImpl;
import com.igot.cb.util.Constants;
import com.igot.cb.util.dto.SBApiResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConsumerConfigurationTest {

    private ConsumerConfiguration config;
    private EnrollmentServiceImpl enrollmentService;

    @BeforeEach
    void setUp() {
        config = new ConsumerConfiguration();

        // Inject test values manually
        setField(config, "kafkabootstrapAddress", "localhost:9092");
        setField(config, "kafkaOffsetResetValue", "earliest");
        setField(config, "kafkaMaxPollInterval", 300000);
        setField(config, "kafkaMaxPollRecords", 500);

        enrollmentService = mock(EnrollmentServiceImpl.class);
        setField(config, "enrollmentService", enrollmentService);
        setField(config, "objectMapper", new ObjectMapper());
    }

    private void setField(Object target, String fieldName, Object value) {
        try {
            var field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testConsumerConfigs() {
        Map<String, Object> configs = config.consumerConfigs();
        assertEquals("localhost:9092", configs.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(false, configs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals("1000", configs.get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG));
        assertEquals("15000", configs.get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG));
        assertEquals(StringDeserializer.class, configs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(StringDeserializer.class, configs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        assertEquals("earliest", configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(300000, configs.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG));
        assertEquals(500, configs.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    @Test
    void testConsumerFactory() {
        ConsumerFactory<String, String> factory = config.consumerFactory();
        assertNotNull(factory);
    }

    @Test
    void testKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = (ConcurrentKafkaListenerContainerFactory<String, String>) config.kafkaListenerContainerFactory();
        assertNotNull(factory);
        assertEquals(3000, factory.getContainerProperties().getPollTimeout());
        assertEquals(ContainerProperties.AckMode.MANUAL_IMMEDIATE, factory.getContainerProperties().getAckMode());
    }

    @Test
    void testPaidCourseKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                (ConcurrentKafkaListenerContainerFactory<String, String>) config.paidCourseKafkaListenerContainerFactory();
        assertNotNull(factory);
        assertEquals(3000, factory.getContainerProperties().getPollTimeout());
        assertEquals(ContainerProperties.AckMode.MANUAL_IMMEDIATE, factory.getContainerProperties().getAckMode());
    }

    private ConsumerRecord<String, String> paidCourseRecord(String payload) {
        return new ConsumerRecord<>("paid-course-topic", 0, 5L, "key", payload);
    }

    private Map<String, Object> wellFormedPayloadMap() {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put(Constants.EVENT_USER_ID, "user1");
        eventData.put(Constants.CONTEXT_ID, "course1");
        eventData.put(Constants.COURSE_NAME, "Course One");
        eventData.put(Constants.PROVIDER_NAME, "Provider One");
        Map<String, Object> event = new HashMap<>();
        event.put(Constants.DATA, eventData);
        return event;
    }

    @Test
    void buildPaidCourseRecoverer_notEnrolled_triggersReawardOnce() throws Exception {
        String payload = new ObjectMapper().writeValueAsString(wellFormedPayloadMap());
        when(enrollmentService.isUserEnrolled(any(SBApiResponse.class), eq("user1"), eq("course1"))).thenReturn(false);

        ConsumerRecordRecoverer recoverer = config.buildPaidCourseRecoverer();
        recoverer.accept(paidCourseRecord(payload), new RuntimeException("retries exhausted"));

        verify(enrollmentService, times(1)).triggerCoinsReaward(any(), eq("Course One"), eq("Provider One"),
                eq("Paid course enrolment failed after exhausting retries"));
    }

    @Test
    void buildPaidCourseRecoverer_alreadyEnrolled_skipsReaward() throws Exception {
        String payload = new ObjectMapper().writeValueAsString(wellFormedPayloadMap());
        when(enrollmentService.isUserEnrolled(any(SBApiResponse.class), eq("user1"), eq("course1"))).thenReturn(true);

        ConsumerRecordRecoverer recoverer = config.buildPaidCourseRecoverer();
        recoverer.accept(paidCourseRecord(payload), new RuntimeException("retries exhausted"));

        // Retries exhausted doesn't mean the enrolment actually failed - the user is already
        // enrolled, so rewarding here would refund coins for a course they legitimately got.
        verify(enrollmentService, never()).triggerCoinsReaward(any(), any(), any(), any());
    }

    @Test
    void buildPaidCourseRecoverer_malformedPayload_doesNotThrowAndNeverAttemptsReaward() {
        ConsumerRecordRecoverer recoverer = config.buildPaidCourseRecoverer();
        ConsumerRecord<String, String> record = paidCourseRecord("{not valid json}");

        // Nothing to reward without a parseable userId/courseId - there is no failure-topic
        // fallback here on purpose (a queue nobody drains is equivalent to silent loss), so this
        // case can only ever be a CRITICAL log; verify it at least never throws or half-acts.
        assertDoesNotThrow(() -> recoverer.accept(record, new RuntimeException("retries exhausted")));

        verify(enrollmentService, never()).isUserEnrolled(any(), any(), any());
        verify(enrollmentService, never()).triggerCoinsReaward(any(), any(), any(), any());
    }

    @Test
    void buildPaidCourseRecoverer_triggerCoinsRewardFails_doesNotThrowAndAttemptsOnlyOnce() throws Exception {
        String payload = new ObjectMapper().writeValueAsString(wellFormedPayloadMap());
        when(enrollmentService.isUserEnrolled(any(SBApiResponse.class), eq("user1"), eq("course1"))).thenReturn(false);
        doThrow(new RuntimeException("kafka down"))
                .when(enrollmentService).triggerCoinsReaward(any(), any(), any(), any());

        ConsumerRecordRecoverer recoverer = config.buildPaidCourseRecoverer();
        ConsumerRecord<String, String> record = paidCourseRecord(payload);

        // No retry loop here: the container already retried the whole listener with backoff
        // before this ever ran. A failure here must still not propagate - the last resort is a
        // CRITICAL log, not an exception - but there's no second attempt to fall back on.
        assertDoesNotThrow(() -> recoverer.accept(record, new RuntimeException("retries exhausted")));

        verify(enrollmentService, times(1)).triggerCoinsReaward(any(), any(), any(), any());
    }
}