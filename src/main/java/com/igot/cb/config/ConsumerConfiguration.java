package com.igot.cb.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.enrollment.service.impl.EnrollmentServiceImpl;
import com.igot.cb.util.Constants;
import com.igot.cb.util.dto.SBApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;
@Configuration
@Slf4j
public class ConsumerConfiguration {
    @Value("${spring.kafka.bootstrap.servers}")
    private String kafkabootstrapAddress;

    @Value("${kakfa.offset.reset.value}")
    private String kafkaOffsetResetValue;

    @Value("${kafka.max.poll.interval.ms}")
    private Integer kafkaMaxPollInterval;

    @Value("${kafka.max.poll.records}")
    private Integer kafkaMaxPollRecords;

    @Autowired
    private EnrollmentServiceImpl enrollmentService;

    @Autowired
    private ObjectMapper objectMapper;

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(4);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, 3)));
        return factory;
    }

    // Dedicated factory for the paid-course-enrolment listener only. Its retries-exhausted
    // recovery interprets the payload as a karma-coin reward candidate - applying that same
    // recoverer to the other listeners sharing kafkaListenerContainerFactory() above would be
    // meaningless (different event shapes) and crash-prone, so this listener opts into its own
    // factory instead (see KafkaConsumer#validateAndEnrolPaidCourses's containerFactory attribute).
    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> paidCourseKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(4);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(new DefaultErrorHandler(buildPaidCourseRecoverer(), new FixedBackOff(1000L, 3)));
        return factory;
    }

    // Package-private (rather than folded into the lambda) so it can be unit tested directly by
    // invoking accept(...) on the returned recoverer, without needing to dig it back out of the
    // container factory bean.
    ConsumerRecordRecoverer buildPaidCourseRecoverer() {
        return (record, exception) -> {
            log.error("Paid course enrolment failed after exhausting retries. topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset(), exception);

            Map<String, Object> eventData;
            String userId;
            String courseId;
            String courseName;
            String providerName;
            try {
                Map<String, Object> paidCourseEvent = objectMapper.readValue(
                        (String) record.value(), new TypeReference<Map<String, Object>>() {});
                eventData = (Map<String, Object>) paidCourseEvent.get(Constants.DATA);
                userId = (String) eventData.get(Constants.EVENT_USER_ID);
                courseId = (String) eventData.get(Constants.CONTEXT_ID);
                courseName = (String) eventData.get(Constants.COURSE_NAME);
                providerName = (String) eventData.get(Constants.PROVIDER_NAME);
            } catch (Exception parseException) {
                // Nothing to reward without knowing who/how much - there is no fallback for
                // this case, it is surfaced as a CRITICAL log only.
                log.error("CRITICAL: could not parse the paid course enrolment event to attempt a reaward - "
                                + "manual intervention required, coins are NOT refunded. offset={}, payload={}",
                        record.offset(), record.value(), parseException);
                return;
            }

            try {
                if (enrollmentService.isUserEnrolled(new SBApiResponse(), userId, courseId)) {
                    log.warn("User {} is already enrolled in course {} despite retries being exhausted - skipping reaward",
                            userId, courseId);
                    return;
                }
                enrollmentService.triggerCoinsReaward(eventData, courseName, providerName,
                        "Paid course enrolment failed after exhausting retries");
            } catch (Exception e) {
                log.error("CRITICAL: reaward attempt failed after paid course enrolment retries were exhausted - "
                                + "manual intervention required, coins are NOT refunded. userId={}, courseId={}, offset={}",
                        userId, courseId, record.offset(), e);
            }
        };
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());

    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> propsMap = new HashMap<>();
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkabootstrapAddress);
        propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        propsMap.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "1000");
        propsMap.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaOffsetResetValue);
        propsMap.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafkaMaxPollInterval);
        propsMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaMaxPollRecords);
        return propsMap;
    }
}
