package com.igot.cb.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;
@Configuration
public class ConsumerConfiguration {
    @Value("${spring.kafka.bootstrap.servers}")
    private String kafkabootstrapAddress;

    @Value("${kakfa.offset.reset.value}")
    private String kafkaOffsetResetValue;

    @Value("${kafka.max.poll.interval.ms}")
    private Integer kafkaMaxPollInterval;

    @Value("${kafka.max.poll.records}")
    private Integer kafkaMaxPollRecords;

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(4);
        factory.getContainerProperties().setPollTimeout(3000);
        // Commit the offset only once a listener method has actually returned (success or a
        // caught/logged internal failure - our listeners never rethrow), never on a background
        // timer. With auto-commit, the offset can advance on a fixed interval regardless of
        // whether processing has finished; a pod restart between that timer firing and the
        // listener completing silently drops the in-flight event forever, since Kafka believes
        // it was already committed. MANUAL_IMMEDIATE ties the commit to actual completion
        // instead, so a restart mid-processing redelivers the event next time rather than
        // losing it. This does still allow a message to be reprocessed if the pod dies after
        // the listener returns but before the commit round-trip finishes - normal at-least-once
        // behavior - so any listener whose side effects aren't naturally safe to repeat still
        // needs its own idempotency guard rather than relying on the commit strategy alone.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // A listener that lets an exception propagate (rather than catching/logging it
        // internally, as our other two listeners still do) gets retried up to twice, 1s apart,
        // before the container gives up, logs, and moves past that record - this only changes
        // behavior for a listener that actually throws; a caught-and-logged internal failure
        // never reaches this handler at all, so the other two listeners are unaffected.
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, 2)));
        return factory;
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
