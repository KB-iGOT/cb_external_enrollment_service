package com.igot.cb.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

@Component
@Slf4j
public class Producer {
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private ObjectMapper objectMapper;

    public void push(String topic, Object data) {
        push(topic, data, null);
    }

    /**
     * Keyed send - Kafka's own partitioner routes every message sharing the same key to the
     * same partition, and a partition is only ever consumed by one thread at a time within a
     * consumer group. Callers that need strict per-key ordering (e.g. every event for a given
     * user/partner pair processed strictly one at a time, in order) should pass that key rather
     * than using the unkeyed overload above.
     */
    public void push(String topic, Object data, String key) {
        try {
            String message = objectMapper.writeValueAsString(data);
            log.info("KafkaProducer::sendCornellData: topic: {}", topic);
            if (key != null) {
                this.kafkaTemplate.send(topic, key, message);
            } else {
                this.kafkaTemplate.send(topic, message);
            }
            log.info("Data sent to kafka topic {} and message is {}", topic, message);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }

    }

}
