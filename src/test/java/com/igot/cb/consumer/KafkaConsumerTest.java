package com.igot.cb.consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class KafkaConsumerTest {

    @Mock
    private KafkaConsumer kafkaConsumer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    // @Test
    // void testConsumeMessage_Success() {
    //     String topic = "test-topic";
    //     String message = "test-message";
    //     // Updated to mock a valid consume method
    //     doNothing().when(kafkaConsumer).consumeMessage(topic, message);
    //     kafkaConsumer.consumeMessage(topic, message);
    //     verify(kafkaConsumer, times(1)).consumeMessage(topic, message);
    // }

    // @Test
    // void testConsumeMessage_NullMessage() {
    //     String topic = "test-topic";
    //     String message = null;
    //     // Updated to mock a valid consume method
    //     doThrow(new IllegalArgumentException("Message cannot be null")).when(kafkaConsumer).consumeMessage(topic, message);
    //     Exception exception = assertThrows(IllegalArgumentException.class, () -> {
    //         kafkaConsumer.consumeMessage(topic, message);
    //     });
    //     assertEquals("Message cannot be null", exception.getMessage());
    // }
}