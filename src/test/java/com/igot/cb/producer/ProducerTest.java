package com.igot.cb.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ProducerTest {

    @Mock
    private Producer producer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testPushMessage_Success() {
        String topic = "test-topic";
        String message = "test-message";
        doNothing().when(producer).push(topic, message);

        producer.push(topic, message);

        verify(producer, times(1)).push(topic, message);
    }

    @Test
    void testPushMessage_NullMessage() {
        String topic = "test-topic";
        String message = null;
        doThrow(new IllegalArgumentException("Message cannot be null")).when(producer).push(topic, message);

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            producer.push(topic, message);
        });

        assertEquals("Message cannot be null", exception.getMessage());
    }
}