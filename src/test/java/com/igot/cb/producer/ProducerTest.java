package com.igot.cb.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.server.ResponseStatusException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ProducerTest {

    @InjectMocks
    private Producer producer;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    static class TestData {
        public String name;
        public int value;

        public TestData(String name, int value) {
            this.name = name;
            this.value = value;
        }
    }

    @Test
    void testPush_success() throws Exception {
        // Arrange
        TestData data = new TestData("test", 123);
        String topic = "test-topic";
        String json = "{\"name\":\"test\",\"value\":123}";

        when(objectMapper.writeValueAsString(data)).thenReturn(json);

        // Act
        producer.push(topic, data);

        // Assert
        verify(kafkaTemplate, times(1)).send(topic, json);
    }

    @Test
    void testPush_exception() throws Exception {
        // Arrange
        TestData data = new TestData("error", 456);
        String topic = "error-topic";

        when(objectMapper.writeValueAsString(data)).thenThrow(new RuntimeException("Serialization failed"));

        // Act & Assert
        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> {
            producer.push(topic, data);
        });

        assertEquals(HttpStatus.BAD_REQUEST, exception.getStatusCode());
        assertTrue(exception.getReason().contains("Serialization failed"));

        verify(kafkaTemplate, never()).send(any(), any());
    }
}