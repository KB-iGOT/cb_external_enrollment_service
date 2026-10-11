package com.igot.cb.util.exceptions;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CustomExceptionTest {

    @Test
    void testNoArgConstructor() {
        CustomException exception = new CustomException();
        assertNull(exception.getCode());
        assertNull(exception.getMessage());
        assertNull(exception.getHttpStatusCode());
    }

    @Test
    void testParameterizedConstructor() {
        CustomException exception = new CustomException("ERR", "message", HttpStatus.BAD_REQUEST);
        assertEquals("ERR", exception.getCode());
        assertEquals("message", exception.getMessage());
        assertEquals(HttpStatus.BAD_REQUEST, exception.getHttpStatusCode());
    }
}
