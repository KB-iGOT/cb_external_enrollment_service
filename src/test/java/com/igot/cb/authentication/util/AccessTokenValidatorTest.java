package com.igot.cb.authentication.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AccessTokenValidatorTest {

    @Mock
    private AccessTokenValidator accessTokenValidator;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testValidateToken_Success() {
        String token = "validToken";
        when(accessTokenValidator.verifyUserToken(token)).thenReturn("userId123");

        String userId = accessTokenValidator.verifyUserToken(token);

        assertNotNull(userId);
        assertEquals("userId123", userId);
    }

    @Test
    void testValidateToken_InvalidToken() {
        String token = "invalidToken";
        when(accessTokenValidator.verifyUserToken(token)).thenReturn(null);

        String userId = accessTokenValidator.verifyUserToken(token);

        assertNull(userId);
    }
}