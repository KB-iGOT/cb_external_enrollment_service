package com.igot.cb.authentication.model;

import org.junit.jupiter.api.Test;

import java.security.PublicKey;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class KeyDataTest {

    @Test
    void testConstructorGettersAndSetters() {
        PublicKey initialKey = mock(PublicKey.class);

        KeyData keyData = new KeyData("key-1", initialKey);

        // Verify constructor initialization
        assertAll(
                () -> assertEquals("key-1", keyData.getKeyId()),
                () -> assertSame(initialKey, keyData.getPublicKey())
        );

        // Verify setters
        PublicKey newKey = mock(PublicKey.class);

        keyData.setKeyId("key-2");
        keyData.setPublicKey(newKey);

        assertAll(
                () -> assertEquals("key-2", keyData.getKeyId()),
                () -> assertSame(newKey, keyData.getPublicKey())
        );
    }
}
