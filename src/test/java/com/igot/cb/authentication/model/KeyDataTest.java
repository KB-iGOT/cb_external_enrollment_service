package com.igot.cb.authentication.model;

import org.junit.jupiter.api.Test;

import java.security.PublicKey;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class KeyDataTest {

    @Test
    void testSetters() {
        KeyData keyData = new KeyData("key1", mock(PublicKey.class));

        PublicKey newPublicKey = mock(PublicKey.class);
        keyData.setKeyId("key2");
        keyData.setPublicKey(newPublicKey);

        assertEquals("key2", keyData.getKeyId());
        assertEquals(newPublicKey, keyData.getPublicKey());
    }
}
