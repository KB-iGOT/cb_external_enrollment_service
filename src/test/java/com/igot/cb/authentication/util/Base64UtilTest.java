package com.igot.cb.authentication.util;

import static org.junit.Assert.*;
import org.junit.Test;
import java.nio.charset.StandardCharsets;

public class Base64UtilTest {

    @Test
    public void testEncodeToString() {
        byte[] input = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        String encoded = Base64Util.encodeToString(input, 0);
        assertEquals("SGVsbG8sIFdvcmxkIQ==", encoded);
    }

    @Test
    public void testDecode() {
        String encoded = "SGVsbG8sIFdvcmxkIQ==";
        byte[] decoded = Base64Util.decode(encoded, 0);
        assertArrayEquals("Hello, World!".getBytes(StandardCharsets.UTF_8), decoded);
    }

    @Test
    public void testEncode() {
        byte[] input = "Test".getBytes(StandardCharsets.UTF_8);
        byte[] encoded = Base64Util.encode(input, 0);
        assertArrayEquals("VGVzdA==".getBytes(StandardCharsets.US_ASCII), encoded);
    }

    @Test
    public void testDecodeByteArray() {
        byte[] encoded = "VGVzdA==".getBytes(StandardCharsets.US_ASCII);
        byte[] decoded = Base64Util.decode(encoded, 0);
        assertArrayEquals("Test".getBytes(StandardCharsets.UTF_8), decoded);
    }
}