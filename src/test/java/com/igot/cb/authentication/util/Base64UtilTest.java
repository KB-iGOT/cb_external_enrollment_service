package com.igot.cb.authentication.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;


@ExtendWith(MockitoExtension.class)
public class Base64UtilTest {
    
    // Explicit public constructor (though the default would work too)
    public Base64UtilTest() {
        // Empty constructor
    }
    
    @Test
    public void testEncodeToString() {
        byte[] input = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        String encoded = Base64Util.encodeToString(input, 0);
        assertEquals("SGVsbG8sIFdvcmxkIQ==", encoded.trim());
    }
    
    @Test
    public void testDecode() {
        String encoded = "SGVsbG8sIFdvcmxkIQ==";
        byte[] decoded = Base64Util.decode(encoded, 0);
        assertArrayEquals("Hello, World!".getBytes(StandardCharsets.UTF_8), decoded);
    }
    
    @Test
    public void testDecodeByteArray() {
        byte[] encoded = "VGVzdA==".getBytes(StandardCharsets.US_ASCII);
        byte[] decoded = Base64Util.decode(encoded, 0);
        assertArrayEquals("Test".getBytes(StandardCharsets.UTF_8), decoded);
    }
}