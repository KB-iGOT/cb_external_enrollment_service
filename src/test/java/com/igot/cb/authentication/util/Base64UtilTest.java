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

    @Test
    public void testEncodeWithFlags() {
        byte[] input = "Test with flags".getBytes(StandardCharsets.UTF_8);
        String encoded = Base64Util.encodeToString(input, Base64Util.NO_PADDING);
        assertEquals("VGVzdCB3aXRoIGZsYWdz", encoded.trim());

        encoded = Base64Util.encodeToString(input, Base64Util.NO_WRAP);
        assertEquals("VGVzdCB3aXRoIGZsYWdz", encoded.trim());

        encoded = Base64Util.encodeToString(input, Base64Util.CRLF);
        assertEquals("VGVzdCB3aXRoIGZsYWdz", encoded.trim());

        encoded = Base64Util.encodeToString(input, Base64Util.URL_SAFE);
        assertEquals("VGVzdCB3aXRoIGZsYWdz", encoded.trim());
    }

    @Test
    public void testDecodeWithFlags() {
        String encoded = "VGVzdCB3aXRoIGZsYWdz";
        byte[] decoded = Base64Util.decode(encoded, Base64Util.NO_PADDING);
        assertArrayEquals("Test with flags".getBytes(StandardCharsets.UTF_8), decoded);

        decoded = Base64Util.decode(encoded, Base64Util.NO_WRAP);
        assertArrayEquals("Test with flags".getBytes(StandardCharsets.UTF_8), decoded);

        decoded = Base64Util.decode(encoded, Base64Util.CRLF);
        assertArrayEquals("Test with flags".getBytes(StandardCharsets.UTF_8), decoded);

        decoded = Base64Util.decode(encoded, Base64Util.URL_SAFE);
        assertArrayEquals("Test with flags".getBytes(StandardCharsets.UTF_8), decoded);
    }

    @Test
    public void testEncodeDecodeConsistency() {
        byte[] input = "Consistency Test".getBytes(StandardCharsets.UTF_8);
        String encoded = Base64Util.encodeToString(input, Base64Util.DEFAULT);
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);
        assertArrayEquals(input, decoded);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecodeInvalidInput() {
        String invalidEncoded = "Invalid===";
        Base64Util.decode(invalidEncoded, Base64Util.DEFAULT);
    }
}