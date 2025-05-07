package com.igot.cb.authentication.util;

import static org.junit.Assert.*;
import org.junit.Test;
import java.nio.charset.StandardCharsets;

public class Base64UtilTest {

    private static final String TEST_STRING = "Hello, World!";
    private static final byte[] TEST_BYTES = TEST_STRING.getBytes(StandardCharsets.UTF_8);
    
    @Test
    public void testBasicEncodeDecodeString() {
        // Encode string to Base64
        String encoded = Base64Util.encodeToString(TEST_BYTES, Base64Util.DEFAULT);
        assertEquals("SGVsbG8sIFdvcmxkIQ==", encoded);
        
        // Decode Base64 back to original
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);
        String decodedString = new String(decoded, StandardCharsets.UTF_8);
        assertEquals(TEST_STRING, decodedString);
    }
    
    @Test
    public void testBasicEncodeDecodeBytes() {
        // Encode bytes to Base64
        byte[] encoded = Base64Util.encode(TEST_BYTES, Base64Util.DEFAULT);
        String encodedString = new String(encoded, StandardCharsets.US_ASCII);
        assertEquals("SGVsbG8sIFdvcmxkIQ==", encodedString);
        
        // Decode Base64 back to original
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);
        assertArrayEquals(TEST_BYTES, decoded);
    }
    
    @Test
    public void testUrlSafeEncoding() {
        String unsafe = "Subject?_a=b+c/d";
        byte[] unsafeBytes = unsafe.getBytes(StandardCharsets.UTF_8);
        
        // Standard encoding might use + and /
        String standard = Base64Util.encodeToString(unsafeBytes, Base64Util.DEFAULT);
        assertTrue(standard.contains("+") || standard.contains("/"));
        
        // URL-safe should use - and _
        String urlSafe = Base64Util.encodeToString(unsafeBytes, Base64Util.URL_SAFE);
        assertFalse(urlSafe.contains("+"));
        assertFalse(urlSafe.contains("/"));
        
        // Decode URL-safe encoding
        byte[] decoded = Base64Util.decode(urlSafe, Base64Util.URL_SAFE);
        assertEquals(unsafe, new String(decoded, StandardCharsets.UTF_8));
    }
    
    @Test
    public void testNoPadding() {
        // Default encoding includes padding
        String withPadding = Base64Util.encodeToString(TEST_BYTES, Base64Util.DEFAULT);
        assertTrue(withPadding.endsWith("=="));
        
        // NO_PADDING should omit the = signs
        String noPadding = Base64Util.encodeToString(TEST_BYTES, Base64Util.NO_PADDING);
        assertFalse(noPadding.contains("="));
        
        // Should still decode correctly
        byte[] decoded = Base64Util.decode(noPadding, Base64Util.DEFAULT);
        assertEquals(TEST_STRING, new String(decoded, StandardCharsets.UTF_8));
    }
    
    @Test
    public void testNoWrap() {
        // Create a longer string to ensure line wrapping would occur
        StringBuilder longString = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            longString.append(TEST_STRING);
        }
        byte[] longBytes = longString.toString().getBytes(StandardCharsets.UTF_8);
        
        // Default should wrap lines
        String wrapped = Base64Util.encodeToString(longBytes, Base64Util.DEFAULT);
        assertTrue(wrapped.contains("\n"));
        
        // NO_WRAP should have no line breaks
        String noWrap = Base64Util.encodeToString(longBytes, Base64Util.NO_WRAP);
        assertFalse(noWrap.contains("\n"));
        
        // Should still decode correctly
        byte[] decoded = Base64Util.decode(noWrap, Base64Util.DEFAULT);
        assertEquals(longString.toString(), new String(decoded, StandardCharsets.UTF_8));
    }
    
    @Test
    public void testCrLfOption() {
        // Create a string long enough to trigger line wrapping
        StringBuilder longString = new StringBuilder();
        for (int i = 0; i < 30; i++) {
            longString.append(TEST_STRING);
        }
        byte[] longBytes = longString.toString().getBytes(StandardCharsets.UTF_8);
        
        // Default should use just LF
        String defaultEncoded = Base64Util.encodeToString(longBytes, Base64Util.DEFAULT);
        assertTrue(defaultEncoded.contains("\n"));
        assertFalse(defaultEncoded.contains("\r\n"));
        
        // CRLF option should use CR+LF
        String crlfEncoded = Base64Util.encodeToString(longBytes, Base64Util.CRLF);
        assertTrue(crlfEncoded.contains("\r\n"));
        
        // Both should decode correctly
        byte[] decoded1 = Base64Util.decode(defaultEncoded, Base64Util.DEFAULT);
        byte[] decoded2 = Base64Util.decode(crlfEncoded, Base64Util.DEFAULT);
        
        assertEquals(longString.toString(), new String(decoded1, StandardCharsets.UTF_8));
        assertEquals(longString.toString(), new String(decoded2, StandardCharsets.UTF_8));
    }
    
    @Test
    public void testEmptyInput() {
        // Test encoding empty string
        String emptyEncoded = Base64Util.encodeToString(new byte[0], Base64Util.DEFAULT);
        assertEquals("", emptyEncoded);
        
        // Test decoding empty string
        byte[] emptyDecoded = Base64Util.decode("", Base64Util.DEFAULT);
        assertEquals(0, emptyDecoded.length);
    }
    
    @Test
    public void testPartialBlocksEncoding() {
        // Test with 1 byte (requires 2 chars + 2 padding)
        byte[] oneByte = new byte[] { 65 }; // 'A'
        String oneByteEncoded = Base64Util.encodeToString(oneByte, Base64Util.DEFAULT);
        assertEquals("QQ==", oneByteEncoded);
        
        // Test with 2 bytes (requires 3 chars + 1 padding)
        byte[] twoBytes = new byte[] { 65, 66 }; // 'AB'
        String twoBytesEncoded = Base64Util.encodeToString(twoBytes, Base64Util.DEFAULT);
        assertEquals("QUI=", twoBytesEncoded);
    }
    
    @Test
    public void testDifferentDecodeSources() {
        String base64 = "SGVsbG8sIFdvcmxkIQ==";
        
        // Decode from String
        byte[] fromString = Base64Util.decode(base64, Base64Util.DEFAULT);
        assertEquals(TEST_STRING, new String(fromString, StandardCharsets.UTF_8));
        
        // Decode from byte[]
        byte[] fromBytes = Base64Util.decode(base64.getBytes(StandardCharsets.US_ASCII), Base64Util.DEFAULT);
        assertEquals(TEST_STRING, new String(fromBytes, StandardCharsets.UTF_8));
        
        // Decode from specific portion of byte[]
        byte[] withPrefix = ("prefix" + base64).getBytes(StandardCharsets.US_ASCII);
        byte[] fromPortion = Base64Util.decode(withPrefix, 6, base64.length(), Base64Util.DEFAULT);
        assertEquals(TEST_STRING, new String(fromPortion, StandardCharsets.UTF_8));
    }
    
    @Test
    public void testBinaryData() {
        // Create binary data with all possible byte values
        byte[] binaryData = new byte[256];
        for (int i = 0; i < 256; i++) {
            binaryData[i] = (byte) i;
        }
        
        // Encode and decode
        String encoded = Base64Util.encodeToString(binaryData, Base64Util.DEFAULT);
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);
        
        // Verify all bytes were preserved
        assertArrayEquals(binaryData, decoded);
    }
    
    @Test
    public void testOffsetAndLength() {
        byte[] data = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        
        // Encode portion of the data (offset 10, length 5)
        String encoded = Base64Util.encodeToString(data, 10, 5, Base64Util.DEFAULT);
        
        // Verify the encoded data
        byte[] subArray = new byte[5];
        System.arraycopy(data, 10, subArray, 0, 5);
        String expectedEncoded = Base64Util.encodeToString(subArray, Base64Util.DEFAULT);
        assertEquals(expectedEncoded, encoded);
        
        // Decode and verify
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);
        assertEquals(new String(subArray, StandardCharsets.UTF_8), new String(decoded, StandardCharsets.UTF_8));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidBase64() {
        // This string has invalid Base64 characters
        Base64Util.decode("This is not valid Base64!", Base64Util.DEFAULT);
    }
    
    @Test
    public void testCombinedFlags() {
        // Test URL_SAFE + NO_PADDING + NO_WRAP
        int flags = Base64Util.URL_SAFE | Base64Util.NO_PADDING | Base64Util.NO_WRAP;
        
        String encoded = Base64Util.encodeToString(TEST_BYTES, flags);
        
        // Should not contain padding
        assertFalse(encoded.contains("="));
        
        // Should not contain line breaks
        assertFalse(encoded.contains("\n"));
        assertFalse(encoded.contains("\r"));
        
        // Should use URL-safe alphabet
        assertFalse(encoded.contains("+"));
        assertFalse(encoded.contains("/"));
        
        // Should still decode correctly
        byte[] decoded = Base64Util.decode(encoded, flags);
        assertEquals(TEST_STRING, new String(decoded, StandardCharsets.UTF_8));
    }
    
    @Test
    public void testWhitespaceHandling() {
        // Add whitespace to the encoded string
        String clean = Base64Util.encodeToString(TEST_BYTES, Base64Util.DEFAULT);
        String withWhitespace = clean.substring(0, 5) + " \t\n\r" + clean.substring(5);
        
        // Should still decode correctly
        byte[] decoded = Base64Util.decode(withWhitespace, Base64Util.DEFAULT);
        assertEquals(TEST_STRING, new String(decoded, StandardCharsets.UTF_8));
    }
}