package com.igot.cb.authentication.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class Base64UtilTest {

    /**
     * Tests that the decode method throws an IllegalArgumentException when given input with incorrect padding.
     * This tests the explicitly handled edge case in the method's implementation where incorrect padding is detected.
     */
    @Test
    void testDecodeWithIncorrectPadding() {
        String inputWithIncorrectPadding = "SGVsbG8gV29ybGQ====="; // Extra padding

        assertThrows(IllegalArgumentException.class, () -> {
            Base64Util.decode(inputWithIncorrectPadding, Base64Util.DEFAULT);
        });
    }

    /**
     * Tests that decode throws IllegalArgumentException when input contains incorrect padding.
     */
    @Test
    void testDecodeWithIncorrectPadding_2() {
        byte[] input = "Invalid==Padding".getBytes();

        assertThrows(IllegalArgumentException.class, () -> {
            Base64Util.decode(input, Base64Util.DEFAULT);
        });
    }

    /**
     * Tests that the encode method handles an empty input byte array correctly.
     * This is an edge case where the input is valid but contains no data.
     */
    @Test
    void testEncodeEmptyInput() {
        byte[] emptyInput = new byte[0];
        byte[] result = Base64Util.encode(emptyInput, Base64Util.DEFAULT);
        assertNotNull(result);
        assertEquals(0, result.length);
    }


    /**
     * Tests encoding with padding, non-multiple of 3 input length, and newline insertion.
     *
     * Path constraints:
     * - encoder.do_padding is true
     * - len % 3 > 0
     * - encoder.do_newline && len > 0
     */
    @Test
    void testEncodeWithPaddingAndNewline() {
        byte[] input = "Hello, World!".getBytes();
        int offset = 0;
        int len = input.length;
        int flags = Base64Util.DEFAULT;

        byte[] result = Base64Util.encode(input, offset, len, flags);

        String expected = "SGVsbG8sIFdvcmxkIQ==\n";
        assertEquals(expected, new String(result));
    }

    /**
     * Test encoding with zero-length input.
     * This tests the edge case of providing an empty byte array as input.
     */
    @Test
    void testEncodeWithZeroLengthInput() {
        byte[] input = new byte[0];
        byte[] result = Base64Util.encode(input, Base64Util.DEFAULT);
        assertNotNull(result);
        assertEquals(0, result.length);
    }

    /**
     * Tests the decode method with a simple Base64 encoded string.
     * This test verifies that the method correctly decodes a standard Base64 string
     * using the default flags.
     */
    @Test
    void test_decode_1() {
        String input = "SGVsbG8gV29ybGQ=";
        byte[] expected = "Hello World".getBytes();
        byte[] result = Base64Util.decode(input, Base64Util.DEFAULT);
        assertArrayEquals(expected, result);
    }

    /**
     * Test case for decoding a simple Base64 encoded string
     * This test verifies that the decode method correctly decodes a Base64 encoded input
     * using the default flags (Base64Util.DEFAULT)
     */
    @Test
    void test_decode_1_2() {
        String input = "SGVsbG8gV29ybGQ="; // "Hello World" in Base64
        byte[] expectedOutput = "Hello World".getBytes();
        byte[] result = Base64Util.decode(input.getBytes(), Base64Util.DEFAULT);
        assertArrayEquals(expectedOutput, result);
    }

    /**
     * Test case for decoding an empty input array.
     * This test verifies that the decode method correctly handles an empty input
     * and returns an empty byte array without throwing an exception.
     */
    @Test
    void test_decode_emptyInput() {
        byte[] input = new byte[0];
        byte[] result = Base64Util.decode(input, 0, 0, Base64Util.DEFAULT);
        assertArrayEquals(new byte[0], result);
    }

    /**
     * Test case for decoding a Base64 input that results in an output of exact length.
     * This test verifies that the decode method correctly processes the input and
     * returns the output array without needing to create a new array.
     */
    @Test
    void test_decode_exactOutputLength() {
        // Input that will decode to an exact length output
        byte[] input = "SGVsbG8gV29ybGQ=".getBytes();
        int offset = 0;
        int len = input.length;
        int flags = Base64Util.DEFAULT;

        byte[] result = Base64Util.decode(input, offset, len, flags);

        // Expected output: "Hello World"
        byte[] expected = {72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100};
        assertArrayEquals(expected, result);
    }


    /**
     * Tests the encode method with no padding and newline insertion.
     * This test case covers the path where padding is disabled (!encoder.do_padding)
     * and newline insertion is enabled (encoder.do_newline && len > 0).
     */
    @Test
    void test_encode_3() {
        byte[] input = "Hello, World!".getBytes();
        int offset = 0;
        int len = input.length;
        int flags = Base64Util.NO_PADDING | Base64Util.CRLF;

        byte[] result = Base64Util.encode(input, offset, len, flags);

        String expected = "SGVsbG8sIFdvcmxkIQ==\r\n".replace("==", "");
        String actual = new String(result);

        assertEquals(expected, actual);
    }

    /**
     * Testcase 4 for public static byte[] encode(byte[] input, int offset, int len, int flags)
     * Tests encoding with padding when input length is not a multiple of 3 and newlines are disabled.
     */
    @Test
    void test_encode_4() {
        byte[] input = {1, 2, 3, 4, 5};
        int offset = 0;
        int len = 5;
        int flags = Base64Util.NO_WRAP; // Disable newlines

        byte[] result = Base64Util.encode(input, offset, len, flags);

        // Expected output: "AQIDBAU=" (Base64 encoded with padding)
        byte[] expected = {65, 81, 73, 68, 66, 65, 85, 61};
        assertArrayEquals(expected, result);
    }

    @Test
    void testEncodeWithUrlSafe() {
        byte[] input = "Test_URL_Safe".getBytes();
        byte[] result = Base64Util.encode(input, Base64Util.URL_SAFE);
        assertNotNull(result);
        String encoded = new String(result);
        assertFalse(encoded.contains("+"));
        assertFalse(encoded.contains("/"));
    }

    @Test
    void testEncodeNoPaddingNoWrap() {
        byte[] input = "PaddingTest".getBytes();
        byte[] result = Base64Util.encode(input, Base64Util.NO_PADDING | Base64Util.NO_WRAP);
        String encoded = new String(result);
        assertFalse(encoded.contains("="));
        assertFalse(encoded.contains("\n"));
    }

    @Test
    void testEncodeToString() {
        byte[] input = "Hello".getBytes();
        String encoded = Base64Util.encodeToString(input, Base64Util.DEFAULT);
        assertNotNull(encoded);
        assertEquals("SGVsbG8=", encoded.trim());
    }

    @Test
    void testDecodeUrlSafe() {
        String urlSafe = "U29tZS1fdXJsX3NhZmUtZGF0YQ";
        byte[] decoded = Base64Util.decode(urlSafe, Base64Util.URL_SAFE);
        assertEquals("Some-_url_safe-data", new String(decoded));
    }

    @Test
    void testDecodeWithWhitespace() {
        String withWhitespace = "U29tZSBkYXRh\n";
        byte[] decoded = Base64Util.decode(withWhitespace, Base64Util.DEFAULT);
        assertEquals("Some data", new String(decoded));
    }

    @Test
    void testEncodeWithCRLFAndNoWrap() {
        byte[] input = "LineCheck".getBytes();
        byte[] result = Base64Util.encode(input, Base64Util.CRLF | Base64Util.NO_WRAP);
        String encoded = new String(result);
        assertFalse(encoded.contains("\r\n"));
    }

    @Test
    void testEncodeDecodeRoundTrip() {
        String original = "RoundTripTest123";
        String encoded = Base64Util.encodeToString(original.getBytes(), Base64Util.DEFAULT);
        byte[] decoded = Base64Util.decode(encoded, Base64Util.DEFAULT);
        assertEquals(original, new String(decoded));
    }

    @Test
    void testEncodeTailOneByteRemaining() {
        byte[] input = {(byte) 'A'};
        byte[] result = Base64Util.encode(input, Base64Util.DEFAULT);
        assertEquals("QQ==", new String(result).trim());
    }


    @Test
    void testEncodeTailTwoBytesRemaining() {
        byte[] input = {(byte) 'A', (byte) 'B'};
        byte[] result = Base64Util.encode(input, Base64Util.DEFAULT);
        assertEquals("QUI=", new String(result).trim());
    }

    @Test
    void testDecodeSinglePadding() {
        String input = "QUI=";
        byte[] decoded = Base64Util.decode(input, Base64Util.DEFAULT);
        assertEquals("AB", new String(decoded));
    }

    @Test
    void testDecodeCompletelyInvalidInput_shouldReturnEmptyOrFailGracefully() {
        String invalidBase64 = "!@#$%^";
        byte[] result = Base64Util.decode(invalidBase64, Base64Util.DEFAULT);
        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "A$==",
            "QUJD$A==",
            "Q",
            "QUJD=="
    })
    void testDecoderWithInvalidInputs(String base64Input) {
        byte[] input = base64Input.getBytes();
        assertThrows(IllegalArgumentException.class, () ->
                Base64Util.decode(input, 0, input.length, Base64Util.DEFAULT)
        );
    }

    @Test
    void testEncoderLineWrapping() {
        byte[] input = new byte[57];
        for (int i = 0; i < 57; i++) input[i] = (byte) i;
        byte[] result = Base64Util.encode(input, Base64Util.DEFAULT);
        String encoded = new String(result);
        assertTrue(encoded.contains("\n"));
    }

    @Test
    void testEncoderUrlSafeWithTail() {
        byte[] input = new byte[]{'T', 'E'};
        byte[] result = Base64Util.encode(input, Base64Util.URL_SAFE);
        String encoded = new String(result);
        assertTrue(encoded.startsWith("VE"));
    }

    @Test
    void testEncodeToStringWithOffsetAndLength() {
        byte[] input = "HelloWorld".getBytes();
        String encoded = Base64Util.encodeToString(input, 0, input.length, Base64Util.DEFAULT);
        assertEquals("SGVsbG9Xb3JsZA==", encoded.trim());
    }

    @Test
    void testEncodeExactMultipleOf3Bytes() {
        byte[] input = {1, 2, 3};
        byte[] encoded = Base64Util.encode(input, Base64Util.DEFAULT);
        assertEquals("AQID", new String(encoded).trim());
    }

    @Test
    void testEncodeWithNewlines() {
        byte[] input = new byte[57];
        for (int i = 0; i < 57; i++) input[i] = (byte) i;
        byte[] encoded = Base64Util.encode(input, Base64Util.CRLF);
        String output = new String(encoded);
        assertTrue(output.contains("\r\n"));
    }

    @Test
    void testDecoderExtraDataAfterPadding() {
        byte[] input = "TWE===".getBytes();
        assertThrows(IllegalArgumentException.class, () -> {
            Base64Util.decode(input, 0, input.length, Base64Util.DEFAULT);
        });
    }

    @Test
    void testDecoderSingleEqualsOnly() {
        byte[] input = "TWE=".getBytes();
        assertDoesNotThrow(() -> Base64Util.decode(input, 0, input.length, Base64Util.DEFAULT));
    }

    @Test
    void testEncodeWithOffsetAndLength() {
        byte[] input = "012HelloWorld".getBytes();
        byte[] result = Base64Util.encode(input, 3, 5, Base64Util.DEFAULT);
        assertEquals("SGVsbG8=", new String(result).trim());
    }

    @Test
    void testDecodeWithOffsetAndLength() {
        byte[] input = "prefixSGVsbG8=".getBytes();
        byte[] result = Base64Util.decode(input, 6, 8, Base64Util.DEFAULT);
        assertEquals("Hello", new String(result));
    }

    @Test
    void testEncodeWithCRLFAndPadding() {
        byte[] input = new byte[57];
        for (int i = 0; i < input.length; i++) input[i] = 'A';
        byte[] result = Base64Util.encode(input, Base64Util.CRLF);
        String encoded = new String(result);
        assertTrue(encoded.contains("\r\n"));
    }

    @Test
    void testEncoderTailHandlingThreeBytes() {
        byte[] input = "ABCD".getBytes();
        byte[] result = Base64Util.encode(input, Base64Util.NO_WRAP);
        assertNotNull(result);
    }

    @Test
    void testEncodeToString_withOffsetAndLength() {
        byte[] input = "HelloWorld".getBytes();
        String encoded = Base64Util.encodeToString(input, 5, 5, Base64Util.DEFAULT);
        assertEquals("V29ybGQ=", encoded.trim());
    }

    @Test
    void testEncodeWithCRLFNewlinesEvery76Chars() {
        byte[] input = new byte[114];
        for (int i = 0; i < input.length; i++) input[i] = 'A';
        byte[] encoded = Base64Util.encode(input, Base64Util.CRLF);
        String result = new String(encoded);
        assertTrue(result.contains("\r\n"));
    }

    @Test
    void testEncodeNoPaddingProducesNoEqualSigns() {
        byte[] input = {1, 2};
        byte[] encoded = Base64Util.encode(input, Base64Util.NO_PADDING | Base64Util.NO_WRAP);
        String result = new String(encoded);
        assertFalse(result.contains("="));
    }

    @Test
    void testEncodeNoWrap() {
        byte[] input = new byte[100];
        for (int i = 0; i < 100; i++) input[i] = 'A';
        byte[] encoded = Base64Util.encode(input, Base64Util.NO_WRAP);
        String result = new String(encoded);
        assertFalse(result.contains("\n"));
    }

    @Test
    void testEncodeUrlSafeContainsNoPlusOrSlash() {
        byte[] input = new byte[] {(byte) 0xfb, (byte) 0xef};
        byte[] encoded = Base64Util.encode(input, Base64Util.URL_SAFE);
        String result = new String(encoded);
        assertFalse(result.contains("+"));
        assertFalse(result.contains("/"));
    }

    @Test
    void testDecodeUrlSafeWithoutPadding() {
        String urlSafeInput = "U29tZVRleHQtV2l0aG91dFBhZGRpbmc";
        byte[] decoded = Base64Util.decode(urlSafeInput, Base64Util.URL_SAFE);
        assertEquals("SomeText-WithoutPadding", new String(decoded));
    }

    @Test
    void testEncodeWithOffsetLengthFinalPadding() {
        byte[] input = "startDataEnd".getBytes();
        byte[] encoded = Base64Util.encode(input, 5, 4, Base64Util.NO_WRAP);
        String encodedStr = new String(encoded);
        assertEquals("RGF0YQ==", encodedStr);
    }

    @Test
    void testDecodeDoublePadding() {
        byte[] decoded = Base64Util.decode("QQ==", Base64Util.DEFAULT);
        assertEquals("A", new String(decoded));
    }

    @Test
    void testDecodeWithCRLFInput() {
        String original = "SomeDataThatSpansMultipleLinesToTriggerCRLFEncodingInBase64";
        byte[] encoded = Base64Util.encode(original.getBytes(), Base64Util.CRLF);
        String base64WithCRLF = new String(encoded);
        assertTrue(base64WithCRLF.contains("\r\n"));
        byte[] decoded = Base64Util.decode(base64WithCRLF, Base64Util.DEFAULT);
        assertEquals(original, new String(decoded));
    }


    @Test
    void testDecodeFailsOnCharAfterPadding() {
        byte[] input = "QQ==A".getBytes();
        assertThrows(IllegalArgumentException.class, () ->
                Base64Util.decode(input, 0, input.length, Base64Util.DEFAULT)
        );
    }




}