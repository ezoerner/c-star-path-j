package com.ebuddy.cassandra.structure;

import static org.apache.commons.lang3.ObjectUtils.NULL;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.commons.lang3.ArrayUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Serializer for legacy format of property values without using PropertyValue itself.
 * NOTE: Experimental, not sure yet if this will actually be used anywhere.
 * // TODO: integrate this into spring Converter framework
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class StructureConverter {
    private static final String UTF_8 = "UTF-8";
    private static final Charset UTF_8_CHARSET = Charset.forName(UTF_8);
    private static final StructureConverter INSTANCE = new StructureConverter();
    protected static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    /**
     * Header char, a unicode non-character, used to flag a JSON deserialized object.
     */
    private static final int HEADER_CHAR = '\uFFFE';
    /**
     * utf-8 encoded bytes for HEADER_CHAR.
     */
    private static final byte[] UTF8_HEADER_BYTES = {(byte)0xef, (byte)0xbf, (byte)0xbe};

    /**
     * Only instantiated once for the static singleton.
     */
    private StructureConverter() { }

    /**
     * Get the singleton instance of StructureConverter.
     *
     * @return the singleton StructureConverter
     */
    public static StructureConverter get() {
        return INSTANCE;
    }

    /**
     * @throws DataFormatException if format of the string is incorrect and could not be parsed as JSON
     */
    @SuppressWarnings("fallthrough")
    public Object fromString(String str) {
        if (str == null) {
            return null;
        }
        if (str.isEmpty()) {
            return str;
        }

        // look for header char to determine if a JSON object or legacy NestedProperties
        int firstChar = str.charAt(0);
        switch (firstChar) {
            case '\uFFFF':
                // legacy NestedProperties, obsolete and interpreted now as simply a JSON encoded Map
            case HEADER_CHAR:
                try {
                    return JSON_MAPPER.readValue(str.substring(1), Object.class);
                } catch (IOException e) {
                    throw new DataFormatException("Could not parse JSON", e);
                }
            default:
                // if no special header, then just a string
                return str;
        }
    }

    /**
     * @throws DataFormatException if the object could not be encoded as JSON
     */
    public String toString(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return (String)obj;
        }

        // write as special header bytes followed by JSON
        // intercept the Null token which stands in for a real null
        if (obj == NULL) {
            obj = null;
        }

        String jsonString = encodeJson(obj);
        char[] chars = new char[jsonString.length() + 1];
        chars[0] = HEADER_CHAR;
        jsonString.getChars(0,jsonString.length(),chars,1);
        return new String(chars);
    }

    /**
     * @throws DataFormatException if object cannot be encoded as JSON
     */
    public ByteBuffer toByteBuffer(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return ByteBuffer.wrap(((String)obj).getBytes(UTF_8_CHARSET));
        }

        // write as special header bytes followed by JSON
        // intercept the Null token which stands in for a real null
        if (obj == NULL) {
            obj = null;
        }
        String jsonString = encodeJson(obj);
        byte[] jsonBytes = jsonString.getBytes(UTF_8_CHARSET);
        byte[] result = new byte[jsonBytes.length + UTF8_HEADER_BYTES.length];
        System.arraycopy(UTF8_HEADER_BYTES, 0, result, 0, UTF8_HEADER_BYTES.length);
        System.arraycopy(jsonBytes, 0, result, UTF8_HEADER_BYTES.length, jsonBytes.length);
        return ByteBuffer.wrap(result);
    }

    /**
     * @throws DataFormatException is data in the byte buffer is incorrect and cannot be decoded
     */
    public Object fromByteBuffer(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes, 0, bytes.length);
        return decodeBytes(bytes);
    }

    @SuppressWarnings("fallthrough")
    private Object decodeBytes(byte[] bytes) {
        // look for header char to determine if a JSON object or legacy NestedProperties
        InputStream inputStream = new ByteArrayInputStream(bytes);
        InputStreamReader reader = new InputStreamReader(inputStream, UTF_8_CHARSET);
        try {
            int firstChar = reader.read();
            switch (firstChar) {
                case '\uFFFF':
                    // legacy NestedProperties, obsolete and interpreted now as simply a JSON encoded Map
                case HEADER_CHAR:
                    return JSON_MAPPER.readValue(ArrayUtils.subarray(bytes, UTF8_HEADER_BYTES.length, bytes.length),
                                                 Object.class);
                default:
                    // if no special header, then just a string
                    return UTF_8_CHARSET.decode(ByteBuffer.wrap(bytes)).toString();
            }
        } catch (IOException ioe) {
            throw new DataFormatException("Could not parse JSON", ioe);
        }
    }

    private String encodeJson(Object value) {
        String jsonString;
        try {
            jsonString = JSON_MAPPER.writeValueAsString(value);
            return jsonString;
        } catch (IOException ioe) {
            throw new DataFormatException("Could not encode object as JSON: class=" + value.getClass().getName(), ioe);
        }
    }
}
