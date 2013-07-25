package com.ebuddy.cassandra.dao;

import static org.apache.commons.lang3.ObjectUtils.NULL;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.commons.lang3.ArrayUtils;
import org.codehaus.jackson.map.ObjectMapper;

import com.ebuddy.cassandra.UncategorizedCassandraException;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;

/**
 * Serializer for legacy format of property values without using PropertyValue itself.
 * NOTE: Experimental, not sure yet if this will actually be used anywhere.
 *
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class StructureSerializer extends AbstractSerializer<Object> {
    private static final StructureSerializer INSTANCE = new StructureSerializer();
    protected static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    /**
     * header char, a unicode non-character, used to flag a JSON deserialized object
     */
    private static final int HEADER_CHAR = '\uFFFE';
    /**
     * utf-8 encoded bytes for HEADER_CHAR.
     */
    private static final byte[] UTF8_HEADER_BYTES = {(byte)0xef, (byte)0xbf, (byte)0xbe};

    /**
     * Only instantiated once for the static singleton.
     */
    private StructureSerializer() {
    }

    /**
     * Get the singleton instance of PropertyValueSerializer.
     *
     * @return the singleton PropertyValueSerializer
     */
    public static StructureSerializer get() {
        return INSTANCE;
    }

    @Override
    public ByteBuffer toByteBuffer(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return StringSerializer.get().toByteBuffer((String)obj);
        }

        // write as special header bytes followed by JSON
        // intercept the Null token which stands in for a real null
        if (obj == NULL) {
            obj = null;
        }
        String jsonString = encodeJson(obj);
        byte[] jsonBytes = jsonString.getBytes(Charset.forName("UTF-8"));
        byte[] result = new byte[jsonBytes.length + UTF8_HEADER_BYTES.length];
        System.arraycopy(UTF8_HEADER_BYTES, 0, result, 0, UTF8_HEADER_BYTES.length);
        System.arraycopy(jsonBytes, 0, result, UTF8_HEADER_BYTES.length, jsonBytes.length);
        return ByteBuffer.wrap(result);
    }

    @Override
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
        InputStreamReader reader = new InputStreamReader(inputStream, Charset.forName("UTF-8"));
        try {
            int firstChar = reader.read();
            switch (firstChar) {
                case '\uFFFF':
                    // legacy NestedProperties, obsolete and interpreted now as simply a JSON encoded Map
                case HEADER_CHAR:
                    return JSON_MAPPER.readValue(ArrayUtils.subarray(bytes,
                                                                     UTF8_HEADER_BYTES.length,
                                                                     bytes.length), Object.class);
                default:
                    // if no special header, then just a string
                    return StringSerializer.get().fromBytes(bytes);
            }
        } catch (IOException ioe) {
            throw new UncategorizedCassandraException("Could not", ioe);
        }
    }

    private String encodeJson(Object value) {
        String jsonString;
        try {
            jsonString = JSON_MAPPER.writeValueAsString(value);
            return jsonString;
        } catch (IOException ioe) {
            throw new UncategorizedCassandraException("Could not encode object as JSON: class=" + value.getClass()
                    .getName(),
                                                      ioe);
        }
    }
}
