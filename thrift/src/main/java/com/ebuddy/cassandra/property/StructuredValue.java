package com.ebuddy.cassandra.property;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.commons.lang3.ObjectUtils;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.JsonSerializableWithType;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.TypeSerializer;

/**
 * An implementation of PropertyValue that stores a structured object as JSON.
 * It is accessed using simple data binding from Jackson,
 * i.e. Java Maps, Lists, Strings, Numbers, booleans
 *
 * NOTE: For a direct instance of StructuredValue, T must be a class that jackson simple binding supports.
 * Subclasses can support other types, but must then override the {@link #getValue} method with the
 * appropriate implementation.
 * 
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
// needs to be public so jackson can access getValue() by reflection
@Deprecated
public abstract class StructuredValue<T> implements PropertyValue<T>, JsonSerializableWithType {
    protected static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    protected T value;
    // value is lazily decoded from the underlying JSON
    protected String jsonString;

    StructuredValue(T value) {
        if (value == null) {
            throw new IllegalArgumentException("value should not be null");
        }
        this.value = value;
    }

    StructuredValue(String jsonString) {
        if (jsonString == null) {
            throw new IllegalArgumentException("jsonString should not be null");
        }
        this.jsonString = jsonString;
    }

    @Override
    public final String toString() {
        return String.valueOf(getValue());
    }

    @Override
    public final boolean isStructured() {
        return true;
    }

    @Override
    public boolean isNestedProperties() {
        return false;
    }

    @Override
    public boolean isList() {
        return false;
    }

    @Override
    public final ByteBuffer toBytes() {
        String jString = encode();
        byte[] headerBytes = getUtf8HeaderBytes();
        byte[] jsonBytes = jString.getBytes(Charset.forName("UTF-8"));
        byte[] result = new byte[jsonBytes.length + headerBytes.length];
        System.arraycopy(headerBytes, 0, result, 0,  headerBytes.length);
        System.arraycopy(jsonBytes, 0, result, headerBytes.length, jsonBytes.length);
        return ByteBuffer.wrap(result);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof StructuredValue)) {
            return false;
        }
        Object value = getValue();
        Object otherValue = ((PropertyValue<?>)obj).getValue();
        return ObjectUtils.equals(value, otherValue);
    }

    @Override
    public int hashCode() {
        Object value = getValue();
        if (value == null) {
            return System.identityHashCode(null);
        }

        return getValue().hashCode();
    }

    @Override
    public void serializeWithType(JsonGenerator jgen, SerializerProvider provider, TypeSerializer typeSer)
            throws IOException {
        jgen.writeRawValue(encode());
    }

    @Override
    public void serialize(JsonGenerator jgen, SerializerProvider provider) throws IOException {
        jgen.writeRawValue(encode());
    }

    protected abstract byte[] getUtf8HeaderBytes();

    private String encode() {
        if (jsonString != null) {
            return jsonString;
        }

        try {
            jsonString = JSON_MAPPER.writeValueAsString(value);
            return jsonString;
        } catch (IOException ioe) {
            // expected to never happen
            throw new RuntimeException("unexpected exception", ioe);
        }
    }
    
}
