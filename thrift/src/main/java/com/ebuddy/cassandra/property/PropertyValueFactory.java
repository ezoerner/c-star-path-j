package com.ebuddy.cassandra.property;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.DeserializerProvider;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.deser.StdDeserializer;
import org.codehaus.jackson.type.JavaType;
import org.codehaus.jackson.type.TypeReference;

import me.prettyprint.cassandra.serializers.StringSerializer;

/**
 * Factory for creating instances of PropertyValue implementations.
 * Currently supports String values, Maps, and Lists.
 * TODO: Get rid of PropertyValues. Although the idea was to make it more strongly typed than "Object"
 * TODO: to only allow certain types, in practice it complicates the API too much
 *
 * @deprecated just use Objects instead of PropertyValues
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
@SuppressWarnings({"UnusedDeclaration", "unchecked"})
@Deprecated
public class PropertyValueFactory extends StdDeserializer<PropertyValue<?>> {
    /**
     * The delimiter character for separating parts of a hierarchical property name.
     */
    public static final char DELIMITER = '|';
    /**
     * The delimiter character in a String for separating parts of a hierarchical property name.
     */
    public static final String DELIMITER_STRING = new String(new char[]{DELIMITER});

    /** Reusable PropertyValue containing the empty string. */
    public static final PropertyValue<String> EMPTY_STRING = StringValue.fromString("");

    /** Reusable PropertyValue containing a TRUE value as a string. */
    public static final PropertyValue<String> TRUE = StringValue.fromString("1");

    /** Reusable PropertyValue containing a FALSE value as a string. */
    public static final PropertyValue<String> FALSE = StringValue.fromString("0");

    private static final PropertyValueFactory INSTANCE = new PropertyValueFactory();

    private ObjectMapper mapper = new ObjectMapper();


    /**
     * Construct an instance. Intended to be a private static final singleton only.
     */
    private PropertyValueFactory() {
        super(PropertyValueFactory.class);
    }

    public static PropertyValueFactory get() {
        return INSTANCE;
    }

    /**
     * @deprecated use PropertyValue#createPropertyValue(String) instead
     */
    // !!ezoerner::20130503 This method is a bad idea.
    // The whole point of PropertyValue is to make it stronger typing than Object.
    // this method makes it possible to create property values from domain objects, for example,
    // and having them converted with toString would be astonishing to the caller.
    // I actually removed a method like this at one point and it was later recreated by someone else...
    @Deprecated
    public PropertyValue<?> createPropertyValue(Object value) {
        if (value instanceof List) {
            return createPropertyValue((List<String>)value);
        }
        if (value instanceof Map) {
            return createPropertyValue((Map<String,PropertyValue<?>>)value);
        }
        return value == null ? createPropertyValue("") : createPropertyValue(value.toString());
    }

    /**
     * Create a PropertyValue<String> given a String.
     * @param value the String value
     * @return the PropertyValue
     */
    public PropertyValue<String> createPropertyValue(String value) {
        return StringValue.fromString(value);
    }

    /**
     * Create a PropertyValue<Map> for a Map of PropertyValues keyed by strings.
     * @param map the Map value
     * @return the PropertyValue
     */
    public PropertyValue<Map<String, PropertyValue<?>>> createPropertyValue(Map<String, PropertyValue<?>> map) {
        return new NestedProperties(map);
    }

    /**
     * Create a PropertyValue for a List of property values.
     * @param value the raw value
     * @return the PropertyValue
     */
    public PropertyValue<List<String>> createPropertyValue(List<String> value) {
        return new ListValue(value);
    }

    /**
     * Create a PropertyValue based on raw bytes in the form of a ByteBuffer.
     * @param byteBuffer the raw bytes
     * @return the PropertyValue
     */
    public PropertyValue<?> createPropertyValue(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return EMPTY_STRING;
        }
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes, 0, bytes.length);
        return createPropertyValue(bytes);
    }


    /**
     * Create a property Value based on raw bytes in the form of a byte[].
     * @param storeBytes the raw bytes
     * @return the created PropertyValue
     */
    @SuppressWarnings({"MethodCanBeVariableArityMethod"})
    public PropertyValue<?> createPropertyValue(byte[] storeBytes) {
        // look for header char to determine if nested properties
        InputStream inputStream = new ByteArrayInputStream(storeBytes);
        InputStreamReader reader = new InputStreamReader(inputStream, Charset.forName("UTF-8"));
        try {
            int firstChar = reader.read();
            switch(firstChar) {
                case NestedProperties.HEADER_CHAR:
                    return new NestedProperties(new String(storeBytes,
                                                           NestedProperties.UTF8_HEADER_BYTES.length,
                                                           storeBytes.length - NestedProperties.UTF8_HEADER_BYTES.length,
                                                           Charset.forName("UTF-8")));
                case ListValue.HEADER_CHAR:
                    // BUG FIX: Some data was written by a previous version of the code that
                    // wrote the structured header bytes followed by double-quoted string.
                    // This should be interpreted as a string, not a list
                    return mapper.readValue(ArrayUtils.subarray(storeBytes,
                                                                ListValue.UTF8_HEADER_BYTES.length,
                                                                storeBytes.length),
                                            new TypeReference<PropertyValue<?>>() {
                                            });
/*
                    return new ListValue(new String(storeBytes,
                                                          ListValue.UTF8_HEADER_BYTES.length,
                                                          storeBytes.length - ListValue.UTF8_HEADER_BYTES.length,
                                                          Charset.forName("UTF-8")));
*/
                default:
                    return StringValue.fromString(StringSerializer.get().fromBytes(storeBytes));
            }
        } catch (IOException ioe) {
            //noinspection ProhibitedExceptionThrown
            throw new RuntimeException("unexpected", ioe);
        }
    }

    @Override
    public PropertyValue<?> deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        assert jp.hasCurrentToken();
        JsonToken t = jp.getCurrentToken();
        // currently we only support lists, maps, and string values
        switch (t) {
            case VALUE_STRING:
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:
                return StringValue.fromString(jp.getText());
            case START_OBJECT:
            {
                JavaType valueType = mapper.getTypeFactory().
                        constructType(new TypeReference<Map<String,PropertyValue<?>>>() { }.getType());
                DeserializerProvider provider = ctxt.getDeserializerProvider();
                JsonDeserializer<?> deser = provider.findTypedValueDeserializer(ctxt.getConfig(), valueType, null);
                if (deser == null) { // can this happen?
                    throw new JsonMappingException("Can not find a deserializer for type "+ valueType);
                }

                @SuppressWarnings({"unchecked"})
                Map<String, PropertyValue<?>> map = (Map<String, PropertyValue<?>>)deser.deserialize(jp, ctxt);
                
                return new NestedProperties(map);
            }
            case START_ARRAY:
            {
                JavaType valueType = mapper.getTypeFactory().
                        constructType(new TypeReference<List<String>>() { }.getType());
                DeserializerProvider provider = ctxt.getDeserializerProvider();
                JsonDeserializer<?> deser = provider.findTypedValueDeserializer(ctxt.getConfig(), valueType, null);
                if (deser == null) { // can this happen?
                    throw new JsonMappingException("Can not find a deserializer for type "+ valueType);
                }

                @SuppressWarnings({"unchecked"})
                List<String> list = (List<String>)deser.deserialize(jp, ctxt);
                
                return new ListValue(list);
            }
            default:
                throw ctxt.mappingException(PropertyValue.class);
        }
    }
}
