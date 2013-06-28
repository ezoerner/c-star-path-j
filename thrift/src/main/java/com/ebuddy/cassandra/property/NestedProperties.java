package com.ebuddy.cassandra.property;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.Map;

/**
 * A PropertyValue implementation that stores a map of PropertyValues.
 * WARNING: NOT THREAD SAFE
 * 
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
// needs to be public so jackson can access getValue() by reflection
public final class NestedProperties extends StructuredValue<Map<String, PropertyValue<?>>> {
    private static final Logger LOG = Logger.getLogger(NestedProperties.class);

    /** header char, a unicode non-character, used to flag a binary encoded NestedProperties. */
    static final int HEADER_CHAR = '\uFFFF';
    /** utf-8 encoded bytes for HEADER_CHAR. */
    static final byte[] UTF8_HEADER_BYTES = {(byte)0xef, (byte)0xbf, (byte)0xbf};


    NestedProperties(Map<String, PropertyValue<?>> map) {
        super(map);
    }

    NestedProperties(String jsonString) {
        super(jsonString);
    }

    @Override
    public Map<String, PropertyValue<?>> getValue() {
        if (value != null) {
            return value;
        }

        TypeReference<Map<String, PropertyValue<?>>> ref = new TypeReference<Map<String, PropertyValue<?>>>() { };
        try {
            value = JSON_MAPPER.readValue(jsonString, ref);
            return value;
        } catch (JsonParseException e) {
            LOG.error("Unable to parse '" + jsonString + "', returning null", e);
            return null;
        } catch (IOException ioe) {
            throw new RuntimeException("unexpected on '" + jsonString + "'", ioe);
        }
    }

    @Override
    public boolean isNestedProperties() {
        return true;
    }

    @Override
    protected byte[] getUtf8HeaderBytes() {
        return UTF8_HEADER_BYTES;
    }

    @Override
    public boolean isEmpty() {
        return value.isEmpty();
    }
}
