package com.ebuddy.cassandra.property;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.type.TypeReference;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

/**
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class ListValue extends StructuredValue<List<String>> {
    private static final Logger LOG = Logger.getLogger(ListValue.class);

    /** header char, a unicode non-character, used to flag a binary encoded StructuredValue. */
    static final int HEADER_CHAR = '\uFFFE';
    /** utf-8 encoded bytes for HEADER_CHAR. */
    static final byte[] UTF8_HEADER_BYTES = {(byte)0xef, (byte)0xbf, (byte)0xbe};
    
    ListValue(@Nonnull List<String> value) {
        super(value);
    }

    ListValue(String jsonString) {
        super(jsonString);
    }

    @Override
    protected byte[] getUtf8HeaderBytes() {
        return UTF8_HEADER_BYTES;
    }

    @Override
    public boolean isList() {
        return true;
    }

    @Override
    public List<String> getValue() {
        if (value != null) {
            return value;
        }

        TypeReference<List<String>> ref = new TypeReference<List<String>>() { };
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
    public boolean isEmpty() {
        return value.isEmpty();
    }
}