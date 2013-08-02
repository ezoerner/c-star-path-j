package com.ebuddy.cassandra.property;

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.serializers.StringSerializer;

/**
 * An implementation of PropertyValue that stores a String.
 * 
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
// needs to be public so jackson can access getValue() by reflection
@Deprecated
public final class StringValue implements PropertyValue<String> {
    private final String value;

    private StringValue(String value) {
        if (value == null) {
            throw new IllegalArgumentException("value should not be null");
        }
        this.value = value;
    }

    @Override
    public ByteBuffer toBytes() {
        return StringSerializer.get().toByteBuffer(value);
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public boolean isStructured() {
        return false;
    }

    @Override
    public boolean isList() {
        return false;
    }

    @Override
    public boolean isNestedProperties() {
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        //noinspection OverlyStrongTypeCast
        return obj instanceof StringValue && getValue().equals(((StringValue)obj).getValue());
    }

    @Override
    public int hashCode() {
        return getValue().hashCode();
    }

    static StringValue fromString(String value) {
        return new StringValue(value);
    }

    @Override
    public boolean isEmpty() {
        return value.isEmpty();
    }
}
