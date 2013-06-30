package com.ebuddy.cassandra.property;

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;

/**
 * Serializer for PropertyValues.
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 * @deprecated use regular objects with a StructureDao instead of PropertyValues
 */
@Deprecated
public class PropertyValueSerializer extends AbstractSerializer<PropertyValue<?>> {
    private static final PropertyValueSerializer INSTANCE = new PropertyValueSerializer();

    /**
     * Only instantiated once for the static singleton.
     */
    private PropertyValueSerializer() { }

    /**
     * Get the singleton instance of PropertyValueSerializer.
     * @return the singleton PropertyValueSerializer
     */
    public static PropertyValueSerializer get() {
      return INSTANCE;
    }

    @Override
    public ByteBuffer toByteBuffer(PropertyValue<?> obj) {
        return obj.toBytes();
    }

    @Override
    public PropertyValue<?> fromByteBuffer(ByteBuffer byteBuffer) {
        return PropertyValueFactory.get().createPropertyValue(byteBuffer);
    }
}
