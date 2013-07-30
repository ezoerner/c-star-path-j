package com.ebuddy.cassandra.dao;

import java.nio.ByteBuffer;

import com.ebuddy.cassandra.structure.StructureConverter;

import me.prettyprint.cassandra.serializers.AbstractSerializer;

/**
 * Serializer for legacy format of property values without using PropertyValue itself.


 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class StructureSerializer extends AbstractSerializer<Object> {
    private static final StructureSerializer INSTANCE = new StructureSerializer();

    /**
     * Only instantiated once for the static singleton.
     */
    private StructureSerializer() { }

    /**
     * Get the singleton instance of StructureSerializer.
     *
     * @return the singleton StructureSerializer
     */
    public static StructureSerializer get() {
        return INSTANCE;
    }

    @Override
    public ByteBuffer toByteBuffer(Object obj) {
        return StructureConverter.get().toByteBuffer(obj);
    }

    @Override
    public Object fromByteBuffer(ByteBuffer byteBuffer) {
        return StructureConverter.get().fromByteBuffer(byteBuffer);
    }
}
