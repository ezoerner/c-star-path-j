package com.ebuddy.cassandra;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.Composite;

/**
 * This is a fix for the CompositeSerializer in the hector library.
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public final class CompositeSerializerFix extends CompositeSerializer {
    private final List<Serializer<?>> serializersByPosition;

    public CompositeSerializerFix(Serializer<?>... serializers) {
        serializersByPosition = Arrays.asList(serializers);
    }

    @Override
    public Composite fromByteBuffer(ByteBuffer byteBuffer) {
        Composite composite = createComposite();
        composite.deserialize(byteBuffer);
        return composite;
    }

    private Composite createComposite() {
        Composite composite = new Composite();
        composite.setSerializersByPosition(serializersByPosition);
        return composite;
    }
}
