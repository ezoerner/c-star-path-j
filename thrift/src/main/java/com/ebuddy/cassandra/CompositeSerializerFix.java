package com.ebuddy.cassandra;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.Composite;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
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
