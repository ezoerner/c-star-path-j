package com.ebuddy.cassandra;

import me.prettyprint.cassandra.serializers.BigIntegerSerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.Composite;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class CompositeSerializerFixTest {

    @Test(enabled=false)
    public void testFromByteBufferWithSingletonCompositeSerializer() throws Exception {
        Composite composite = new Composite("test", 42);
        CompositeSerializer ser = CompositeSerializer.get();
        ByteBuffer byteBuffer = ser.toByteBuffer(composite);

        assertEquals(composite, ser.fromByteBuffer(byteBuffer));
    }

    @Test
    public void testFromByteBufferWithConstructedSerializer() throws Exception {
        Composite composite = new Composite("test", 42);
        CompositeSerializer ser = new CompositeSerializerFix(StringSerializer.get(), BigIntegerSerializer.get());
        ByteBuffer byteBuffer = ser.toByteBuffer(composite);
        assertEquals(composite, ser.fromByteBuffer(byteBuffer));
    }
}
