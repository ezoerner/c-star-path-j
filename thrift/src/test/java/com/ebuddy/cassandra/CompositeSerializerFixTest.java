package com.ebuddy.cassandra;

import static org.junit.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.nio.ByteBuffer;

import org.testng.annotations.Test;

import me.prettyprint.cassandra.serializers.BigIntegerSerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.Composite;

/**
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class CompositeSerializerFixTest {

    @Test(groups = "unit", description = "test that the singleton CompositeSerializer is broken")
    public void testFromByteBufferWithSingletonCompositeSerializer() throws Exception {
        Composite composite = new Composite("test", 42);
        CompositeSerializer ser = CompositeSerializer.get();
        ByteBuffer byteBuffer = ser.toByteBuffer(composite);

        assertNotEquals(composite, ser.fromByteBuffer(byteBuffer));
    }

    @Test(groups="unit")
    public void testFromByteBufferWithConstructedSerializer() throws Exception {
        Composite composite = new Composite("test", 42);
        CompositeSerializer ser = new CompositeSerializerFix(StringSerializer.get(), BigIntegerSerializer.get());
        ByteBuffer byteBuffer = ser.toByteBuffer(composite);
        assertEquals(composite, ser.fromByteBuffer(byteBuffer));
    }
}
