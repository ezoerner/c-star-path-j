/*
 * Copyright 2013 eBuddy B.V.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
