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
