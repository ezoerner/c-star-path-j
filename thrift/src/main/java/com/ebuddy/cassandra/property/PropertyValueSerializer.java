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

package com.ebuddy.cassandra.property;

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;

/**
 * Serializer for PropertyValues.
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 * @deprecated use regular objects with a StructuredDataSupport instead of PropertyValues
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
