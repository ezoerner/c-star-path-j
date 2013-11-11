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

package com.ebuddy.cassandra.dao;

import java.nio.ByteBuffer;

import com.ebuddy.cassandra.structure.StructureConverter;

import me.prettyprint.cassandra.serializers.AbstractSerializer;

/**
 * Serializer that uses the StructureConverter to serialize structured data.


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
