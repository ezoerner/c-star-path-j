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

import org.codehaus.jackson.annotate.JsonValue;
import org.codehaus.jackson.map.annotate.JsonDeserialize;

/**
 * An immutable value that is either a string or a structured object (e.g. a map of maps), and can be converted
 * to bytes and back for storage. Used to implement nested properties, but can potentially also be used for other
 * structured types, e.g. lists or UUIDs.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
@Deprecated
@JsonDeserialize(using=PropertyValueFactory.class)
public interface PropertyValue<T> {
    
    /**
     * Convert to a byte array for external storage.
     * @return a byte array
     */
    ByteBuffer toBytes();

    /**
     * Return the value.
     * @return the value object
     */
    @JsonValue
    T getValue();

    /**
     * Return true if this property value is anything other than a simple string value.
     * @return true if this is a structured object
     */
    boolean isStructured();

    /**
     * Return true if this property value is a list.
     * @return true if this property value is a list
     */
    boolean isList();

    /**
     * Return true if this property value is a map.
     * @return true if nestable
     */
    boolean isNestedProperties();

    /**
     * Return true if the value of this property is empty.
     * @return true if value is empty
     */
    boolean isEmpty();
}
