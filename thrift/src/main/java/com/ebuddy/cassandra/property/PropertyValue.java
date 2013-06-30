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
