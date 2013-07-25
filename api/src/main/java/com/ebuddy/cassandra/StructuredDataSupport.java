package com.ebuddy.cassandra;

/**
 * Provides data access for structured objects in Cassandra.
 * Objects are read using a hierarchical path that must contain at least one path element.
 * When reading values, they will be constructed as the specified type in a TypeReference.
 * Any object that can be converted to JSON can be written to a path.
 *
 * Paths are delimited by forward slash and in general each element should be URLEncoded
 * by the caller. The URL encoding allows each path element to contain vertical bars and other special
 * characters used by the implementation (currently only '@'). It also allows support for
 * encoding maps that have special characters in the keys.
 *
 * Paths that refer to list elements use a special notation, the '@' symbol followed by
 * the element index within the list (as a string).
 *
 * @param <K> The row key type
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public interface StructuredDataSupport<K> {

    <T> T readFromPath(String columnFamily, K rowKey, String path, TypeReference<T> type);

    void writeToPath(String columnFamily, K rowKey, String path, Object value);

    void writeToPath(String columnFamily, K rowKey, String path, Object value, BatchContext batchContext);
}
