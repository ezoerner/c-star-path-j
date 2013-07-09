package com.ebuddy.cassandra.cql.dao;

import com.ebuddy.cassandra.StructureDao;
import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Implementation of StructureDao for CQL.
 *
 * It is possible to write data that will cause inconsistencies in on object structure
 * when it is reconstructed when read. The implementation will resolve inconsistencies as follows:
 *
 * If data objects are found at a particular path as well as longer paths, the data object
 * is returned in a map structure with the special key "@ROOT". This may cause an error
 * if the data is attempted to be deserialized into a POJO.
 *
 * If list elements are found at the same level as longer paths or a data object, then
 * the list elements are returned in a map with the index as keys in the map, e.g. "@0", "@1",
 * etc.
 *
 * If inconsistencies such as these are preventing data from being deserialized into a
 * particular POJO, the data can always be retrieved using an instance of TypeReference<Object>,
 * which will return the basic JSON to Java mappings, i.e. Maps, Lists and Strings, etc.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class CqlStructureDao<K> implements StructureDao<K> {

    @Override
    public <T> T readFromPath(K rowKey, String path, TypeReference<T> type) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void writeToPath(K rowKey, String path, Object value) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
