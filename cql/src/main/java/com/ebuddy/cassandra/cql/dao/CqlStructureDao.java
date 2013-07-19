package com.ebuddy.cassandra.cql.dao;

import com.ebuddy.cassandra.StructureDao;
import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Implementation of StructureDao for CQL.
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
