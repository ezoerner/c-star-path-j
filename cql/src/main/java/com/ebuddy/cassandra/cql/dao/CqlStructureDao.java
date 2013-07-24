package com.ebuddy.cassandra.cql.dao;

import com.ebuddy.cassandra.BatchContext;
import com.ebuddy.cassandra.StructureDao;
import com.ebuddy.cassandra.TypeReference;

/**
 * Implementation of StructureDao for CQL.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class CqlStructureDao<K> implements StructureDao<K> {

    @Override
    public <T> T readFromPath(String columnFamily, K rowKey, String path, TypeReference<T> type) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void writeToPath(String columnFamily, K rowKey, String path, Object value) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void writeToPath(String columnFamily, K rowKey, String path, Object value, BatchContext batchContext) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
