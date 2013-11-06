package com.ebuddy.cassandra.dao.mapper;

/**
 * Maps columns into a parameterized type.
 *
 * @param <T> The result type.
 * @param <N> The type of a column name.
 * @param <V> The type of a column value.
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public interface ColumnMapperWithTimestamps<T,N,V> {

    T mapColumn(N columnName, V columnValue, long timestamp);
}
