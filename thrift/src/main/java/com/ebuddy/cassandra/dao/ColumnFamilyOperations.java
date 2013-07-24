package com.ebuddy.cassandra.dao;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.ebuddy.cassandra.BatchContext;
import com.ebuddy.cassandra.dao.mapper.ColumnFamilyRowMapper;
import com.ebuddy.cassandra.dao.mapper.ColumnMapper;

/**
 * Core Column Family operations.
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public interface ColumnFamilyOperations<K,N,V> {

    /**
     * Create a BatchContext for use with this keyspace.
     *
     * @return the BatchContext
     */
    BatchContext begin();

    /**
     * Execute a batch of mutations using a mutator.
     *
     * @param batchContext the BatchContext
     */
    void commit(@Nonnull BatchContext batchContext);

    V readColumnValue(K rowKey, N columnName);
    V readColumnValue(String columnFamily, K rowKey, N columnName);

    Map<N,V> readColumnsAsMap(K rowKey);
    Map<N,V> readColumnsAsMap(String columnFamily, K rowKey, N start, N finish, int count, boolean reversed);
    Map<N,V> readColumnsAsMap(K rowKey, N start, N finish, int count, boolean reversed);

    <T> List<T> readColumns(K rowKey, ColumnMapper<T,N,V> columnMapper);

    <T> List<T> readColumns(String columnFamily,
                            K rowKey,
                            N start,
                            N finish,
                            int count,
                            boolean reversed,
                            ColumnMapper<T,N,V> columnMapper);
    <T> List<T> readColumns(K rowKey, N start, N finish, int count, boolean reversed, ColumnMapper<T,N,V> columnMapper);

    Map<K,Map<N,V>> multiGetAsMap(Iterable<K> rowKeys);

    Map<K,Map<N,V>> multiGetColumnsAsMap(Iterable<K> rowKeys, N... columnNames);

    Map<K,Map<N,V>> readRowsAsMap(String columnFamily);
    Map<K,Map<N,V>> readRowsAsMap();

    <T> List<T> multiGet(Iterable<K> rowKeys, ColumnFamilyRowMapper<T,K,N,V> rowMapper);

    <T> List<T> multiGetColumns(Iterable<K> rowKeys, ColumnFamilyRowMapper<T,K,N,V> rowMapper, N... columnNames);

    void writeColumn(K rowKey, N columnName, V columnValue);

    void writeColumn(K rowKey, N columnName, V columnValue, long timeToLive, TimeUnit timeToLiveTimeUnit);

    void writeColumn(K rowKey, N columnName, V columnValue, @Nonnull BatchContext batchContext);

    void writeColumn(K rowKey,
                     N columnName,
                     V columnValue,
                     int timeToLive,
                     TimeUnit timeToLiveTimeUnit,
                     @Nonnull BatchContext batchContext);

    void writeColumns(String columnFamily, K rowKey, Map<N,V> map);
    void writeColumns(K rowKey, Map<N,V> map);

    void writeColumns(String columnFamily, K rowKey, Map<N,V> map, @Nonnull BatchContext batchContext);
    void writeColumns(K rowKey, Map<N,V> map, @Nonnull BatchContext batchContext);

    void deleteColumns(String columnFamily, K rowKey, N... columnNames);
    void deleteColumns(K rowKey, N... columnNames);

    void removeRow(K rowKey);

    void removeRow(K rowKey, BatchContext batchContext);

}
