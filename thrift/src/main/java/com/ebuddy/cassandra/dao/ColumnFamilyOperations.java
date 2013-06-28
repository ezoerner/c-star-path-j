package com.ebuddy.cassandra.dao;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.ebuddy.cassandra.dao.mapper.ColumnFamilyRowMapper;
import com.ebuddy.cassandra.dao.mapper.ColumnMapper;

/**
 * Core Column Family operations.
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public interface ColumnFamilyOperations<K,N,V> {

    /**
     * Create a TransactionContext for use with this keyspace.
     *
     * @return the TransactionContext
     */
    TransactionContext begin();

    /**
     * Execute a batch of mutations using a mutator.
     *
     * @param transactionContext the TransactionContext
     */
    void commit(@Nonnull TransactionContext transactionContext);

    V readColumnValue(K rowKey, N columnName);

    Map<N,V> readColumnsAsMap(K rowKey);

    <T> List<T> readColumns(K rowKey, ColumnMapper<T,N,V> columnMapper);

    <T> List<T> readColumns(K rowKey, N start, N finish, int count, boolean reversed, ColumnMapper<T,N,V> columnMapper);

    Map<K,Map<N,V>> multiGetAsMap(Iterable<K> rowKeys);

    Map<K,Map<N,V>> multiGetColumnsAsMap(Iterable<K> rowKeys, N... columnNames);

    Map<K,Map<N,V>> readRowsAsMap();

    <T> List<T> multiGet(Iterable<K> rowKeys, ColumnFamilyRowMapper<T,K,N,V> rowMapper);

    <T> List<T> multiGetColumns(Iterable<K> rowKeys, ColumnFamilyRowMapper<T,K,N,V> rowMapper, N... columnNames);

    void writeColumn(K rowKey, N columnName, V columnValue);

    void writeColumn(K rowKey, N columnName, V columnValue, long timeToLive, TimeUnit timeToLiveTimeUnit);

    void writeColumn(K rowKey, N columnName, V columnValue, @Nonnull TransactionContext txnContext);

    void writeColumn(K rowKey,
                     N columnName,
                     V columnValue,
                     int timeToLive,
                     TimeUnit timeToLiveTimeUnit,
                     @Nonnull TransactionContext txnContext);

    void writeColumns(K rowKey, Map<N,V> map);

    void writeColumns(K rowKey, Map<N,V> map, @Nonnull TransactionContext txnContext);

    void deleteColumns(K rowKey, N... columnNames);

    void removeRow(K rowKey);

    void removeRow(K rowKey, TransactionContext txnContext);
}
