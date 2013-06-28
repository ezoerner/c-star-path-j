package com.ebuddy.cassandra.dao;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.ebuddy.cassandra.dao.mapper.ColumnMapper;
import com.ebuddy.cassandra.dao.mapper.SuperColumnMapper;

/**
 * Operations for a super column family.
 * @param <K>  the Row Key type.
 * @param <SN> The supercolumn name type.
 * @param <N>  The subcolumn name type.
 * @param <V>  The column value type.

 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public interface SuperColumnFamilyOperations<K,SN,N,V>  {

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

    V readColumnValue(K rowKey, SN superColumnName, N columnName);

    Map<N,V> readColumnsAsMap(K rowKey, SN superColumnName, N... columnNames);

    <T> List<T> readColumns(K rowKey, SN superColumnName, ColumnMapper<T,N,V> columnMapper);

    <T> List<T> readColumns(K rowKey,
                            SN superColumnName,
                            N start,
                            N finish,
                            int count,
                            boolean reversed,
                            ColumnMapper<T,N,V> columnMapper);

    Map<K,Map<N,V>> multiGetAsMap(Collection<K> rowKeys, SN superColumnName);

    Map<K,Map<N,V>> multiGetColumnsAsMap(Collection<K> rowKeys, SN superColumnName, N... columnNames);

    <T> List<T> multiGetSuperColumn(Collection<K> rowKeys,
                                    SN superColumnName,
                                    SuperColumnMapper<T,K,SN,N,V> superColumnMapper);

    <T> List<T> multiGetColumns(Collection<K> rowKeys,
                                SN superColumnName,
                                SuperColumnMapper<T,K,SN,N,V> superColumnMapper,
                                N... columnNames);

    Map<SN,Map<N,V>> readRowAsMap(K key);

    <T> List<T> readRow(K key, SuperColumnMapper<T,K,SN,N,V> superColumnMapper);

    void writeColumns(K rowKey, SN superColumnName, Map<N,V> columnMap);

    void writeColumns(K rowKey, SN superColumnName, Map<N,V> columnMap, @Nonnull TransactionContext txnContext);

    void writeColumn(K rowKey, SN superColumnName, N columnName, V columnValue);

    void writeColumn(K rowKey, SN superColumnName, N columnName, V columnValue, @Nonnull TransactionContext txnContext);

    void deleteColumns(K rowKey, SN superColumnName, Iterable<N> subcolumnNames);

    void deleteColumns(K rowKey,
                       SN superColumnName,
                       Iterable<N> subcolumnNames,
                       @Nonnull TransactionContext txnContext);

    /**
     * Remove the named super column as part of a larger transaction.
     *
     * @param rowKey the row key
     * @param superColumnName the name of the super column being removed
     * @param txnContext the transactional context for batch operations.
     */
    void deleteSuperColumn(K rowKey, SN superColumnName, @Nonnull TransactionContext txnContext);

    /**
     * Remove the named super column.
     *
     * @param rowKey the row key
     * @param superColumnName the name of the super column being removed
     */
    void deleteSuperColumn(K rowKey, SN superColumnName);

    void removeRow(K rowKey);

    void removeRow(K rowKey, TransactionContext txnContext);
}
