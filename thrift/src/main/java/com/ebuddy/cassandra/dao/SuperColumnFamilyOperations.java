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

package com.ebuddy.cassandra.dao;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.ebuddy.cassandra.BatchContext;
import com.ebuddy.cassandra.dao.mapper.ColumnMapper;
import com.ebuddy.cassandra.dao.mapper.SuperColumnFamilyRowMapper;
import com.ebuddy.cassandra.dao.mapper.SuperColumnMapper;
import com.ebuddy.cassandra.dao.visitor.ColumnVisitor;

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
     * Create a BatchContext for use with this keyspace.
     *
     * @return the BatchContext
     */
    BatchContext begin();

    /**
     * Execute a batch of updates.
     *
     * @param batchContext the BatchContext
     */
    void commit(@Nonnull BatchContext batchContext);

    V readColumnValue(K rowKey, SN superColumnName, N columnName);

    Map<N,V> readColumnsAsMap(K rowKey, SN superColumnName, N... columnNames);

    <T> List<T> readColumns(K rowKey, SN superColumnName, ColumnMapper<T,N,V> columnMapper);

    <T> List<T> readColumns(K rowKey,
                            SN superColumnName,
                            N start,
                            N finish,
                            int count,
                            boolean reversed,
                            ColumnMapper<T, N, V> columnMapper);

    void visitColumns(K rowKey,
                         SN superColumnName,
                         N start,
                         N finish,
                         int count,
                         boolean reversed,
                         ColumnVisitor<N, V> columnVisitor);

    Map<K,Map<N,V>> multiGetAsMap(Collection<K> rowKeys, SN superColumnName);

    Map<K,Map<N,V>> multiGetColumnsAsMap(Collection<K> rowKeys, SN superColumnName, N... columnNames);

    <T> List<T> multiGetSuperColumn(Collection<K> rowKeys,
                                    SN superColumnName,
                                    SuperColumnMapper<T,K,SN,N,V> superColumnMapper);

    <T> List<T> multiGetAllSuperColumns(Collection<K> rowKeys,
                                        SuperColumnFamilyRowMapper<T,K,SN,N,V> superColumnFamilyRowMapper);

    <T> List<T> multiGetColumns(Collection<K> rowKeys,
                                SN superColumnName,
                                SuperColumnMapper<T,K,SN,N,V> superColumnMapper,
                                N... columnNames);

    Map<SN,Map<N,V>> readRowAsMap(K key);

    <T> List<T> readRow(K key, SuperColumnMapper<T,K,SN,N,V> superColumnMapper);

    void writeColumns(K rowKey, SN superColumnName, Map<N,V> columnMap);

    void writeColumns(K rowKey, SN superColumnName, Map<N,V> columnMap, @Nonnull BatchContext batchContext);

    void writeColumn(K rowKey, SN superColumnName, N columnName, V columnValue);

    void writeColumn(K rowKey, SN superColumnName, N columnName, V columnValue, @Nonnull BatchContext batchContext);

    void deleteColumns(K rowKey, SN superColumnName, Iterable<N> subcolumnNames);

    void deleteColumns(K rowKey,
                       SN superColumnName,
                       Iterable<N> subcolumnNames,
                       @Nonnull BatchContext batchContext);

    /**
     * Remove the named super column as part of a larger transaction.
     *
     * @param rowKey the row key
     * @param superColumnName the name of the super column being removed
     * @param batchContext the transactional context for batch operations.
     */
    void deleteSuperColumn(K rowKey, SN superColumnName, @Nonnull BatchContext batchContext);

    /**
     * Remove the named super column.
     *
     * @param rowKey the row key
     * @param superColumnName the name of the super column being removed
     */
    void deleteSuperColumn(K rowKey, SN superColumnName);

    void removeRow(K rowKey);

    void removeRow(K rowKey, BatchContext batchContext);
}
