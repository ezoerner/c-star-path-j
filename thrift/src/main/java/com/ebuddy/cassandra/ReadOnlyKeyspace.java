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

package com.ebuddy.cassandra;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.CounterSuperColumn;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SuperColumn;

import me.prettyprint.cassandra.service.BatchMutation;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.KeyspaceService;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.exceptions.HectorException;

/**
 * A Keyspace implementation that throws an UnsupportedOperation exception
 * on write operations.
 *
 * @author Eric Zoerner
 */
class ReadOnlyKeyspace implements KeyspaceService {
    private static final String READ_ONLY_ERROR_MESSAGE = "Can not write to this Keyspace because it is read-only";
    private final KeyspaceService ks;

    /**
     * Create a read-only Keyspace.
     * 
     * @param fullAccessKeyspace a keyspace that is not read-only
     */
    ReadOnlyKeyspace(KeyspaceService fullAccessKeyspace) {
        ks = fullAccessKeyspace;
    }

    @Override
    public Column getColumn(ByteBuffer key, ColumnPath columnPath) throws HectorException {
        return ks.getColumn(key, columnPath);
    }

    @Override
    public Column getColumn(String key, ColumnPath columnPath) throws HectorException {
        return ks.getColumn(key, columnPath);
    }

    @Override
    public CounterColumn getCounter(ByteBuffer key, ColumnPath columnPath) throws HectorException {
        return ks.getCounter(key, columnPath);
    }

    @Override
    public CounterColumn getCounter(String key, ColumnPath columnPath) throws HectorException {
        return ks.getCounter(key, columnPath);
    }

    @Override
    public SuperColumn getSuperColumn(ByteBuffer key, ColumnPath columnPath) throws HectorException {
        return ks.getSuperColumn(key, columnPath);
    }

    @Override
    public SuperColumn getSuperColumn(String key, ColumnPath columnPath) throws HectorException {
        return ks.getSuperColumn(key, columnPath);
    }

    @Override
    public SuperColumn getSuperColumn(ByteBuffer key, ColumnPath columnPath, boolean reversed, int size)
            throws HectorException {
        return ks.getSuperColumn(key, columnPath, reversed, size);
    }

    @Override
    public List<Column> getSlice(ByteBuffer key, ColumnParent columnParent, SlicePredicate predicate)
            throws HectorException {
        return ks.getSlice(key, columnParent, predicate);
    }

    @Override
    public List<Column> getSlice(String key, ColumnParent columnParent, SlicePredicate predicate)
            throws HectorException {
        return ks.getSlice(key, columnParent, predicate);
    }

    @Override
    public List<CounterColumn> getCounterSlice(ByteBuffer key, ColumnParent columnParent, SlicePredicate predicate)
            throws HectorException {
        return ks.getCounterSlice(key, columnParent, predicate);
    }

    @Override
    public List<CounterColumn> getCounterSlice(String key, ColumnParent columnParent, SlicePredicate predicate)
            throws HectorException {
        return ks.getCounterSlice(key, columnParent, predicate);
    }

    @Override
    public List<SuperColumn> getSuperSlice(ByteBuffer key, ColumnParent columnParent, SlicePredicate predicate)
            throws HectorException {
        return ks.getSuperSlice(key, columnParent, predicate);
    }

    @Override
    public List<SuperColumn> getSuperSlice(String key, ColumnParent columnParent, SlicePredicate predicate)
            throws HectorException {
        return ks.getSuperSlice(key, columnParent, predicate);
    }

    @Override
    public List<CounterSuperColumn> getCounterSuperSlice(ByteBuffer key,
                                                         ColumnParent columnParent,
                                                         SlicePredicate predicate) throws HectorException {
        return ks.getCounterSuperSlice(key, columnParent, predicate);
    }

    @Override
    public List<CounterSuperColumn> getCounterSuperSlice(String key,
                                                         ColumnParent columnParent,
                                                         SlicePredicate predicate) throws HectorException {
        return ks.getCounterSuperSlice(key, columnParent, predicate);
    }

    @Override
    public Map<ByteBuffer, SuperColumn> multigetSuperColumn(List<ByteBuffer> keys, ColumnPath columnPath)
            throws HectorException {
        return ks.multigetSuperColumn(keys, columnPath);
    }

    @Override
    public Map<ByteBuffer, SuperColumn> multigetSuperColumn(List<ByteBuffer> keys,
                                                            ColumnPath columnPath,
                                                            boolean reversed,
                                                            int size) throws HectorException {
        return ks.multigetSuperColumn(keys, columnPath, reversed, size);
    }

    @Override
    public Map<ByteBuffer, List<Column>> multigetSlice(List<ByteBuffer> keys,
                                                       ColumnParent columnParent,
                                                       SlicePredicate predicate) throws HectorException {
        return ks.multigetSlice(keys, columnParent, predicate);
    }

    @Override
    public Map<ByteBuffer,List<CounterColumn>> multigetCounterSlice(List<ByteBuffer> keys,
                                                                    ColumnParent columnParent,
                                                                    SlicePredicate predicate) throws HectorException {
        return ks.multigetCounterSlice(keys, columnParent, predicate);
    }

    @Override
    public Map<ByteBuffer, List<SuperColumn>> multigetSuperSlice(List<ByteBuffer> keys,
                                                                 ColumnParent columnParent,
                                                                 SlicePredicate predicate) throws HectorException {
        return ks.multigetSuperSlice(keys, columnParent, predicate);
    }

    @Override
    public Map<ByteBuffer,List<CounterSuperColumn>> multigetCounterSuperSlice(List<ByteBuffer> keys,
                                                                              ColumnParent columnParent,
                                                                              SlicePredicate predicate)
            throws HectorException {
        return ks.multigetCounterSuperSlice(keys, columnParent, predicate);
    }

    @Override
    public void insert(ByteBuffer key, ColumnParent columnParent, Column column) throws HectorException {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MESSAGE);
    }

    @Override
    public void insert(String key, ColumnPath columnPath, ByteBuffer value) throws HectorException {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MESSAGE);
    }

    @Override
    public void insert(String key, ColumnPath columnPath, ByteBuffer value, long timestamp) throws HectorException {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MESSAGE);
    }

    @Override
    public void addCounter(ByteBuffer key, ColumnParent columnParent, CounterColumn counterColumn)
            throws HectorException {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MESSAGE);
    }

    @Override
    public void addCounter(String key, ColumnParent columnParent, CounterColumn counterColumn) throws HectorException {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MESSAGE);
    }

    @Override
    public void batchMutate(Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap) throws HectorException {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MESSAGE);
    }

    @SuppressWarnings({"RawUseOfParameterizedType"})
    @Override
    public void batchMutate(BatchMutation batchMutation) throws HectorException {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MESSAGE);
    }

    @Override
    public void remove(ByteBuffer key, ColumnPath columnPath) {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MESSAGE);
    }

    @Override
    public void remove(ByteBuffer key, ColumnPath columnPath, long timestamp) throws HectorException {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MESSAGE);
    }

    @Override
    public void remove(String key, ColumnPath columnPath) throws HectorException {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MESSAGE);
    }

    @Override
    public void remove(String key, ColumnPath columnPath, long timestamp) throws HectorException {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MESSAGE);
    }

    @Override
    public void removeCounter(ByteBuffer key, ColumnPath columnPath) throws HectorException {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MESSAGE);
    }

    @Override
    public void removeCounter(String key, ColumnPath columnPath) throws HectorException {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MESSAGE);
    }

    @Override
    public int getCount(ByteBuffer key, ColumnParent columnParent, SlicePredicate predicate) throws HectorException {
        return ks.getCount(key, columnParent, predicate);
    }

    @Override
    public Map<ByteBuffer, List<Column>> getRangeSlices(ColumnParent columnParent,
                                                        SlicePredicate predicate,
                                                        KeyRange keyRange) throws HectorException {
        return ks.getRangeSlices(columnParent, predicate, keyRange);
    }

    @Override
    public Map<ByteBuffer,List<CounterColumn>> getRangeCounterSlices(ColumnParent columnParent,
                                                                     SlicePredicate predicate,
                                                                     KeyRange keyRange) throws HectorException {
        return ks.getRangeCounterSlices(columnParent, predicate, keyRange);
    }

    @Override
    public Map<ByteBuffer, List<SuperColumn>> getSuperRangeSlices(ColumnParent columnParent,
                                                                  SlicePredicate predicate,
                                                                  KeyRange keyRange) throws HectorException {
        return ks.getSuperRangeSlices(columnParent, predicate, keyRange);
    }

    @Override
    public Map<ByteBuffer,List<CounterSuperColumn>> getSuperRangeCounterSlices(ColumnParent columnParent,
                                                                               SlicePredicate predicate,
                                                                               KeyRange keyRange) {
        return ks.getSuperRangeCounterSlices(columnParent, predicate, keyRange);
    }

    @Override
    public Map<ByteBuffer, List<Column>> getIndexedSlices(ColumnParent columnParent,
                                                          IndexClause indexClause,
                                                          SlicePredicate predicate) throws HectorException {
        return ks.getIndexedSlices(columnParent, indexClause, predicate);
    }

    @Override
    public Map<ByteBuffer, Integer> multigetCount(List<ByteBuffer> keys,
                                                  ColumnParent columnParent,
                                                  SlicePredicate slicePredicate) throws HectorException {
        return ks.multigetCount(keys, columnParent, slicePredicate);
    }

    @Override
    public HConsistencyLevel getConsistencyLevel(OperationType operationType) {
        return ks.getConsistencyLevel(operationType);
    }

    @Override
    public String getName() {
        return ks.getName();
    }

    @Override
    public CassandraHost getCassandraHost() {
        return ks.getCassandraHost();
    }
}
