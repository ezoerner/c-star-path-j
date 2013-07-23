package com.ebuddy.cassandra.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.Validate;
import org.apache.log4j.Logger;

import com.ebuddy.cassandra.BatchContext;
import com.ebuddy.cassandra.dao.mapper.ColumnFamilyRowMapper;
import com.ebuddy.cassandra.dao.mapper.ColumnMapper;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * A Data access template for accessing a (regular) Column Family.
 *
 * @param <K> the type of the row keys
 * @param <N> the type of the column names
 * @param <V> the type of the column values
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public final class ColumnFamilyTemplate<K,N,V> extends AbstractColumnFamilyTemplate<K,N,V>
        implements ColumnFamilyOperations<K,N,V> {
    private static final Logger LOG = Logger.getLogger(ColumnFamilyTemplate.class);


    public ColumnFamilyTemplate(Keyspace keyspace,
                                String columnFamily,
                                Serializer<K> keySerializer,
                                Serializer<N> topSerializer,
                                Serializer<V> valueSerializer) {
        super(keyspace, columnFamily, keySerializer, topSerializer, valueSerializer);
    }


    /**
     * Read a column value.
     *
     * @param rowKey     the row key of type K
     * @param columnName the column name
     * @return the column value or null if not found
     */
    @Override
    public V readColumnValue(K rowKey, N columnName) {
        try {
            ColumnQuery<K,N,V> query = HFactory.createColumnQuery(keyspace,
                                                                  keySerializer,
                                                                  topSerializer,
                                                                  valueSerializer);
            QueryResult<HColumn<N,V>> result = query.
                    setKey(rowKey).
                    setColumnFamily(columnFamily).
                    setName(columnName).
                    execute();
            HColumn<N,V> column = result.get();
            return column != null ? column.getValue() : null;
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
    }

    /**
     * Read the columns as a map from a single row.
     *
     * @param rowKey the row key of type K
     * @return sorted map of columns, key is type N and values are type V.
     */
    @Override
    public Map<N,V> readColumnsAsMap(K rowKey) {
        return readColumnsAsMap(rowKey, null, null, ALL, false);
    }

    /**
     * Read the columns as a map from a single row specifying start, finish, count, and reversed.
     *
     *
     * @param rowKey the row key of type K
     * @return map of columns, key is type N and values are type V.
     */
    @Override
    public Map<N,V> readColumnsAsMap(K rowKey, N start, N finish, int count, boolean reversed) {
        Map<N,V> maps = new HashMap<N,V>();
        try {
            SliceQuery<K,N,V> query = HFactory.createSliceQuery(keyspace,
                                                                keySerializer,
                                                                topSerializer,
                                                                valueSerializer);
            QueryResult<ColumnSlice<N,V>> result = query.setKey(rowKey).
                    setColumnFamily(columnFamily).
                    setRange(start, finish, reversed, count).
                    execute();
            ColumnSlice<N,V> slice = result.get();

            for (HColumn<N,V> column : slice.getColumns()) {
                maps.put(column.getName(),
                         column.getValue());
            }
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
        return maps;
    }

    @Override
    public <T> List<T> readColumns(K rowKey, ColumnMapper<T,N,V> columnMapper) {
        return readColumns(rowKey, null, null, ALL, false, columnMapper);
    }

    @Override
    public <T> List<T> readColumns(K rowKey,
                                   N start,
                                   N finish,
                                   int count,
                                   boolean reversed,
                                   ColumnMapper<T,N,V> columnMapper) {
        List<T> resultList = new ArrayList<T>();
        try {
            SliceQuery<K,N,V> query = HFactory.createSliceQuery(keyspace,
                                                                keySerializer,
                                                                topSerializer,
                                                                valueSerializer);
            QueryResult<ColumnSlice<N,V>> result = query.setKey(rowKey).
                    setColumnFamily(columnFamily).
                    setRange(start, finish, reversed, count).
                    execute();
            ColumnSlice<N,V> slice = result.get();

            for (HColumn<N,V> column : slice.getColumns()) {
                try {
                    resultList.add(columnMapper.mapColumn(column.getName(), column.getValue()));
                } catch (RuntimeException e) {
                    LOG.error("Error while deserializing, skipping column", e);
                }
            }
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
        return resultList;
    }

    /**
     * Read all columns from multiple rows.
     *
     * @param rowKeys a collection of rows to read
     */
    @Override
    public Map<K,Map<N,V>> multiGetAsMap(Iterable<K> rowKeys) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Calling multiGetAsMap with rowKeys: " + rowKeys);
        }
        return basicMultiGetAsMap(rowKeys, null);
    }

    /**
     * Read specific columns from multiple rows.
     *
     * @param rowKeys     a collection of rows to read
     * @param columnNames names of the columns; if no columns are passed in then
     *                    just the keys are returned with no column data
     */
    @Override
    public Map<K,Map<N,V>> multiGetColumnsAsMap(Iterable<K> rowKeys, N... columnNames) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Calling multiGetColumnsAsMap with rowKeys: " + rowKeys
                              + " and columnNames: " + Arrays.asList(columnNames));
        }
        return basicMultiGetAsMap(rowKeys, columnNames);
    }

    @Override
    public Map<K,Map<N,V>> readRowsAsMap() {
        Map<K,Map<N,V>> resultMap = new HashMap<K,Map<N,V>>();
        try {
            RangeSlicesQuery<K, N, V> rangeSlicesQuery =
                    HFactory.createRangeSlicesQuery(keyspace, keySerializer, topSerializer, valueSerializer);
            rangeSlicesQuery.setColumnFamily(columnFamily);
            rangeSlicesQuery.setRange(null, null, false, ALL);
            rangeSlicesQuery.setRowCount(ALL);
            QueryResult<OrderedRows<K, N, V>> result = rangeSlicesQuery.execute();
            for (Row<K,N,V> row : result.get()) {
                K key = row.getKey();
                ColumnSlice<N,V> slice = row.getColumnSlice();
                Map<N,V> columns = new HashMap<N,V>();
                for (HColumn<N,V> column : slice.getColumns()) {
                    V value = column.getValue();
                    columns.put(column.getName(), value);
                }
                resultMap.put(key, columns);
            }
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Returning result from multiGetColumnsAsMap: " + resultMap);
        }
        return resultMap;
    }

    /**
     * Read all columns from multiple rows using a mapper for the result.
     *
     * @param rowKeys a collection of rows to read
     */
    @Override
    public <T> List<T> multiGet(Iterable<K> rowKeys, ColumnFamilyRowMapper<T,K,N,V> rowMapper) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Calling multiGetAsMap with rowKeys: " + rowKeys);
        }
        return basicMultiGet(rowKeys, rowMapper, null);
    }

    /**
     * Read specific columns from multiple rows using a mapper for the result.
     *
     * @param rowKeys     a collection of rows to read
     * @param columnNames names of the columns; if no columns are passed in then
     *                    just the keys are returned with no column data
     */
    @Override
    public <T> List<T> multiGetColumns(Iterable<K> rowKeys, ColumnFamilyRowMapper<T,K,N,V> rowMapper,
                                       N... columnNames) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Calling multiGetColumnsAsMap with rowKeys: " + rowKeys
                              + " and columnNames: " + Arrays.asList(columnNames));
        }
        return basicMultiGet(rowKeys, rowMapper, columnNames);
    }

    /**
     * Write a column value immediately.
     *
     * @param rowKey      the row key of type K
     * @param columnName  the column name
     * @param columnValue the column value
     */
    @Override
    public void writeColumn(K rowKey, N columnName, V columnValue) {
        basicWriteColumn(rowKey, columnName, columnValue, 0, null, null);
    }

    /**
     * Write a column value immediately with a time to live.
     *
     * @param rowKey             the row key of type K
     * @param columnName         the column name
     * @param columnValue        the column value
     * @param timeToLive         a positive time to live
     * @param timeToLiveTimeUnit the time unit for timeToLive
     */
    @Override
    public void writeColumn(K rowKey, N columnName, V columnValue, long timeToLive, TimeUnit timeToLiveTimeUnit) {
        basicWriteColumn(rowKey, columnName, columnValue, timeToLive, timeToLiveTimeUnit, null);
    }


    /**
     * Write a column value as batch operation.
     *
     * @param rowKey      the row key of type K
     * @param columnName  the column name
     * @param columnValue the column value
     * @param batchContext  BatchContext
     */
    @Override
    public void writeColumn(K rowKey, N columnName, V columnValue, @Nonnull BatchContext batchContext) {
        Validate.notNull(batchContext);
        basicWriteColumn(rowKey, columnName, columnValue, 0, null, batchContext);
    }

    /**
     * Write a column value as batch operation with a time to live.
     *
     * @param rowKey             the row key of type K
     * @param columnName         the column name
     * @param columnValue        the column value
     * @param batchContext         BatchContext
     * @param timeToLive         a positive time to live
     * @param timeToLiveTimeUnit the time unit for timeToLive
     */
    @Override
    public void writeColumn(K rowKey,
                            N columnName,
                            V columnValue,
                            int timeToLive,
                            TimeUnit timeToLiveTimeUnit,
                            @Nonnull BatchContext batchContext) {
        Validate.notNull(batchContext);
        basicWriteColumn(rowKey, columnName, columnValue, timeToLive, timeToLiveTimeUnit, batchContext);
    }

    /**
     * Write multiple columns immediately from a map.
     */
    @Override
    public void writeColumns(K rowKey, Map<N,V> map) {
        try {
            insertColumns(rowKey, map);
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
    }

    /**
     * Write multiple columns from a map as part of a batch operation.
     *
     * @param rowKey     the row key of type K
     * @param map        a map of columns with keys of column name type N and column values V.
     * @param batchContext optional BatchContext for batch operations
     */
    @Override
    public void writeColumns(K rowKey, Map<N,V> map, @Nonnull BatchContext batchContext) {
        Validate.notNull(batchContext);
        Mutator<K> mutator = validateAndGetMutator(batchContext);
        try {
            addInsertions(rowKey, map, mutator);
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
    }

    @Override
    public void deleteColumns(K rowKey, N... columnNames) {
        if (columnNames.length == 0) {
            return;
        }

        Mutator<K> mutator = createMutator();
        try {
            if (columnNames.length == 1) {
                mutator.delete(rowKey, columnFamily, columnNames[0], topSerializer);
            } else {
                for (N columnName : columnNames) {
                    mutator.addDeletion(rowKey, columnFamily, columnName, topSerializer);
                }
                mutator.execute();
            }
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
    }

    /**
     * Helper method to get multiple rows and return result in a Map.
     * @param rowKeys     The row keys to read.
     * @param columnNames if null then get all columns; otherwise, get only specified columns. If empty, then this
     *                    is a key-only query and only the keys are returned.
     * @return The column data
     */
    private Map<K,Map<N,V>> basicMultiGetAsMap(Iterable<K> rowKeys, @Nullable N[] columnNames) {

        Map<K,Map<N,V>> resultMap = new HashMap<K,Map<N,V>>();
        try {
            MultigetSliceQuery<K,N,V> query = HFactory.createMultigetSliceQuery(keyspace,
                                                                                keySerializer,
                                                                                topSerializer,
                                                                                valueSerializer);
            query.setKeys(rowKeys).
                    setColumnFamily(columnFamily).
                    setRange(null, null, false, ALL);
            if (columnNames != null) {
                query.setColumnNames(columnNames);
            }
            QueryResult<Rows<K,N,V>> result = query.execute();

            for (Row<K,N,V> row : result.get()) {
                K key = row.getKey();
                ColumnSlice<N,V> slice = row.getColumnSlice();
                Map<N,V> columns = new HashMap<N,V>();
                for (HColumn<N,V> column : slice.getColumns()) {
                    V value = column.getValue();
                    columns.put(column.getName(), value);
                }
                resultMap.put(key, columns);
            }
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Returning result from multiGetColumnsAsMap: " + resultMap);
        }
        return resultMap;
    }

    /**
     * Helper method to get multiple rows using a row mapper.
     * @param rowKeys     The row keys to read.
     * @param columnNames if null then get all columns; otherwise, get only specified columns. If empty, then this
     *                    is a key-only query and only the keys are returned.
     * @return The column data
     */
    private <T> List<T> basicMultiGet(Iterable<K> rowKeys,
                                      ColumnFamilyRowMapper<T,K,N,V> rowMapper,
                                      @Nullable N[] columnNames) {

        List<T> resultList = new ArrayList<T>();
        try {
            MultigetSliceQuery<K,N,V> query = HFactory.createMultigetSliceQuery(keyspace,
                                                                                keySerializer,
                                                                                topSerializer,
                                                                                valueSerializer);
            query.setKeys(rowKeys).
                    setColumnFamily(columnFamily).
                    setRange(null, null, false, ALL);
            if (columnNames != null) {
                query.setColumnNames(columnNames);
            }
            QueryResult<Rows<K,N,V>> result = query.execute();

            for (Row<K,N,V> row : result.get()) {
                K key = row.getKey();
                ColumnSlice<N,V> slice = row.getColumnSlice();
                List<HColumn<N,V>> columns = new ArrayList<HColumn<N,V>>();
                for (HColumn<N,V> column : slice.getColumns()) {
                    columns.add(column);
                }
                resultList.add(rowMapper.mapRow(key, columns));
            }
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Returning result from multiGetColumnsAsMap: " + resultList);
        }
        return resultList;
    }

    /**
     * Write a column value, with option of doing batch operation if batchContext is provided.
     *
     * @param rowKey             the row key of type K
     * @param columnName         the column name
     * @param columnValue        the column value
     * @param timeToLive         a positive time to live in seconds, or
     *                           ignored if timeToLiveTimeUnit is null.
     * @param timeToLiveTimeUnit the time unit for timeToLive, or null if no time to live is specified.
     * @param batchContext         optional BatchContext for a batch operation
     */
    private void basicWriteColumn(K rowKey,
                                  N columnName,
                                  V columnValue,
                                  long timeToLive,
                                  @Nullable TimeUnit timeToLiveTimeUnit,
                                  @Nullable BatchContext batchContext) {
        Mutator<K> mutator = validateAndGetMutator(batchContext);
        HColumn<N,V> column;

        if (timeToLiveTimeUnit == null) {
            column = HFactory.createColumn(columnName,
                                           columnValue,
                                           topSerializer,
                                           valueSerializer);
        } else {
            Validate.notNull(timeToLiveTimeUnit);
            long timeToLiveInSeconds = TimeUnit.SECONDS.convert(timeToLive, timeToLiveTimeUnit);
            Validate.isTrue(timeToLiveInSeconds <= Integer.MAX_VALUE && timeToLiveInSeconds > 0,
                            "Invalid time to live, must be positive and " +
                                    "fit in an int when converted to seconds: " + timeToLiveInSeconds);
            column = HFactory.createColumn(columnName,
                                           columnValue,
                                           (int)timeToLiveInSeconds,
                                           topSerializer,
                                           valueSerializer);
        }
        try {
            if (mutator == null) {
                createMutator().insert(rowKey, columnFamily, column);
            } else {
                mutator.addInsertion(rowKey, columnFamily, column);
            }
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
    }

    private void addInsertions(K rowKey, Map<N,V> properties, Mutator<K> mutator) {
        for (Map.Entry<N,V> mapEntry : properties.entrySet()) {
            mutator.addInsertion(rowKey, columnFamily, HFactory.createColumn(mapEntry.getKey(),
                                                                             mapEntry.getValue(),
                                                                             topSerializer,
                                                                             valueSerializer));
        }
    }

    private void insertColumns(K rowKey, Map<N,V> properties) {
        Mutator<K> mutator = createMutator();
        for (Map.Entry<N,V> mapEntry : properties.entrySet()) {
            mutator.addInsertion(rowKey, columnFamily, HFactory.createColumn(mapEntry.getKey(),
                                                                             mapEntry.getValue(),
                                                                             topSerializer,
                                                                             valueSerializer));
        }
        mutator.execute();
    }
}
