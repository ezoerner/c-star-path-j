package com.ebuddy.cassandra.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.Validate;
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
public class ColumnFamilyTemplate<K,N,V> extends AbstractColumnFamilyTemplate<K,N,V>
        implements ColumnFamilyOperations<K,N,V> {
    private static final Logger LOG = Logger.getLogger(ColumnFamilyTemplate.class);

    private final ColumnMapper<N,N,V> columnMapperToGetColumnNames = new ColumnMapper<N,N,V>() {
        @Override
        public N mapColumn(N columnName, V columnValue) {
            return columnName;
        }
    };

    public ColumnFamilyTemplate(Keyspace keyspace,
                                Serializer<K> keySerializer,
                                Serializer<N> columnNameSerializer,
                                Serializer<V> valueSerializer) {
        this(keyspace, null, keySerializer, columnNameSerializer, valueSerializer);
    }

    public ColumnFamilyTemplate(Keyspace keyspace,
                                @Nullable String defaultColumnFamily,
                                Serializer<K> keySerializer,
                                Serializer<N> columnNameSerializer,
                                Serializer<V> valueSerializer) {
        super(keyspace,
              defaultColumnFamily,
              keySerializer,
              Validate.notNull(columnNameSerializer),
              Validate.notNull(valueSerializer));
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
        ColumnQuery<K,N,V> query = HFactory.createColumnQuery(getKeyspace(),
                                                              getKeySerializer(),
                                                              getColumnNameSerializer(),
                                                              getValueSerializer());
        QueryResult<HColumn<N,V>> result = query.
                setKey(rowKey).
                setColumnFamily(getColumnFamily()).
                setName(columnName).
                execute();
        HColumn<N,V> column = result.get();
        return column != null ? column.getValue() : null;
        // we used to translate hector exceptions into spring exceptions here, but spring dependency was removed
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
        SliceQuery<K,N,V> query = HFactory.createSliceQuery(getKeyspace(),
                                                            getKeySerializer(),
                                                            getColumnNameSerializer(),
                                                            getValueSerializer());
        QueryResult<ColumnSlice<N,V>> result = query.setKey(rowKey).
                setColumnFamily(getColumnFamily()).
                setRange(start, finish, reversed, count).
                execute();
        ColumnSlice<N,V> slice = result.get();

        for (HColumn<N,V> column : slice.getColumns()) {
            maps.put(column.getName(),
                     column.getValue());
        }
        // we used to translate hector exceptions into spring exceptions here, but spring dependency was removed
        return maps;
    }

    @Override
    public <T> List<T> readColumns(K rowKey, ColumnMapper <T,N,V> columnMapper) {
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

        SliceQuery<K,N,V> query = HFactory.createSliceQuery(getKeyspace(),
                                                            getKeySerializer(),
                                                            getColumnNameSerializer(),
                                                            getValueSerializer());
        QueryResult<ColumnSlice<N,V>> result = query.setKey(rowKey).
                setColumnFamily(getColumnFamily()).
                setRange(start, finish, reversed, count).
                execute();
        ColumnSlice<N,V> slice = result.get();

        for (HColumn<N,V> column : slice.getColumns()) {
            resultList.add(columnMapper.mapColumn(column.getName(), column.getValue()));
        }
        // we used to translate hector exceptions into spring exceptions here, but spring dependency was removed
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
        RangeSlicesQuery<K, N, V> rangeSlicesQuery = HFactory.createRangeSlicesQuery(getKeyspace(),
                                                                                     getKeySerializer(),
                                                                                     getColumnNameSerializer(),
                                                                                     getValueSerializer());
        rangeSlicesQuery.setColumnFamily(getColumnFamily());
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

        // we used to translate hector exceptions into spring exceptions here, but spring dependency was removed

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
        insertColumns(rowKey, map);
        // we used to translate hector exceptions into spring exceptions here, but spring dependency was removed
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
        addInsertions(rowKey, map, mutator);
        // we used to translate hector exceptions into spring exceptions here, but spring dependency was removed
    }

    @Override
    public void deleteColumns(K rowKey, N... columnNames) {
        if (columnNames.length == 0) {
            return;
        }

        Mutator<K> mutator = createMutator();
        if (columnNames.length == 1) {
            mutator.delete(rowKey, getColumnFamily(), columnNames[0], getColumnNameSerializer());
        } else {
            for (N columnName : columnNames) {
                mutator.addDeletion(rowKey, getColumnFamily(), columnName, getColumnNameSerializer());
            }
            mutator.execute();
        }
        // we used to translate hector exceptions into spring exceptions here, but spring dependency was removed
    }

    @Override
    public void deleteColumns(K rowKey, N start, N finish) {
        deleteColumns(rowKey, start, finish, null);
    }

    @Override
    public void deleteColumns(K rowKey, N start, N finish, @Nullable BatchContext batchContext) {
        Mutator<K> mutator;
        boolean shouldExecute;
        if (batchContext == null) {
            shouldExecute = true;
            mutator = createMutator();
        } else {
            shouldExecute = false;
            mutator = validateAndGetMutator(batchContext);
        }

        // unfortunately the thrift API to Cassandra does not support deleting with a SliceRange.
        // !! We have read before delete -- performance and thread safety issue
        // get column names to delete using a slice query
        List<N> columnNamesToDelete = readColumns(rowKey,
                                                  start,
                                                  finish,
                                                  ALL,
                                                  false,
                                                  columnMapperToGetColumnNames);
        for (N columnName : columnNamesToDelete) {
            mutator.addDeletion(rowKey, getColumnFamily(), columnName, getColumnNameSerializer());
        }

        if (shouldExecute) {
            mutator.execute();
        }
        // we used to translate hector exceptions into spring exceptions here, but spring dependency was removed
    }

    /**
     * Helper method to get multiple rows and return result in a Map.
     * @param rowKeys     The row keys to read.
     * @param columnNames if null then get all columns; otherwise, get only specified columns. If empty, then this
     *                    is a key-only query and only the keys are returned.
     * @return The column data
     */
    private Map<K,Map<N,V>> basicMultiGetAsMap(Iterable <K> rowKeys, @Nullable N[] columnNames) {

        Map<K,Map<N,V>> resultMap = new HashMap<K,Map<N,V>>();
        MultigetSliceQuery<K,N,V> query = HFactory.createMultigetSliceQuery(getKeyspace(),
                                                                            getKeySerializer(),
                                                                            getColumnNameSerializer(),
                                                                            getValueSerializer());
        query.setKeys(rowKeys).
                setColumnFamily(getColumnFamily()).
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
        // we used to translate hector exceptions into spring exceptions here, but spring dependency was removed
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
    private <T> List<T> basicMultiGet(Iterable <K> rowKeys,
                                      ColumnFamilyRowMapper<T,K,N,V> rowMapper,
                                      @Nullable N[] columnNames) {

        List<T> resultList = new ArrayList<T>();

        MultigetSliceQuery<K,N,V> query = HFactory.createMultigetSliceQuery(getKeyspace(),
                                                                            getKeySerializer(),
                                                                            getColumnNameSerializer(),
                                                                            getValueSerializer());
        query.setKeys(rowKeys).
                setColumnFamily(getColumnFamily()).
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
        // we used to translate hector exceptions into spring exceptions here, but spring dependency was removed
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
            column = createColumn(columnName, columnValue);
        } else {
            Validate.notNull(timeToLiveTimeUnit);
            long timeToLiveInSeconds = TimeUnit.SECONDS.convert(timeToLive, timeToLiveTimeUnit);
            Validate.isTrue(timeToLiveInSeconds <= Integer.MAX_VALUE && timeToLiveInSeconds > 0,
                            "Invalid time to live, must be positive and " +
                                    "fit in an int when converted to seconds: " + timeToLiveInSeconds);
            column = HFactory.createColumn(columnName,
                                           columnValue,
                                           (int)timeToLiveInSeconds,
                                           getColumnNameSerializer(),
                                           getValueSerializer());
        }

        if (mutator == null) {
            createMutator().insert(rowKey, getColumnFamily(), column);
        } else {
            mutator.addInsertion(rowKey, getColumnFamily(), column);
        }
        // we used to translate hector exceptions into spring exceptions here, but spring dependency was removed
    }

    private void addInsertions(K rowKey, Map<N,V> properties, Mutator<K> mutator) {
        for (Map.Entry<N,V> mapEntry : properties.entrySet()) {
            mutator.addInsertion(rowKey, getColumnFamily(), createColumn(mapEntry.getKey(), mapEntry.getValue()));
        }
    }

    private void insertColumns(K rowKey, Map<N,V> properties) {
        Mutator<K> mutator = createMutator();
        for (Map.Entry<N,V> mapEntry : properties.entrySet()) {
            N key = mapEntry.getKey();
            V value = mapEntry.getValue();
            mutator.addInsertion(rowKey, getColumnFamily(), createColumn(key, value));
        }
        mutator.execute();
    }

    private HColumn<N,V> createColumn(N key, V value) {
        return HFactory.createColumn(key, value, getColumnNameSerializer(), getValueSerializer());
    }

    private Serializer<N> getColumnNameSerializer() {
        return getTopSerializer();
    }
}
