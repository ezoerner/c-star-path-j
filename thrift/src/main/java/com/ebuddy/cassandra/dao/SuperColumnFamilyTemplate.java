package com.ebuddy.cassandra.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.Validate;

import com.ebuddy.cassandra.BatchContext;
import com.ebuddy.cassandra.dao.mapper.ColumnMapper;
import com.ebuddy.cassandra.dao.mapper.SuperColumnMapper;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSubSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SubColumnQuery;
import me.prettyprint.hector.api.query.SubSliceQuery;
import me.prettyprint.hector.api.query.SuperSliceQuery;


/**
 * Data access template for Cassandra Super column families.
 *
 * @param <K>  the Row Key type.
 * @param <SN> The supercolumn name type.
 * @param <N>  The subcolumn name type.
 * @param <V>  The column value type.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public final class SuperColumnFamilyTemplate<K,SN,N,V> extends AbstractColumnFamilyTemplate<K,SN,V>
        implements SuperColumnFamilyOperations<K,SN,N,V> {

    /**
     * The serializer for subcolumn names.
     */
    private final Serializer<N> subSerializer;

    public SuperColumnFamilyTemplate(Keyspace keyspace,
                                     String columnFamily,
                                     Serializer<K> keySerializer,
                                     Serializer<SN> topSerializer,
                                     Serializer<N> subSerializer,
                                     Serializer<V> valueSerializer) {
        super(keyspace, columnFamily, keySerializer, topSerializer, valueSerializer);
        this.subSerializer = subSerializer;
    }

    /**
     * Get a column value from a specified super column.
     *
     * @param rowKey          the row key of type K
     * @param superColumnName the name of the super column of type SN
     * @param columnName      the column name
     * @return the column value or null if not found
     */
    @Override
    public V readColumnValue(K rowKey, SN superColumnName, N columnName) {
        try {
            SubColumnQuery<K,SN,N,V> query = HFactory.createSubColumnQuery(keyspace,
                                                                           keySerializer,
                                                                           topSerializer,
                                                                           subSerializer,
                                                                           valueSerializer);
            QueryResult<HColumn<N,V>> result = query.
                    setKey(rowKey).
                    setColumnFamily(defaultColumnFamily).
                    setSuperColumn(superColumnName).
                    setColumn(columnName).
                    execute();
            HColumn<N,V> column = result.get();
            return column != null ? column.getValue() : null;
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
    }

    /**
     * Get columns from a super column as a map.
     *
     * @param rowKey          the row key
     * @param superColumnName the super column name
     * @param columnNames     if given, a list of column names to get, otherwise get all columns
     * @return a map of column values keyed by row key
     */
    @Override
    public Map<N,V> readColumnsAsMap(K rowKey, SN superColumnName, N... columnNames) {
        Map<N,V> columns = new HashMap<N,V>();
        try {
            SubSliceQuery<K,SN,N,V> query = HFactory.createSubSliceQuery(keyspace,
                                                                         keySerializer,
                                                                         topSerializer,
                                                                         subSerializer,
                                                                         valueSerializer);
            query.setKey(rowKey).
                    setColumnFamily(defaultColumnFamily).
                    setSuperColumn(superColumnName).
                    setRange(null, null, false, ALL);
            if (columnNames.length == 0) {
                query.setRange(null, null, false, ALL);
            } else {
                query.setColumnNames(columnNames);
            }

            QueryResult<ColumnSlice<N,V>> result = query.execute();
            ColumnSlice<N,V> slice = result.get();

            for (HColumn<N,V> column : slice.getColumns()) {
                V value = column.getValue();
                columns.put(column.getName(), value);
            }
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
        return columns;
    }

    /**
     * Get columns from a super column returning a list of mapped super columns.
     *
     * @param rowKey            the row key
     * @param superColumnName   restricts query to this supercolumn.
     * @param columnMapper      a provided mapper to create results from the columns
     * @return list of result objects created by the mapper
     */
    @Override
    public <T> List<T> readColumns(K rowKey, SN superColumnName, ColumnMapper<T,N,V> columnMapper) {
        return readColumns(rowKey, superColumnName, null, null, ALL, false, columnMapper);
    }

    @Override
    public <T> List<T> readColumns(K rowKey,
                                   SN superColumnName,
                                   N start,
                                   N finish,
                                   int count,
                                   boolean reversed,
                                   ColumnMapper<T,N,V> columnMapper) {
        List<T> resultList = new ArrayList<T>();
        try {
            SubSliceQuery<K,SN,N,V> query = HFactory.createSubSliceQuery(keyspace,
                                                                         keySerializer,
                                                                         topSerializer,
                                                                         subSerializer,
                                                                         valueSerializer);
            query.setKey(rowKey).
                    setColumnFamily(defaultColumnFamily).
                    setSuperColumn(superColumnName).
                    setRange(start, finish, reversed, count);

            QueryResult<ColumnSlice<N,V>> result = query.execute();
            ColumnSlice<N,V> slice = result.get();

            for (HColumn<N,V> column : slice.getColumns()) {
                V value = column.getValue();
                resultList.add(columnMapper.mapColumn(column.getName(), value));
            }
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
        return resultList;
    }

    /**
     * Read all columns from multiple rows from a single super column in each row.
     *
     * @param rowKeys         - Keys to search
     * @param superColumnName - superColumn involved in the query
     * @return Map of properties
     */
    @Override
    public Map<K,Map<N,V>> multiGetAsMap(Collection<K> rowKeys, SN superColumnName) {
        return basicMultiGetAsMap(rowKeys, superColumnName, null);
    }


    /**
     * Read specified columns from multiple rows from a single super column in each row.
     *
     * @param rowKeys         - Keys to search
     * @param superColumnName - superColumn involved in the query
     * @param columnNames     - Restrict the query to these column names; if no names are given then reads
     *                        all column names
     * @return Map of properties
     */
    @Override
    public Map<K,Map<N,V>> multiGetColumnsAsMap(Collection<K> rowKeys, SN superColumnName, N... columnNames) {
        return basicMultiGetAsMap(rowKeys, superColumnName, columnNames);
    }

    /**
     * Read all columns from multiple rows from a single super column in each row,
     * returning a list of mapped super columns.
     *
     * @param rowKeys         - Keys to search
     * @param superColumnName - superColumn involved in the query
     * @return List of mapped super columns. The order is only meaningful if rows are ordered in the database.
     */
    @Override
    public <T> List<T> multiGetSuperColumn(Collection<K> rowKeys,
                                                 SN superColumnName,
                                                 SuperColumnMapper<T,K,SN,N,V> superColumnMapper) {
        return basicMultiGetSubSlice(rowKeys, superColumnName, superColumnMapper, null);
    }


    /**
     * Read specified columns from multiple rows from a single super column in each row,
     * returning a list of mapped super columns.
     *
     * @param rowKeys         - Keys to search
     * @param superColumnName - superColumn involved in the query
     * @param columnNames     - Restrict the query to these column names; if no names are given then reads
     *                        all column names
     * @return Map of properties
     */
    @Override
    public <T> List<T> multiGetColumns(Collection<K> rowKeys,
                                             SN superColumnName,
                                             SuperColumnMapper<T,K,SN,N,V> superColumnMapper,
                                             N... columnNames) {
        return basicMultiGetSubSlice(rowKeys, superColumnName, superColumnMapper, columnNames);
    }

    /**
     * Read an entire row as a map of maps.
     *
     * @param key the row key
     * @return the row as a map of maps
     *         where the key is the super column name and the value is a map of column names to column values.
     */
    @Override
    public Map<SN,Map<N,V>> readRowAsMap(K key) {
        try {
            SuperSliceQuery<K,SN,N,V> query = HFactory.createSuperSliceQuery(keyspace,
                                                                             keySerializer,
                                                                             topSerializer,
                                                                             subSerializer,
                                                                             valueSerializer);
            query.setKey(key).
                    setColumnFamily(defaultColumnFamily).
                    setRange(null, null, false, ALL);
            QueryResult<SuperSlice<SN,N,V>> queryResult = query.execute();
            Map<SN,Map<N,V>> results = new HashMap<SN,Map<N,V>>();
            for (HSuperColumn<SN,N,V> superColumn : queryResult.get().getSuperColumns()) {
                List<HColumn<N,V>> allColumns = superColumn.getColumns();
                Map<N,V> columnMap = new HashMap<N,V>(allColumns.size());
                for (HColumn<N,V> column : allColumns) {
                    columnMap.put(column.getName(), column.getValue());
                }
                if (!columnMap.isEmpty()) {
                    results.put(superColumn.getName(), columnMap);
                }
            }
            return results;
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
    }

    /**
     * Read an entire row as a list of mapped supercolumns.
     *
     * @param key the row key
     * @return a List of T as returned by the supercolumn mapper.
     *
     */
    @Override
    public <T> List<T> readRow(K key, SuperColumnMapper<T,K,SN,N,V> superColumnMapper) {
        try {
            SuperSliceQuery<K,SN,N,V> query = HFactory.createSuperSliceQuery(keyspace,
                                                                             keySerializer,
                                                                             topSerializer,
                                                                             subSerializer,
                                                                             valueSerializer);
            query.setKey(key).
                    setColumnFamily(defaultColumnFamily).
                    setRange(null, null, false, ALL);
            QueryResult<SuperSlice<SN,N,V>> queryResult = query.execute();
            List<HSuperColumn<SN,N,V>> superColumns = queryResult.get().getSuperColumns();
            List<T> results = new ArrayList<T>( superColumns.size());
            for (HSuperColumn<SN,N,V> superColumn : superColumns) {
                T mappedSuperColumn = superColumnMapper.mapSuperColumn(key, superColumn.getName(), superColumn.getColumns());
                // don't include null
                if (mappedSuperColumn != null) {
                    results.add(mappedSuperColumn);
                }
            }
            return results;
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
    }

    /**
     * Set subcolumn values for a specified super column and execute immediately.
     *
     * @param rowKey          the row key of type K
     * @param superColumnName the super column name of type SN
     * @param columnMap       a map of column names type N and column values type V.
     */
    @Override
    public void writeColumns(K rowKey, SN superColumnName, Map<N,V> columnMap) {
        basicWriteColumns(rowKey, superColumnName, columnMap, null);

    }

    /**
     * Set subcolumn values for a specified super column as a batch operation.
     *
     * @param rowKey          the row key of type K
     * @param superColumnName the super column name of type SN
     * @param columnMap       a map of column names type N and column values type V.
     * @param txnContext      BatchContext for batch operations
     */
    @Override
    public void writeColumns(K rowKey,
                                   SN superColumnName,
                                   Map<N,V> columnMap,
                                   @Nonnull BatchContext txnContext) {
        Validate.notNull(txnContext);
        basicWriteColumns(rowKey, superColumnName, columnMap, txnContext);
    }

    /**
     * Set a subcolumn value for a specified super column.
     *
     * @param rowKey          the row key of type K
     * @param superColumnName the super column name of type SN
     * @param columnName      the column name
     * @param columnValue     the column value
     */
    @Override
    public void writeColumn(K rowKey, SN superColumnName, N columnName, V columnValue) {
        basicWriteColumn(rowKey, superColumnName, columnName, columnValue, null);

    }


    /**
     * Write a subcolumn value for a specified super column as a batch operation.
     *
     * @param rowKey          the row key of type K
     * @param superColumnName the super column name of type SN
     * @param columnName      the column name
     * @param columnValue     the column value
     * @param txnContext      BatchContext for batch operations
     */
    @Override
    public void writeColumn(K rowKey,
                                  SN superColumnName,
                                  N columnName,
                                  V columnValue,
                                  @Nonnull BatchContext txnContext) {
        Validate.notNull(txnContext);
        basicWriteColumn(rowKey, superColumnName, columnName, columnValue, txnContext);
    }

    @Override
    public void deleteColumns(K rowKey, SN superColumnName, Iterable<N> subcolumnNames) {
        Mutator<K> mutator = createMutator();
        try {
            for (N subcolumnName : subcolumnNames) {
                mutator.subDelete(rowKey, defaultColumnFamily,
                                  superColumnName,
                                  subcolumnName,
                                  topSerializer,
                                  subSerializer);
            }
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
    }

    /**
     * Remove subcolumns in a super column as a batch operation.
     *
     * @param rowKey          the row key
     * @param superColumnName the super column name
     * @param subcolumnNames  the names of the subcolumns
     * @param txnContext      BatchContext for batch operation
     */
    @Override
    public void deleteColumns(K rowKey,
                                    SN superColumnName,
                                    Iterable<N> subcolumnNames,
                                    @Nonnull BatchContext txnContext) {
        Validate.notNull(txnContext);
        Mutator<K> mutator = validateAndGetMutator(txnContext);
        try {
            for (N subcolumn : subcolumnNames) {
                mutator.addSubDelete(rowKey,
                                     defaultColumnFamily, superColumnName, subcolumn, topSerializer, subSerializer);
            }
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
    }

    @Override
    public void deleteSuperColumn(K rowKey, SN superColumnName) {

        Mutator<K> mutator = createMutator();
        try {
            mutator.superDelete(rowKey, defaultColumnFamily, superColumnName, topSerializer);
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
    }

    @Override
    public void deleteSuperColumn(K rowKey, SN superColumnName, @Nonnull BatchContext txnContext) {

        Validate.notNull(txnContext);

        Mutator<K> mutator = validateAndGetMutator(txnContext);
        try {
            mutator.superDelete(rowKey, defaultColumnFamily, superColumnName, topSerializer);
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
    }

    private void basicWriteColumn(K rowKey,
                                  SN superColumnName,
                                  N columnName,
                                  V columnValue,
                                  @Nullable BatchContext txnContext) {


        // create the subcolumns for the super column
        @SuppressWarnings({"unchecked"})
        List<HColumn<N,V>> columns = Arrays.asList(
                HFactory.createColumn(
                        columnName, columnValue, subSerializer, valueSerializer
                )
        );
        HSuperColumn<SN,N,V> superColumn = HFactory.createSuperColumn(superColumnName,
                                                                      columns,
                                                                      topSerializer,
                                                                      subSerializer,
                                                                      valueSerializer);
        if (txnContext == null) {
            insertSuperColumn(rowKey, superColumn);
        } else {
            insertSuperColumn(rowKey, superColumn, txnContext);
        }
    }

    /**
     * Set subcolumn values for a specified super column as a batch operation.
     *
     * @param rowKey          the row key of type K
     * @param superColumnName the super column name of type SN
     * @param columnMap       a map of column names type N and column values type V.
     * @param txnContext      BatchContext for batch operations
     */
    private void basicWriteColumns(K rowKey,
                                   SN superColumnName,
                                   Map<N,V> columnMap,
                                   @Nullable BatchContext txnContext) {

        // create the subcolumns for the super column
        List<HColumn<N,V>> columns = new ArrayList<HColumn<N,V>>(columnMap.size());
        for (Map.Entry<N,V> mapEntry : columnMap.entrySet()) {
            columns.add(HFactory.createColumn(mapEntry.getKey(),
                                              mapEntry.getValue(),
                                              subSerializer,
                                              valueSerializer));
        }

        HSuperColumn<SN,N,V> superColumn = HFactory.createSuperColumn(superColumnName,
                                                                      columns,
                                                                      topSerializer,
                                                                      subSerializer,
                                                                      valueSerializer);

        if (txnContext == null) {
            insertSuperColumn(rowKey, superColumn);
        } else {
            insertSuperColumn(rowKey, superColumn, txnContext);
        }
    }

    /**
     * Read a single super columns from multiple rows and return data as a map.
     *
     * @param columnNames if null then return all columns, otherwise return specific columns. If empty then
     *                    returns keys only (no supercolumns or columns).
     */
    private Map<K,Map<N,V>> basicMultiGetAsMap(Collection<K> rowKeys,
                                               SN superColumnName,
                                               @Nullable N[] columnNames) {

        Map<K,Map<N,V>> superColumns = new HashMap<K,Map<N,V>>();
        try {
            MultigetSubSliceQuery<K,SN,N,V> query = HFactory.createMultigetSubSliceQuery(keyspace,
                                                                                         keySerializer,
                                                                                         topSerializer,
                                                                                         subSerializer,
                                                                                         valueSerializer);
            query.setKeys(rowKeys).
                    setColumnFamily(defaultColumnFamily).
                    setSuperColumn(superColumnName).
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
                superColumns.put(key, columns);
            }
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
        return superColumns;
    }

    /**
     * Read a single super column from multiple rows returns as a list of mapped super columns.
     *
     * @param columnNames if null then return all columns, otherwise return specific columns in each row. If empty then
     *                    returns keys only (no supercolumns or columns).
     * @return a list of row objects as provided by the row mapper
     */
    private <T> List<T> basicMultiGetSubSlice(Collection<K> rowKeys,
                                              SN superColumnName,
                                              SuperColumnMapper<T,K,SN,N,V> superColumnMapper,
                                              @Nullable N[] columnNames) {

        List<T> superColumns = new LinkedList<T>();
        try {
            MultigetSubSliceQuery<K,SN,N,V> query = HFactory.createMultigetSubSliceQuery(keyspace,
                                                                                         keySerializer,
                                                                                         topSerializer,
                                                                                         subSerializer,
                                                                                         valueSerializer);
            query.setKeys(rowKeys).
                    setColumnFamily(defaultColumnFamily).
                    setSuperColumn(superColumnName).
                    setRange(null, null, false, ALL);
            if (columnNames != null) {
                query.setColumnNames(columnNames);
            }
            QueryResult<Rows<K,N,V>> result = query.execute();

            for (Row<K,N,V> row : result.get()) {
                K key = row.getKey();
                ColumnSlice<N,V> slice = row.getColumnSlice();
                List<HColumn<N,V>> columns = slice.getColumns();
                T mappedSuperColumn = superColumnMapper.mapSuperColumn(key, superColumnName, columns);
                superColumns.add(mappedSuperColumn);
            }
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
        return superColumns;
    }


    private void insertSuperColumn(K rowKey,
                                   HSuperColumn<SN,N,V> superColumn) {
        try {
            createMutator().insert(rowKey, defaultColumnFamily, superColumn);
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
    }


    private void insertSuperColumn(K rowKey,
                                   HSuperColumn<SN,N,V> superColumn,
                                   @Nonnull BatchContext txnContext) {
        Validate.notNull(txnContext);
        Mutator<K> mutator = validateAndGetMutator(txnContext);
        try {
            mutator.addInsertion(rowKey, defaultColumnFamily, superColumn);
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
    }
}
