package com.ebuddy.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;

import com.ebuddy.cassandra.dao.StructureSerializer;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.BatchMutation;
import me.prettyprint.cassandra.service.KeyspaceService;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.cassandra.utils.StringUtils;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.exceptions.HNotFoundException;

/**
 * Transitional version of HectorUtils that uses Objects instead of PropertyValues.
 * The purpose of this class is in assisting in the removal of deprecated PropertyValue and
 * PropertyValueFactory classes.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 * @deprecated use DAO objects and ExtendedNetworkId instead
 */
@Deprecated
public final class TransitionalHectorUtils {
    //private static final Logger LOG = Logger.getLogger(HectorUtils.class);

    private static final ByteBuffer EMPTY_BYTES = ByteBuffer.wrap(new byte[0]);

    /**
     * ALL consistency level policy
     */
    public static final ConsistencyLevelPolicy ALL_CONSISTENCY = new ConfigurableConsistencyLevel() {
        @Override
        public HConsistencyLevel get(OperationType op) {
            return HConsistencyLevel.ALL;
        }

        @Override
        public HConsistencyLevel get(OperationType op, String cfName) {
            return HConsistencyLevel.ALL;
        }
    };

    /**
     * A pre-fabricated SlicePredicate that gets all keys (up to SETTINGS_LIMIT).
     */
    public static final SlicePredicate ALL_COLUMNS;

    static {
        SliceRange sliceRange = new SliceRange(EMPTY_BYTES,
                                               EMPTY_BYTES,
                                               false,
                                               Integer.MAX_VALUE);
        ALL_COLUMNS = new SlicePredicate();
        ALL_COLUMNS.setSlice_range(sliceRange);
    }

    private TransitionalHectorUtils() {
    }

    /**
     * Simple utility to find a SuperColumn by name from a list of SuperColumns.
     *
     * @param superColumns    the list of SuperColumns
     * @param superColumnName the ByteBuffer SuperColumn name to look for
     * @return the found SuperColumn or null if not found
     */
    public static SuperColumn findSuperColumn(List<SuperColumn> superColumns, ByteBuffer superColumnName) {
        for (SuperColumn superColumn : superColumns) {
            if (superColumn.bufferForName().equals(superColumnName)) {
                return superColumn;
            }
        }
        return null;
    }


    /**
     * Write a batchMutation to Cassandra.
     *
     * @param batchMutation the batch mutation
     * @param keyspaceName  the name of the keyspace
     * @param template      the cassandra template
     */
    public static void mutate(final BatchMutation<?> batchMutation,
                              final String keyspaceName,
                              CassandraTemplate template) {
        template.write(keyspaceName, new KeyspaceCallback<Void>() {
            public Void execute(KeyspaceService keyspace) {
                keyspace.batchMutate(batchMutation);
/*
                if (LOG.isDebugEnabled()) {
                    LOG.debug("sending batchMutate on keyspace '" + keyspaceName +
                            "'; batchMutation isEmpty=" + batchMutation.isEmpty()); 
                }
*/
                return null;
            }
        });
    }


    /**
     * Remove a row in a column family (or super column family).
     *
     * @param keyspaceName     the name of the keyspace
     * @param columnFamilyName the name of the column family
     * @param rowKey           the row key
     * @param template         the cassandra template
     */
    public static void removeRow(String keyspaceName,
                                 final String columnFamilyName,
                                 final String rowKey,
                                 CassandraTemplate template) {
        template.write(keyspaceName, new KeyspaceCallback<Void>() {
            public Void execute(KeyspaceService keyspace) {
                ColumnPath columnPath = createColumnPath(columnFamilyName, (String) null, null);
                keyspace.remove(rowKey, columnPath);
                return null;
            }
        });
    }

    /**
     * Remove a column in a column family (or super column family).
     *
     * @param keyspaceName     the name of the keyspace
     * @param columnFamilyName the name of the column family
     * @param rowKey           the row key
     * @param superColumnName  the super column name, or null
     * @param columnName       the columnName, or null if deleting all columns
     * @param template         the cassandra template
     */
    @SuppressWarnings({"UnusedDeclaration"})//NOSONAR
    public static void removeColumn(String keyspaceName,
                                    final String columnFamilyName,
                                    final String rowKey,
                                    final String superColumnName,
                                    final String columnName,
                                    CassandraTemplate template) {
        template.write(keyspaceName, new KeyspaceCallback<Void>() {
            public Void execute(KeyspaceService keyspace) {
                ColumnPath columnPath = createColumnPath(columnFamilyName, superColumnName, columnName);
                keyspace.remove(rowKey, columnPath);
                return null;
            }
        });
    }

    /**
     * Remove listed columns in a column family (or super column family).
     *
     * @param keyspaceName     the name of the keyspace
     * @param columnFamilyName the name of the column family
     * @param rowKey           the row key
     * @param superColumnName  the super column name, or null
     * @param columnNames      the column names, or null if deleting all columns
     * @param template         the cassandra template
     */
    public static void removeColumns(String keyspaceName,
                                     final String columnFamilyName,
                                     final String rowKey,
                                     final String superColumnName,
                                     final List<String> columnNames,
                                     CassandraTemplate template) {

        if (columnNames == null ||
            columnNames.size() == 1) {
            template.write(keyspaceName, new KeyspaceCallback<Void>() {
                public Void execute(KeyspaceService keyspace) {
                    String colName = null;
                    if (columnNames != null) {
                        colName = columnNames.get(0);
                    }
                    ColumnPath columnPath = createColumnPath(columnFamilyName, superColumnName, colName);
                    keyspace.remove(rowKey, columnPath);
                    return null;
                }
            });
        } else {
            // use a BatchMutation to do multiple column deletions in one go
            BatchMutation<String> batchMutation = new BatchMutation<String>(StringSerializer.get());
            Deletion deletion = new Deletion();
            deletion.setTimestamp(template.createTimestamp());
            if (superColumnName != null) {
                deletion.setSuper_column(bytes(superColumnName));
            }
            SlicePredicate predicate = new SlicePredicate();
            for (String columnName : columnNames) {
                predicate.addToColumn_names(bytes(columnName));
            }
            deletion.setPredicate(predicate);
            batchMutation.addDeletion(rowKey, Collections.singletonList(columnFamilyName), deletion);

            mutate(batchMutation, keyspaceName, template);
        }
    }


    /**
     * Store key-value pairs to a column family.
     *
     * @param rowKey           the row key
     * @param columnFamilyName the column family name
     * @param keyValuePairs    map with the key value pairs
     * @param template         the cassandra template
     * @param keyspaceName     the keyspace name
     */
    public static void storeKeyValuePairs(final String rowKey,
                                          final String columnFamilyName,
                                          final Map<String, Object> keyValuePairs,
                                          final CassandraTemplate template,
                                          final String keyspaceName,
                                          final String delimiter) {
        storeKeyValuePairs(rowKey, columnFamilyName, null, keyValuePairs, template, keyspaceName, delimiter);
    }


    /**
     * Store key-value pairs to a super column family.
     *
     * @param rowKey           the row key
     * @param columnFamilyName the column family name
     * @param superColumnName  the super column name
     * @param keyValuePairs    map with the key value pairs, keys can be hierarchical using a delimiter;
     * @param template         the cassandra template
     * @param keyspaceName     the keyspace name
     * @param delimiter        the delimiter to use for hierarchical properties
     */
    public static void storeKeyValuePairs(final String rowKey,
                                          final String columnFamilyName,
                                          final String superColumnName,
                                          final Map<String, Object> keyValuePairs,
                                          final CassandraTemplate template,
                                          final String keyspaceName,
                                          final String delimiter) {
        template.write(keyspaceName, new KeyspaceCallback<Void>() {
            public Void execute(KeyspaceService keyspace) {
                NestedBatchMutation batchMutation = new NestedBatchMutation(template,
                                                                            keyspaceName,
                                                                            rowKey,
                                                                            columnFamilyName,
                                                                            superColumnName);

                for (Map.Entry<String, Object> keyValue : keyValuePairs.entrySet()) {
                    Object propValue = keyValue.getValue();
                    if (!(propValue instanceof String)) {
                        throw new IllegalArgumentException("storage of structured property values not yet implemented");
                    }
                    batchMutation.addInsertion(keyValue.getKey(), (String)propValue, delimiter);
                }

                keyspace.batchMutate(batchMutation.getMutationMap());
                return null;
            }
        });
    }

    /**
     * Store one key-value pairs to a super column family.
     *
     * @param rowKey           the row key
     * @param columnFamilyName the column family name
     * @param superColumnName  the super column name
     * @param propertyName     the property name
     * @param value            the string value of the property
     * @param template         the cassandra template
     * @param keyspaceName     the keyspace name
     */
    @SuppressWarnings({"UnusedDeclaration"})
    public static void storeOneValue(final String rowKey,
                                     final String columnFamilyName,
                                     @Nullable final String superColumnName,
                                     final String propertyName,
                                     final String value,
                                     final CassandraTemplate template,
                                     String keyspaceName) {
        template.write(keyspaceName, new KeyspaceCallback<Void>() {
            public Void execute(KeyspaceService keyspace) {
                ColumnPath columnPath = createColumnPath(columnFamilyName, superColumnName, propertyName);
                keyspace.insert(rowKey, columnPath, bytes(value));
                return null;
            }
        });
    }

    /**
     * Store one key-value pairs to a super column family.
     *
     * @param rowKey           the row key
     * @param columnFamilyName the column family name
     * @param superColumnName  the super column name
     * @param propertyName     the name of the property
     * @param value            the string value of the property
     * @param template         the cassandra template
     * @param keyspaceName     the keyspace name
     * @param consistencyLevel the consistency level for this operation
     */
    public static void storeOneValue(final String rowKey,
                                     final String columnFamilyName,
                                     final String superColumnName,
                                     final String propertyName,
                                     final String value,
                                     final CassandraTemplate template,
                                     String keyspaceName,
                                     ConsistencyLevelPolicy consistencyLevel) {
        template.execute(keyspaceName, new KeyspaceCallback<Void>() {
            public Void execute(KeyspaceService keyspace) {
                ColumnPath columnPath = createColumnPath(columnFamilyName, superColumnName, propertyName);
                keyspace.insert(rowKey, columnPath, bytes(value));
                return null;
            }
        }, consistencyLevel, false);
    }

    /**
     * Store one key-value pairs to a super column family.
     *
     * @param rowKey           the row key
     * @param columnFamilyName the column family name
     * @param superColumnName  the super column name
     * @param propertyName     the property name
     * @param value            the string value of the property
     * @param template         the cassandra template
     * @param keyspaceName     the keyspace name
     * @param ttlInMinutes     		   the ttlInMinutes
     */
    public static void storeOneValue(final String rowKey,
                                     final String columnFamilyName,
                                     @Nullable final String superColumnName,
                                     final String propertyName,
                                     final String value,
                                     final CassandraTemplate template,
                                     String keyspaceName,
                                     final int ttlInMinutes) {
        template.write(keyspaceName, new KeyspaceCallback<Void>() {
            public Void execute(KeyspaceService keyspace) {
            	int ttlInSeconds = (int) TimeUnit.SECONDS.convert(ttlInMinutes, TimeUnit.MINUTES);
                ColumnPath columnPath = createColumnPath(columnFamilyName, superColumnName, propertyName);
                ColumnParent columnParent = new ColumnParent(columnPath.getColumn_family());
                if (columnPath.isSetSuper_column()) {
                	columnParent.setSuper_column(columnPath.getSuper_column());
                }
                
                Column column = new Column(ByteBuffer.wrap(columnPath.getColumn()));
                column.setTtl(ttlInSeconds);
                ByteBuffer key = StringSerializer.get().toByteBuffer(rowKey);
                keyspace.insert(key, columnParent, column);

                return null;
            }
        });
    }
    
    /**
     * Get all the key value pairs from a column family for a given row.
     *
     * @param rowKey           the row key
     * @param columnFamilyName the name of the column family
     * @param template         the cassandra template
     * @param keyspaceName     the keyspace name
     * @return a map of the key value pairs
     */
    public static Map<String, Object> getAllKeyValuePairs(final String rowKey,
                                                                    final String columnFamilyName,
                                                                    CassandraTemplate template,
                                                                    String keyspaceName) {
        return getAllKeyValuePairs(rowKey, columnFamilyName, null, template, keyspaceName);
    }


    /**
     * Get all the key value pairs from a super column for a given row.
     *
     * @param rowKey           the row key
     * @param columnFamilyName the name of the column family
     * @param superColumnName  the name of the super column
     * @param template         the cassandra template
     * @param keyspaceName     the name of the keyspace
     * @return a map containing the key value pairs
     */
    public static Map<String, Object> getAllKeyValuePairs(final String rowKey,
                                                                    final String columnFamilyName,
                                                                    final String superColumnName,
                                                                    CassandraTemplate template,
                                                                    String keyspaceName) {
        return template.read(keyspaceName, new KeyspaceCallback<Map<String, Object>>() {
            public Map<String, Object> execute(KeyspaceService keyspace) {
                ColumnParent columnParent = new ColumnParent(columnFamilyName);
                if (superColumnName != null) {
                    columnParent.setSuper_column(bytes(superColumnName));
                }

                List<Column> list = keyspace.getSlice(rowKey, columnParent, ALL_COLUMNS);
                Map<String, Object> props = new HashMap<String, Object>(list.size());
                for (Column c : list) {
                    props.put(string(c.getName()),
                              StructureSerializer.get().fromBytes(c.getValue()));
                }
                return props;
            }
        });
    }

    /**
     * Get one value from a column in a column family.
     *
     * @param rowKey           the row key
     * @param columnFamilyName the name of the column family
     * @param columnName       the name of the column (i.e. the key)
     * @param template         the cassandra template
     * @param keyspaceName     the name of the keyspace
     * @return the value
     */
    public static Object getOneValue(final String rowKey,
                                               final String columnFamilyName,
                                               final String columnName,
                                               CassandraTemplate template,
                                               String keyspaceName) {
        return getOneValue(rowKey, columnFamilyName, null, columnName, template, keyspaceName);
    }


    /**
     * Get one value from a column in a super column.
     *
     * @param rowKey           the row key
     * @param columnFamilyName the column family name
     * @param superColumnName  the name of the super column
     * @param columnName       the name of the subcolumn
     * @param template         the cassandra template
     * @param keyspaceName     the keyspace name
     * @return the value or null if the specified column can't be found
     */
    public static Object getOneValue(final String rowKey,
                                               final String columnFamilyName,
                                               @Nullable final String superColumnName,
                                               final String columnName,
                                               CassandraTemplate template,
                                               String keyspaceName) {
        return template.read(keyspaceName, new KeyspaceCallback<Object>() {
            public Object execute(KeyspaceService keyspace) {
                ColumnPath columnPath = createColumnPath(columnFamilyName, superColumnName, columnName);
                try {
                    Column column = keyspace.getColumn(rowKey, columnPath);
                    // allow for possibility that this could return null instead of throwing not found exception
                    if (column == null) {
                        return null;
                    }
                    return StructureSerializer.get().fromBytes(column.getValue());
                } catch (HNotFoundException e) {
                    return null;
                }
            }
        });
    }


    /**
     * Get one super column from each of multiple rows in a super column family.
     *
     * @param keys             the keys of the rows to get
     * @param columnFamilyName the name of the column family
     * @param superColumnName  the name of the super column as a String
     * @param template         the cassandra template
     * @param keyspaceName     the keyspace name
     * @return a map where the keys are the row keys and the values are the (one) super column from that row
     */
    public static Map<ByteBuffer, SuperColumn> multiGetOneSuperColumn(final List<ByteBuffer> keys,
                                                                      final String columnFamilyName,
                                                                      String superColumnName,
                                                                      CassandraTemplate template,
                                                                      String keyspaceName) {
        return multiGetOneSuperColumn(keys, columnFamilyName, bytes(superColumnName), template, keyspaceName);
    }

    /**
     * Get one super column from each of multiple rows in a super column family.
     *
     * @param keys             the keys of the rows to get
     * @param columnFamilyName the name of the column family
     * @param superColumnName  the name of the super column as a ByteBuffer
     * @param template         the cassandra template
     * @param keyspaceName     the keyspace name
     * @return a map where the keys are the row keys and the values are the (one) super column from that row
     */
    public static Map<ByteBuffer, SuperColumn> multiGetOneSuperColumn(final List<ByteBuffer> keys,
                                                                      final String columnFamilyName,
                                                                      final ByteBuffer superColumnName,
                                                                      CassandraTemplate template,
                                                                      String keyspaceName) {
        return template.read(keyspaceName, new KeyspaceCallback<Map<ByteBuffer, SuperColumn>>() {
            public Map<ByteBuffer, SuperColumn> execute(KeyspaceService keyspace) {
                ColumnPath columnPath = createColumnPath(columnFamilyName, superColumnName, null);
                return keyspace.multigetSuperColumn(keys, columnPath);
            }
        });
    }

    /**
     * Get one column from each of multiple rows in a column family.
     *
     * @param keys             the keys of the rows to get
     * @param columnFamilyName the name of the column family
     * @param columnName       the name of the column to get
     * @param template         the cassandra template
     * @param keyspaceName     the keyspace name
     * @return a map where the keys are the row keys and the values are the (one) column from that row
     */
    public static Map<ByteBuffer, Column> multiGetOneColumn(final List<ByteBuffer> keys,
                                                            final String columnFamilyName,
                                                            final String columnName,
                                                            CassandraTemplate template,
                                                            String keyspaceName) {
        return multiGetOneColumn(keys, columnFamilyName, null, bytes(columnName), template, keyspaceName);
    }

    /**
     * Get one column from each of multiple rows in a column or super-column family.
     *
     * @param keys             the keys of the rows to get
     * @param columnFamilyName the name of the column family
     * @param superColumnName  the name of the super column, or null if this is a column family
     * @param columnName       the name of the column to get
     * @param template         the cassandra template
     * @param keyspaceName     the keyspace name
     * @return a map where the keys are the row keys and the values are the (one) column from that row
     */
    public static Map<ByteBuffer, Column> multiGetOneColumn(final List<ByteBuffer> keys,
                                                            final String columnFamilyName,
                                                            final ByteBuffer superColumnName,
                                                            final ByteBuffer columnName,
                                                            CassandraTemplate template,
                                                            String keyspaceName) {

        Map<ByteBuffer, List<Column>> multiGetResult = template
                .read(keyspaceName, new KeyspaceCallback<Map<ByteBuffer, List<Column>>>() {
                    public Map<ByteBuffer, List<Column>> execute(KeyspaceService keyspace) {
                        ColumnParent columnParent = new ColumnParent(columnFamilyName);
                        if (superColumnName != null) {
                            columnParent.setSuper_column(superColumnName);
                        }
                        SlicePredicate slicePredicate = new SlicePredicate();
                        slicePredicate.setColumn_names(Collections.singletonList(columnName));
                        return keyspace.multigetSlice(keys, columnParent, slicePredicate);
                    }
                });

        // replace list of columns with single column
        Map<ByteBuffer, Column> result = new HashMap<ByteBuffer, Column>(multiGetResult.size());
        for (Map.Entry<ByteBuffer, List<Column>> entry : multiGetResult.entrySet()) {
            List<Column> columnList = entry.getValue();
            if (!columnList.isEmpty()) {
                assert columnList.size() == 1 : columnList.size();
                result.put(entry.getKey(), columnList.get(0));
            }
        }
        return result;
    }


    /**
     * Get a slice of SuperColumns from each of multiple rows in a super column family.
     *
     * @param keys             the keys of the rows to get
     * @param columnFamilyName the name of the column family
     * @param slicePredicate   the predicate specifying which super columns to get
     * @param template         the cassandra template
     * @param keySpaceName     the keyspace name
     * @return a map where the keys are the row keys and the value is the list of super columns from that row
     */

    public static Map<ByteBuffer, List<SuperColumn>> multiGetSuperSlice(final List<ByteBuffer> keys,
                                                                        final String columnFamilyName,
                                                                        final SlicePredicate slicePredicate,
                                                                        CassandraTemplate template,
                                                                        String keySpaceName) {
        return template.read(keySpaceName, new KeyspaceCallback<Map<ByteBuffer, List<SuperColumn>>>() {
            public Map<ByteBuffer, List<SuperColumn>> execute(KeyspaceService keyspace) {
                ColumnParent columnParent = new ColumnParent(columnFamilyName);
                return keyspace.multigetSuperSlice(keys, columnParent, slicePredicate);
            }
        });
    }


    /**
     * Gets all the SuperColumns for all rows in a Super Column Family up to rowLimit number of rows
     * starting at a given startKey.
     *
     * @param keyspaceName     the name of the keyspace
     * @param columnFamilyName the name of the column family
     * @param template         the cassandra template
     * @param startKey         the start key
     * @param rowLimit         the row limit
     * @return a map where the keys are the row keys and the value is a list of super columns for that row
     */
    // Should to use LinkedHashMap here instead of Map because it needs to be made known to the caller
    // that the map has ordered iteration, albeit arbitrary
    public static Map<ByteBuffer, List<SuperColumn>> getSuperRowsStartingAt(String keyspaceName,
                                                                            final String columnFamilyName,
                                                                            CassandraTemplate template,
                                                                            final String startKey,
                                                                            final int rowLimit) {
        return template.read(keyspaceName, new KeyspaceCallback<Map<ByteBuffer, List<SuperColumn>>>() {
            public Map<ByteBuffer, List<SuperColumn>> execute(KeyspaceService keyspace) {
                ColumnParent columnParent = new ColumnParent(columnFamilyName);
                SlicePredicate predicate = ALL_COLUMNS;
                KeyRange keyRange = new KeyRange(rowLimit);
                keyRange.setStart_key(bytes(startKey));
                keyRange.setEnd_key(bytes(""));
                return keyspace.getSuperRangeSlices(columnParent, predicate, keyRange);
            }
        });
    }


    /**
     * Gets all the Columns for all rows in a Column Family up to rowLimit number of rows
     * starting at a given startKey.
     *
     * @param keyspaceName     the name of the keyspace
     * @param columnFamilyName the name of the column family
     * @param template         the cassandra template
     * @param startKey         the start key
     * @param rowLimit         the row limit
     * @return a map where the keys are the row keys and the value is a list of super columns for that row
     */
    // Should use LinkedHashMap here instead of Map because it needs to be made known to the caller
    // that the map has ordered iteration, albeit arbitrary
    public static Map<ByteBuffer, List<Column>> getRowsStartingAt(String keyspaceName,
                                                                  final String columnFamilyName,
                                                                  CassandraTemplate template,
                                                                  final String startKey,
                                                                  final int rowLimit) {
        return template.read(keyspaceName, new KeyspaceCallback<Map<ByteBuffer, List<Column>>>() {
            public Map<ByteBuffer, List<Column>> execute(KeyspaceService keyspace) {
                ColumnParent columnParent = new ColumnParent(columnFamilyName);
                SlicePredicate predicate = ALL_COLUMNS;
                KeyRange keyRange = new KeyRange(rowLimit);
                keyRange.setStart_key(bytes(startKey));
                keyRange.setEnd_key(bytes(""));
                return keyspace.getRangeSlices(columnParent, predicate, keyRange);
            }
        });
    }


    /**
     * Get one super column.
     *
     * @param rowKey           the row key
     * @param columnFamilyName the column family name
     * @param superColumnName  the super column name
     * @param template         the cassandra template
     * @param keyspaceName     the name of the keyspace
     * @return the named super column, or null if not found
     */
    public static SuperColumn getSuperColumn(final String rowKey,
                                             final String columnFamilyName,
                                             final String superColumnName,
                                             CassandraTemplate template,
                                             String keyspaceName) {
        return template.read(keyspaceName, new KeyspaceCallback<SuperColumn>() {
            public SuperColumn execute(KeyspaceService keyspace) {
                ColumnPath columnPath = createColumnPath(columnFamilyName, superColumnName, null);
                try {
                    return keyspace.getSuperColumn(rowKey, columnPath);
                } catch (HNotFoundException e) {
                    return null;
                }
            }
        });
    }

    /**
     * Gets all the columns in a column family.
     *
     * @param rowKey           the row key
     * @param columnFamilyName the column family name
     * @param template         the cassandra template
     * @param keyspaceName     the keyspace name
     * @return the list of columns
     */
    @SuppressWarnings({"UnusedDeclaration"})//NOSONAR
    public static List<Column> getSlice(String rowKey,
                                        String columnFamilyName,
                                        CassandraTemplate template,
                                        String keyspaceName) {
        return getSlice(rowKey, columnFamilyName, null, ALL_COLUMNS, template, keyspaceName);
    }

    /**
     * Gets the columns in a column family or from a supercolumn based on a predicate.
     *
     * @param rowKey           the row key
     * @param columnFamilyName the column family name
     * @param superColumnName  optional super column name, if present get columns from a super column, if not then from a column family
     * @param slicePredicate   slice predicate
     * @param template         the cassandra template
     * @param keyspaceName     the keyspace name
     * @return the list of columns
     */
    public static List<Column> getSlice(final String rowKey,
                                        final String columnFamilyName,
                                        @Nullable final String superColumnName,
                                        final SlicePredicate slicePredicate,
                                        CassandraTemplate template,
                                        String keyspaceName) {
        return template.read(keyspaceName, new KeyspaceCallback<List<Column>>() {
            public List<Column> execute(KeyspaceService keyspace) {
                ColumnParent columnParent = new ColumnParent(columnFamilyName);
                if (superColumnName != null) {
                    columnParent.setSuper_column(bytes(superColumnName));
                }
                return keyspace.getSlice(rowKey, columnParent, slicePredicate);
            }
        });
    }


    /**
     * Gets all the supercolumns in a super column family for a given row.
     *
     * @param rowKey           the  row
     * @param columnFamilyName the name of the column family
     * @param template         the cassandra template
     * @param keyspaceName     the keyspace name
     * @return the list of super columns
     */
    public static List<SuperColumn> getSuperSlice(final String rowKey,
                                                  final String columnFamilyName,
                                                  CassandraTemplate template,
                                                  String keyspaceName) {
        return template.read(keyspaceName, new KeyspaceCallback<List<SuperColumn>>() {
            public List<SuperColumn> execute(KeyspaceService keyspace) {
                ColumnParent columnParent = new ColumnParent(columnFamilyName);
                return keyspace.getSuperSlice(rowKey, columnParent, ALL_COLUMNS);
            }
        });
    }

    /**
     * Get the supercolumns in a super column family for a given row based on a predicate.
     *
     * @param rowKey           the  row
     * @param columnFamilyName the name of the column family
     * @param slicePredicate   the predicate
     * @param template         the cassandra template
     * @param keyspaceName     the keyspace name
     * @return the list of super columns
     */
    public static List<SuperColumn> getSuperSlice(final String rowKey,
                                                  final String columnFamilyName,
                                                  final SlicePredicate slicePredicate,
                                                  CassandraTemplate template,
                                                  String keyspaceName) {
        return template.read(keyspaceName, new KeyspaceCallback<List<SuperColumn>>() {
            public List<SuperColumn> execute(KeyspaceService keyspace) {
                ColumnParent columnParent = new ColumnParent(columnFamilyName);
                return keyspace.getSuperSlice(rowKey, columnParent, slicePredicate);
            }
        });
    }

    /**
     * Return a system-wide unique network id as a String.
     *
     * @param network   the (canonical) name of the network
     * @param networkId the network id within this network
     * @param <T>       the type of the networkId
     * @return the unique network id as a string
     */
    public static <T> String extendedNetworkId(String network, T networkId) {
        return network + "_" + networkId;
    }

    /**
     * Given an extended network id as returned by extendedNetworkId, extract the network
     * local network id as a String.
     *
     * @param extNetworkId the extended network id
     * @return the local network id as a String
     */
    public static String networkId(String extNetworkId) {
        int index = extNetworkId.indexOf('_');
        if (index < 0) {
            throw new IllegalArgumentException("nonconforming extended network id:" + extNetworkId);
        }
        return extNetworkId.substring(index + 1);
    }

    public static String network(String extNetworkId) {
        int index = extNetworkId.indexOf('_');
        if (index < 0) {
            throw new IllegalArgumentException("nonconforming extended network id:" + extNetworkId);
        }
        return extNetworkId.substring(0, index);
    }

    /**
     * For adding properties to a mutation map in a super column.
     *
     * @param rowKey           the row rowKey
     * @param columnFamilyName the name of the column family
     * @param superColumnName  the name of the supercolumn as a String, or null if this is not a super column family
     * @param propertyKey      the name of the property key
     * @param propertyValue    the String value of the property
     * @param batchMutation    the BatchMutation map to add to
     * @param timestamp        the timestamp
     */
    public static void storeOneValueToBatchMutation(String rowKey,
                                                    String columnFamilyName,
                                                    @Nullable String superColumnName,
                                                    String propertyKey,
                                                    String propertyValue,
                                                    BatchMutation<String> batchMutation,
                                                    long timestamp) {
        Column column = new Column(bytes(propertyKey));
        column.setValue(bytes(propertyValue));
        column.setTimestamp(timestamp);
        List<Column> columns = Collections.singletonList(column);

        if (superColumnName != null) {
            SuperColumn superColumn = new SuperColumn(bytes(superColumnName), columns);
            batchMutation.addSuperInsertion(rowKey, Collections.singletonList(columnFamilyName), superColumn);
        } else {
            batchMutation.addInsertion(rowKey, Collections.singletonList(columnFamilyName), column);
        }
    }


    /**
     * Store key value pairs for a column family to a mutation map, but don't execute.
     *
     * @param rowKey           the row key
     * @param columnFamilyName the column family name
     * @param keyValuePairs    map with the key value pairs
     * @param template         the cassandra template
     * @param batchMutation    the BatchMutation wherein to put the mutations, which can be executed later
     */
    @SuppressWarnings({"UnusedDeclaration"})//NOSONAR
    public static void storeKeyValuePairsToBatchMutation(String rowKey,
                                                         String columnFamilyName,
                                                         Map<String, Object> keyValuePairs,
                                                         CassandraTemplate template,
                                                         BatchMutation<String> batchMutation) {
        storeKeyValuePairsToBatchMutation(rowKey,
                                          columnFamilyName,
                                          (String) null,
                                          keyValuePairs,
                                          template,
                                          batchMutation);
    }

    /**
     * Store key value pairs for a super column to a mutation map, but don't execute.
     *
     * @param rowKey           the row key
     * @param columnFamilyName the column family name
     * @param superColumnName  the super column name as a String
     * @param keyValuePairs    map with the key value pairs, keys can be hierarchical using a delimiter;
     * @param template         the cassandra template
     * @param batchMutation    the BatchMutation wherein to put the mutations, which can be executed later
     */
    public static void storeKeyValuePairsToBatchMutation(String rowKey,
                                                         String columnFamilyName,
                                                         String superColumnName,
                                                         Map<String, Object> keyValuePairs,
                                                         CassandraTemplate template,
                                                         BatchMutation<String> batchMutation) {
        storeKeyValuePairsToBatchMutation(rowKey,
                                          columnFamilyName,
                                          superColumnName == null ? null : bytes(superColumnName),
                                          keyValuePairs,
                                          template,
                                          batchMutation);
    }

    /**
     * Store key value pairs for a super column to a mutation map, but don't execute.
     *
     * @param rowKey           the row key
     * @param columnFamilyName the column family name
     * @param superColumnName  the super column name as a byte[]
     * @param keyValuePairs    map with the key value pairs, keys can be hierarchical using a delimiter;
     * @param template         the cassandra template
     * @param batchMutation    the BatchMutation wherein to put the mutations, which can be executed later
     */
    public static void storeKeyValuePairsToBatchMutation(String rowKey,
                                                         String columnFamilyName,
                                                         ByteBuffer superColumnName,
                                                         Map<String, Object> keyValuePairs,
                                                         CassandraTemplate template,
                                                         BatchMutation<String> batchMutation) {

        if (superColumnName == null) {
            addPropertiesToBatchMutation(rowKey,
                                         columnFamilyName,
                                         keyValuePairs,
                                         batchMutation,
                                         template.createTimestamp());
        } else {
            addSuperPropertiesToBatchMutation(rowKey,
                                              columnFamilyName,
                                              superColumnName,
                                              keyValuePairs,
                                              batchMutation,
                                              template.createTimestamp());
        }
    }

    /**
     * Add row deletion to a batch mutation for later execution.
     *
     * @param rowKey           the row key to delete
     * @param columnFamilyName the column family name
     * @param template         the cassandra template
     * @param batchMutation    the BatchMutation wherein to put the mutations, which can be executed later
     */
    @SuppressWarnings({"UnusedDeclaration"})//NOSONAR
    public static void deleteRowToBatchMutation(String rowKey,
                                                String columnFamilyName,
                                                CassandraTemplate template,
                                                BatchMutation<String> batchMutation) {
        Deletion deletion = new Deletion();
        deletion.setTimestamp(template.createTimestamp());
        batchMutation.addDeletion(rowKey, Collections.singletonList(columnFamilyName), deletion);
    }

    /**
     * Remove a column or supercolumn in a column family (or super column family).
     *
     * @param columnFamilyName the name of the column family
     * @param rowKey           the row key
     * @param superColumnName  the super column name, or null
     * @param columnName       the columnName, or null if deleting all columns
     * @param template         the cassandra template
     * @param batchMutation    the batch mutation
     */
    public static void deleteColumnToBatchMutation(String rowKey,
                                                   String columnFamilyName,
                                                   String superColumnName,
                                                   @Nullable String columnName,
                                                   CassandraTemplate template,
                                                   BatchMutation<String> batchMutation) {
        Deletion deletion = new Deletion();
        deletion.setTimestamp(template.createTimestamp());
        if (superColumnName != null) {
            deletion.setSuper_column(bytes(superColumnName));
        }
        if (columnName != null) {
            SlicePredicate slicePredicate = new SlicePredicate();
            slicePredicate.setColumn_names(Collections.singletonList(bytes(columnName)));
            deletion.setPredicate(slicePredicate);
        }
        batchMutation.addDeletion(rowKey, Collections.singletonList(columnFamilyName), deletion);
    }


    /**
     * Use the properties from the subcolumns of a SuperColumn to create a Map<String, Object>.
     *
     * @param sc           the super column to get the properties from
     * @return the newly created Map<String, Object>
     */
    public static Map<String, Object> createPropertiesFromSuperColumn(SuperColumn sc) {
        Map<String, Object> properties = new HashMap<String, Object>();
        for (Column column : sc.getColumns()) {
            properties.put(string(column.getName()), StructureSerializer.get().fromBytes(column.getValue()));
        }
        return properties;
    }

    /**
     * Convert byte array to string using UTF-8 encoding.
     *
     * @param b the byte array
     * @return the string
     */
    public static String string(byte[] b) {
        // optimize the empty string case by returning canonical empty string
        if (b != null && b.length == 0) {
            return "";
        }
        return StringUtils.string(b);
    }


    /**
     * Convert ByteBuffer to string using UTF-8 encoding.
     *
     * @param b the ByteBuffer
     * @return the string
     */
    public static String string(ByteBuffer b) {
        // optimize the empty string case by returning canonical empty string
        if (b == null) {
            return "";
        }
        byte[] data = new byte[b.remaining()];
        b.get(data);
        b.rewind();
        return string(data);
    }


    /**
     * Convert a string to a byte array using UTF-8 encoding.
     *
     * @param s the string
     * @return the byte array
     */
    public static ByteBuffer bytes(String s) {
        // the hector version of bytes() in StringUtils does not handle null properly
        if (s == null) {
            return EMPTY_BYTES;
        }
        // also, optimize the empty string case
        if (s.isEmpty()) {
            return EMPTY_BYTES;
        }

        return ByteBuffer.wrap(StringUtils.bytes(s));
    }

    private static ColumnPath createColumnPath(String columnFamily,
                                               @Nullable String superColumnName,
                                               String columnName) {
        return createColumnPath(columnFamily, superColumnName == null ? null : bytes(superColumnName), columnName);
    }

    private static ColumnPath createColumnPath(String columnFamily, ByteBuffer superColumnName, String columnName) {
        ColumnPath columnPath = new ColumnPath(columnFamily);
        if (superColumnName != null) {
            columnPath.setSuper_column(superColumnName);
        }
        if (columnName != null) {
            columnPath.setColumn(bytes(columnName));
        }
        return columnPath;
    }

    /**
     * For adding properties to a mutation map in a column.
     *
     * @param rowKey           the row rowKey
     * @param columnFamilyName the name of the column family
     * @param properties       the properties to add to the mutation map
     * @param batchMutation    the BatchMutation to add to
     * @param timestamp        the timestamp
     */
    private static void addPropertiesToBatchMutation(String rowKey,
                                                     String columnFamilyName,
                                                     Map<String, Object> properties,
                                                     BatchMutation<String> batchMutation,
                                                     long timestamp) {
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            Column column = new Column(bytes(entry.getKey()));
            column.setValue(StructureSerializer.get().toByteBuffer(entry.getValue()));
            column.setTimestamp(timestamp);
            batchMutation.addInsertion(rowKey, Collections.singletonList(columnFamilyName), column);
        }
    }

    /**
     * For adding properties to a mutation map in a super column.
     *
     * @param rowKey           the row rowKey
     * @param columnFamilyName the name of the column family
     * @param superColumnName  the name of the supercolumn as a byte[]
     * @param properties       the properties to add to the mutation map
     * @param batchMutation    the BatchMutation map to add to
     * @param timestamp        the timestamp
     */
    private static void addSuperPropertiesToBatchMutation(String rowKey,
                                                          String columnFamilyName,
                                                          ByteBuffer superColumnName,
                                                          Map<String, Object> properties,
                                                          BatchMutation<String> batchMutation,
                                                          long timestamp) {
        if (superColumnName == null) {
            throw new IllegalArgumentException("superColumnName can not be null");
        }
        List<Column> columns = new ArrayList<Column>(properties.size());
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            if (entry.getValue() != null) { //Skip null values
                Column column = new Column(bytes(entry.getKey()));
                column.setValue(StructureSerializer.get().toByteBuffer(entry.getValue()));
                column.setTimestamp(timestamp);
                columns.add(column);
            }
        }

        SuperColumn superColumn = new SuperColumn(superColumnName, columns);
        batchMutation.addSuperInsertion(rowKey, Collections.singletonList(columnFamilyName), superColumn);
    }


    /**
     * Remove all rows from a column family.
     *
     * @param keyspaceName     the name of the keyspace
     * @param columnFamilyName the name of the column family.
     * @param template         the cassandra template
     */
    @SuppressWarnings({"UnusedDeclaration", "UnusedParameters"})//NOSONAR
    public static void truncate(String keyspaceName, String columnFamilyName, CassandraTemplate template) {
        template.write(keyspaceName, new KeyspaceCallback<Void>() {

            @Override
            public Void execute(KeyspaceService keyspace) {
                throw new UnsupportedOperationException("Not yet available in Hector");
            }
        });
    }


    /////////////////////  BEGIN TRUNCATE IMPLEMENTATION: To Be Removed with Cassandra 0.7+ //////////

    /**
     * Remove all rows from a super column family. This is a temporary method until Cassandra 0.7,
     * when the truncate method can be used instead.
     *
     * @param keyspaceName     the name of the keyspace
     * @param columnFamilyName the name of the column family
     * @param template         the cassandra template
     */

    @SuppressWarnings({"UnusedDeclaration"})
    public static void truncateSuperColumnFamily(String keyspaceName,
                                                 final String columnFamilyName,
                                                 CassandraTemplate template) {
        String startKey = "";
        int batchSize = 500;

        // get rows in batches, overlapping by one row
        Map<ByteBuffer, List<SuperColumn>> rowMap;

        boolean firstBatch = true;
        do {

            rowMap = getSuperRowsStartingAt(keyspaceName,
                                            columnFamilyName,
                                            template,
                                            startKey,
                                            batchSize);

            deleteAllRowsInSuperColumnFamily(keyspaceName, columnFamilyName, rowMap, template);

            //if null the loop will break anyway  because rowMap is empty
            startKey = removeOverlapFromProcessedBatch(rowMap, firstBatch);
            firstBatch = false;
        } while (rowMap.size() > 1);
    }


    private static void deleteAllRowsInSuperColumnFamily(String keyspace,
                                                         String columnFamilyName,
                                                         Map<ByteBuffer, List<SuperColumn>> rowMap,
                                                         CassandraTemplate template) {
        final BatchMutation<String> batchMutation = new BatchMutation<String>(StringSerializer.get());
        // for each row, add a Deletion Mutation
        for (ByteBuffer rowKey : rowMap.keySet()) {
            deleteRowToBatchMutation(string(rowKey),
                                     columnFamilyName,
                                     template,
                                     batchMutation);
        }
        mutate(batchMutation, keyspace, template);
    }

    /**
     * Basically this is to fix an issue with the API we are using to read from cassandra
     * since we are not using an ordered reading we need to depend on the paging order
     * so the order in which the data is saved on desk.
     * so when reading the data the start key for the new batch will be always the last row
     * of the old batch ... what we are doing here we are trying to skip it by return the start key in the one after
     *
     * @param rowMap     -- LinkedHashMap<String, ?> list of the batch we are going to work with
     * @param firstBatch -- boolean is this my 1st batch if so don't skip 1st
     * @return String with the starting key, or null if the rowMap is empty
     */
    private static String removeOverlapFromProcessedBatch(Map<ByteBuffer, List<SuperColumn>> rowMap,
                                                          boolean firstBatch) {
        String startKey = null;
        // if this is not the first batch, then remove the first row
        // remember the last key as the startKey for the next batch
        if (!rowMap.isEmpty()) {
            List<ByteBuffer> rowList = new ArrayList<ByteBuffer>(rowMap.keySet());
            startKey = string(rowList.get(rowMap.size() - 1));
            if (!firstBatch) {
                rowMap.remove(rowList.get(0));
            }
        }
        return startKey;
    }

    /////////////////////  END TRUNCATE IMPLEMENTATION: To Be Removed with Cassandra 0.7+ //////////
}
