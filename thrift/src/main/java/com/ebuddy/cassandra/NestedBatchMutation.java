package com.ebuddy.cassandra;

import com.ebuddy.cassandra.property.PropertyValue;
import com.ebuddy.cassandra.property.PropertyValueFactory;
import org.apache.cassandra.thrift.*;
import org.apache.commons.collections.KeyValue;
import org.apache.commons.collections.keyvalue.DefaultKeyValue;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.util.*;

import static com.ebuddy.cassandra.HectorUtils.*;

/**
 * An NestedBatchMutation encapsulates a set of updates/insertions/deletions all submitted
 * at the same time to Cassandra.
 * <p/>
 * Similar to the Hector BatchMutation, but this implementation also supports a delta
 * insertion to an embedded Map object, to support hierarchically nested properties.
 * <p/>
 * A NestedBatchMutation is also limited to a single row and single column family.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 * @deprecated use DAO objects and ExtendedNetworkId instead
 *
 */
@Deprecated
class NestedBatchMutation {
    private final CassandraTemplate cassandraTemplate;
    private final String keySpace;
    private final String rowKey;
    private final String columnFamily;
    private final String superColumnName; // may be null

    /** map of mutations, new values keyed by column name. */
    private final Map<String, ByteBuffer> mutations = new HashMap<String, ByteBuffer>();

    /** map of partial mutations, keyed by column name. */
    private final Map<String, PartialMutation> partials = new HashMap<String, PartialMutation>();
    private boolean partialsApplied = false;



    /**
     * Construct a NestedBatchMutation.
     *
     * @param cassandraTemplate
     * @param keySpace
     * @param rowKey the row key
     * @param columnFamily the column family
     * @param superColumnName the super column name, or null if this is a regular column family
     */
    public NestedBatchMutation(CassandraTemplate cassandraTemplate, String keySpace, String rowKey, String columnFamily, String superColumnName) {
        this.cassandraTemplate = cassandraTemplate;
        this.keySpace = keySpace;
        if(rowKey == null) {
            throw new IllegalArgumentException("rowKey should not be null");
        }
        if (columnFamily == null) {
            throw new IllegalArgumentException("columnFamily should not be null");
        }
        this.rowKey = rowKey;
        this.columnFamily = columnFamily;
        this.superColumnName = superColumnName;
    }

    /**
     * Add an Column insertion (or update) to the batch mutation request.
     *
     * @param path the property name, can be hierarchical using delimiter
     * @param value the new String value for the property
     * @return the receiver
     */
    public NestedBatchMutation addInsertion(String path, String value, String delimiter) {
        if (partialsApplied) {
            throw new IllegalStateException("cannot insert after getMutationMap() is called");
        }
        if (path.indexOf(delimiter) >= 0) {
            // first part of path is the column name, extract rest for the relative path
            String[] parts = splitFastAndURLDecode(path,delimiter);
            int restLen = parts.length - 1;
            String[] rest = new String[restLen];
            System.arraycopy(parts, 1, rest, 0, restLen);
            insertPartialMutation(parts[0], rest, value);
        } else {
            mutations.put(urlDecode(path), bytes(value));
        }
        return this;
    }



    /**
     * Calculate and return the mutation map for updating Cassandra.
     * For hierarchical properties, reads the current value from Cassandra, then applies the nested properties to it.
     *
     * @return the mutation map
     */
    Map<ByteBuffer, Map<String, List<Mutation>>> getMutationMap() {
        if (!partials.isEmpty()) {
            fillInOldValues();
            buildMutationsFromPartials();
        }
        
        return Collections.unmodifiableMap(createMutationMapIgnoringPartials());
    }

    private void fillInOldValues() {
        // incorporate the partial mutations into the list of mutations.
        // The old values need to be retrieved first with a single
        // getSlice. Build up a SlicePredicate to fill in the old values.

        SlicePredicate slicePredicate = new SlicePredicate();

        for (Map.Entry<String, PartialMutation> partialKeyValue : partials.entrySet()) {
            // the columnName is the key of the map entry
            String columnName = partialKeyValue.getKey();
            // add this column to the predicate
            slicePredicate.addToColumn_names(bytes(columnName));
        }

        // read old values from Cassandra
        List<Column> columns = getSlice(rowKey,
                                        columnFamily,
                                        superColumnName,
                                        slicePredicate,
                                        cassandraTemplate,
                                        keySpace);

        for (Column column : columns) {
            String columnName = string(column.getName());
            partials.get(columnName).oldValue = column.bufferForValue();
        }
    }


    private void buildMutationsFromPartials() {
        for (Map.Entry<String, PartialMutation> partialKeyValue : partials.entrySet()) {
            PartialMutation partialMutation = partialKeyValue.getValue();

            PropertyValue<?> oldValuePropertyValue = null;
            ByteBuffer oldValueBytes = partialMutation.oldValue;
            if (oldValueBytes != null) {
                 oldValuePropertyValue = PropertyValueFactory.get().createPropertyValue(oldValueBytes);
            }

            Map<String, PropertyValue<?>> topMap;
            if (oldValuePropertyValue == null || !oldValuePropertyValue.isNestedProperties()) {
                // if not nested properties, then storing a nested property ON TOP of a non-nested value.
                // in this case, ignore the old value, will be overwritten with a new NestedProperties
                partialMutation.oldValue = null;
                topMap = new HashMap<String, PropertyValue<?>>();
            } else {
                @SuppressWarnings({"unchecked"})
                PropertyValue<Map<String, PropertyValue<?>>> nestedProps =
                        (PropertyValue<Map<String, PropertyValue<?>>>)oldValuePropertyValue;
                topMap = nestedProps.getValue();
            }
            
            ByteBuffer newValue = applyPathsToNestedMaps(partialMutation.relativePaths, topMap);
            this.mutations.put(partialKeyValue.getKey(), newValue);
        }

        // remember that partials have been applied so no new partials can be added
        partialsApplied = true;
    }

    /**
     * Apply relative paths to nested maps and return the new value for this column as a byte[].
     * 
     * @param relativePaths the relative paths with values to apply
     * @param topMap the top nested map
     * @return the byte[] for new column value
     */
    private ByteBuffer applyPathsToNestedMaps(List<KeyValue> relativePaths, Map<String, PropertyValue<?>> topMap) {
        for (KeyValue relativePathKeyValue : relativePaths) {
            applyPathToNestedMap(relativePathKeyValue, topMap);
        }
        return PropertyValueFactory.get().createPropertyValue(topMap).toBytes();
    }

    // modified topMap in place
    private void applyPathToNestedMap(KeyValue relativePathKeyValue, Map<String, PropertyValue<?>> topMap) {
        String[] parts = (String[])relativePathKeyValue.getKey();
        Map<String, PropertyValue<?>> thisMap = topMap;
        String lastPart = parts[parts.length - 1]; // key for leaf value
        for (String part : parts) {
            if (part.equals(lastPart)) {
                // on the leaf part, so store in map here
                thisMap.put(part, PropertyValueFactory.get().createPropertyValue((String)relativePathKeyValue.getValue()));
            } else {
                PropertyValue<?> nextValue = thisMap.get(part);
                PropertyValue<Map<String, PropertyValue<?>>> nextNestedPropertyValue;
                if (nextValue == null || !nextValue.isNestedProperties()) {
                    // unable to drill down here, so overwrite whatever was there with new map
                    Map<String, PropertyValue<?>> newMap = new HashMap<String, PropertyValue<?>>();
                    nextNestedPropertyValue = PropertyValueFactory.get().createPropertyValue(newMap);
                    thisMap.put(part, nextNestedPropertyValue);
                } else {
                    //noinspection unchecked
                    nextNestedPropertyValue = (PropertyValue<Map<String, PropertyValue<?>>>)nextValue;
                }
                // drill down
                thisMap = nextNestedPropertyValue.getValue();

            }
        }
    }


    /**
     * Build a standard Cassandra mutation map out of the list of mutations. The partials are assumed
     * to have already been applied.
     * 
     * @see #getMutationMap
     * @return mutation map
     */
    private Map<ByteBuffer, Map<String, List<Mutation>>> createMutationMapIgnoringPartials() {
        assert partials.isEmpty() || partialsApplied;
        Map<ByteBuffer, Map<String, List<Mutation>>> result = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

        Map<String, List<Mutation>> innerMutationMap = new HashMap<String, List<Mutation>>();
        
        List<Mutation> mutationList = new ArrayList<Mutation>(mutations.size());
        long timestamp = cassandraTemplate.createTimestamp();
        if (superColumnName != null) {
            List<Column> columns = new ArrayList<Column>();
            Mutation mutation = new Mutation();

            for (Map.Entry<String, ByteBuffer> entry : mutations.entrySet()) {
                Column column = new Column(bytes(entry.getKey()));
                column.setValue(entry.getValue());
                column.setTimestamp(timestamp);
                columns.add(column);
            }

            SuperColumn superColumn = new SuperColumn(bytes(superColumnName), columns);
            mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn().setSuper_column(superColumn));
            mutationList.add(mutation);
        } else {
            for (Map.Entry<String, ByteBuffer> entry : mutations.entrySet()) {
                Column column = new Column(bytes(entry.getKey()));
                column.setValue(entry.getValue());
                column.setTimestamp(timestamp);
                Mutation mutation = new Mutation();
                mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));
                mutationList.add(mutation);
            }
        }
        innerMutationMap.put(columnFamily, mutationList);
        result.put(bytes(rowKey), innerMutationMap);
        return result;
    }


    private void insertPartialMutation(String columnName, String[] relativePath, String newValue) {
        // see if there is an existing partial mutation
        PartialMutation partialMutation = partials.get(columnName);
        if (partialMutation == null) {
            partialMutation = new PartialMutation();
            partials.put(columnName, partialMutation);
        }
        partialMutation.relativePaths.add(new DefaultKeyValue(relativePath, newValue));
    }

    private static String[] splitFastAndURLDecode(String toSplit,String delimiter) {
        StringTokenizer strTok = new StringTokenizer(toSplit, delimiter, false);
        String[] values = new String[strTok.countTokens()];
        int idx = 0;
        while(strTok.hasMoreTokens()) {
            values[idx++] = urlDecode(strTok.nextToken());
        }
        return values;
    }

    private static String urlDecode(String s) {
        try {
          return URLDecoder.decode(s, "utf-8");
        } catch (UnsupportedEncodingException e) {
            // this will never happen since the character encoding name is pre-verified
            AssertionError ae = new AssertionError("never happens");
            ae.initCause(e);
            throw ae;
        }
    }


    private static class PartialMutation {
        private ByteBuffer oldValue;
        // key is a String[] hierarchical path , value is a String
        private final List<KeyValue> relativePaths;

        PartialMutation() {
            this.relativePaths = new ArrayList<KeyValue>();
        }
    }

}
