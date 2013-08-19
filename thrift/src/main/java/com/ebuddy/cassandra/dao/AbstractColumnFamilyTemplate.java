package com.ebuddy.cassandra.dao;

import javax.annotation.Nullable;

import com.ebuddy.cassandra.BatchContext;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * Abstract superclass for Cassandra Data Access templates.
 *
 * @param <K> the type of row key
 * @param <N> the type of top column name (super column name for Super Column Families);
 *            use Void if this is not specific to a column family
 * @param <V> the type of column value;
 *            use Void if this is not specific to a column family
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */

// Extends KeyspaceTemplate as an implementation convenience to inherit the implementations of begin and commit
// If in the future we add more operations to KeyspaceOperations, then we will need to split keyspace template
// so this class doesn't inherit methods only appropriate for a keyspace.
public abstract class AbstractColumnFamilyTemplate<K,N,V> extends KeyspaceTemplate<K> {

    /**
     * Used for queries where we just ask for all columns.
     */
    protected static final int ALL = Integer.MAX_VALUE;

    /**
     * The serializer used for column values, or null if not specific to one column family..
     */
    @Nullable
    private final Serializer<V> valueSerializer;


    /**
     * The serializer for a standard column name or a super-column name, or null if not specific to one column family.
     */
    @Nullable
    private final Serializer<N> topSerializer;

    /**
     * The default column family for this DAO, or null if not specific to a single column family.
     */
    @Nullable
    private final String columnFamily;

    /**
     * Constructor.
     *
     * @param keyspace      the Keyspace
     * @param columnFamily  the name of the column family or null if this is not specific to a column family.
     * @param keySerializer the serializer for row keys
     * @param topSerializer the serializer for the top columns (columns for a Column Family,
     *                      superColumns for a Super Column Family).
     *                      If null, then this instance is not specific to one Column Family.
     */
    protected AbstractColumnFamilyTemplate(Keyspace keyspace,
                                           @Nullable String columnFamily,
                                           Serializer<K> keySerializer,
                                           @Nullable Serializer<N> topSerializer,
                                           @Nullable Serializer<V> valueSerializer) {
        super(keyspace, keySerializer);
        this.topSerializer = topSerializer;
        this.columnFamily = columnFamily;
        this.valueSerializer = valueSerializer;
    }


    /**
     * Remove the entire row.
     * @param rowKey the row key
     */
    @SuppressWarnings("UnusedDeclaration")
    public final void removeRow(K rowKey) {
        removeRow(rowKey, null);
    }

    /**
     * Remove the entire row in the default column family.
     * @param rowKey the row key
     * @param batchContext optional BatchContext
     */
    public final void removeRow(K rowKey,
                                @Nullable BatchContext batchContext) {
        Mutator<K> mutator = validateAndGetMutator(batchContext);

        if (mutator == null) {
            createMutator().delete(rowKey, columnFamily, null, null);
        } else {
            mutator.addDeletion(rowKey, columnFamily);
        }
        // we used to translate hector exceptions into spring exceptions here, but spring dependency was removed
    }

    protected final Mutator<K> createMutator() {
        return HFactory.createMutator(getKeyspace(), getKeySerializer());
    }

    protected final Serializer<V> getValueSerializer() {
        // the assumption is that if this method is called then null is not acceptable
        if (valueSerializer == null) {
            throw new IllegalStateException("value serializer is null");
        }
        return valueSerializer;
    }

    protected final Serializer<N> getTopSerializer() {
        // the assumption is that if this method is called then null is not acceptable
        if (topSerializer == null) {
            throw new IllegalStateException("top serializer is null");
        }
        return topSerializer;
    }

    @Nullable
    public String getColumnFamily() {
        return columnFamily;
    }
}
