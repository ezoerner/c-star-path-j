package com.ebuddy.cassandra.dao;

import javax.annotation.Nullable;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.exceptions.HectorException;
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
    protected final Serializer<V> valueSerializer;

    /**
     * The column family for this DAO, or null if not specific to a single column family.
     */
    @Nullable
    protected final String columnFamily;

    /**
     * The serializer for a standard column name or a super-column name, or null if not specific to one column family.
     */
    @Nullable
    protected final Serializer<N> topSerializer;

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
     * Remove the entire row.
     * @param rowKey the row key
     * @param txnContext optional TransactionContext
     */
    public final void removeRow(K rowKey,
                                @Nullable TransactionContext txnContext) {
        Mutator<K> mutator = validateAndGetMutator(txnContext);
        try {
            if (mutator == null) {
                createMutator().delete(rowKey, columnFamily, null, null);
            } else {
                mutator.addDeletion(rowKey, columnFamily);
            }
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
    }

    protected final Mutator<K> createMutator() {
        return HFactory.createMutator(keyspace, keySerializer);
    }


}
