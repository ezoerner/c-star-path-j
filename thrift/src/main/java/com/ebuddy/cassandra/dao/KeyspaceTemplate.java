package com.ebuddy.cassandra.dao;

import javax.annotation.Nonnull;

import org.apache.commons.lang.Validate;

import com.ebuddy.cassandra.HectorExceptionTranslator;
import com.ebuddy.cassandra.NoSQLExceptionTranslator;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * Template for a keyspace. Implements methods that operate on a Keyspace.
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */

// See comment in AbstractColumnFamilyTemplate.
public class KeyspaceTemplate<K> implements KeyspaceOperations {
    /**
     * The exception translator to translate HectorExceptions to Spring
     * Data Access exceptions.
     */
    protected static final NoSQLExceptionTranslator<HectorException> EXCEPTION_TRANSLATOR
            = new HectorExceptionTranslator();
    /**
     * The keyspace for operations.
     */
    protected final Keyspace keyspace;
    /**
     * The serializer for row keys.
     */
    protected final Serializer<K> keySerializer;

    public KeyspaceTemplate(Keyspace keyspace, Serializer<K> keySerializer) {
        this.keyspace = keyspace;
        this.keySerializer = keySerializer;
    }

    /**
     * Create a TransactionContext for use with batch operations.
     */
    @Override
    public final TransactionContext begin() {
        return new HectorTransactionContext(keyspace, keySerializer);
    }

    /**
     * Execute a batch.
     *
     * @param txnContext the  TransactionContext
     */
    @Override
    public final void commit(@Nonnull TransactionContext txnContext) {
        Mutator<K> mutator = validateAndGetMutator(txnContext);
        Validate.notNull(mutator);
        try {
            mutator.execute();
        } catch (HectorException e) {
            throw EXCEPTION_TRANSLATOR.translate(e);
        }
    }

    protected final Mutator<K> validateAndGetMutator(TransactionContext txnContext) {
        if (txnContext == null) {
            return null;
        }

        //noinspection UnnecessarilyQualifiedInnerClassAccess
        Validate.isTrue(txnContext instanceof KeyspaceTemplate.HectorTransactionContext,
                        "TransactionContext not valid for this DAO implementation");

        @SuppressWarnings({"unchecked", "ConstantConditions"})
        HectorTransactionContext htc = (HectorTransactionContext)txnContext;

        htc.validateSameKeyspace(keyspace);
        return htc.getMutator();
    }

    class HectorTransactionContext implements TransactionContext {
        final Mutator<K> mutator;

        private HectorTransactionContext(Keyspace keyspace, Serializer<K> keySerializer) {
            mutator = HFactory.createMutator(keyspace, keySerializer);
        }

        Mutator<K> getMutator() {
            return mutator;
        }

        void validateSameKeyspace(Keyspace keyspace) {
            if (KeyspaceTemplate.this.keyspace != keyspace) {
                throw new IllegalArgumentException("This transaction object was created for a different keyspace");
            }
        }
    }
}
