package com.ebuddy.cassandra.dao;

import javax.annotation.Nonnull;

/**
 * Interface for all Data Access Objects for Cassandra using the Hector client API.
 * Provides support for batch execution using a TransactionContext.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public interface KeyspaceOperations {

    /**
     * Create a TransactionContext for use with this keyspace.
     *
     * @return the TransactionContext
     */
    TransactionContext begin();

    /**
     * Execute a batch of mutations using a mutator.
     *
     * @param transactionContext the TransactionContext
     */
    void commit(@Nonnull TransactionContext transactionContext);
}
