package com.ebuddy.cassandra.dao;

import javax.annotation.Nonnull;

import com.ebuddy.cassandra.BatchContext;

/**
 * Interface for all Data Access Objects for Cassandra using the Hector client API.
 * Provides support for batch execution using a BatchContext.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public interface KeyspaceOperations {

    /**
     * Create a BatchContext for use with this keyspace.
     *
     * @return the BatchContext
     */
    BatchContext begin();

    /**
     * Execute a batch of mutations using a mutator.
     *
     * @param batchContext the BatchContext
     */
    void commit(@Nonnull BatchContext batchContext);
}
