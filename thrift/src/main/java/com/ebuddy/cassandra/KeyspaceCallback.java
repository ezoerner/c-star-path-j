package com.ebuddy.cassandra;

import me.prettyprint.cassandra.service.KeyspaceService;

/**
 * A callback used with a CassandraTemplate when a single Keyspace is needed.
 * T is the type of result from the {@link #execute} method.
 *
 * @author Eric Zoerner
 * @see CassandraTemplate
 */
public interface KeyspaceCallback<T> {

    /**
     * Execute an operation using a Keyspace.
     *
     * @param keyspace the Keyspace
     * @return the result
     */
    T execute(KeyspaceService keyspace);
}
