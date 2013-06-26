package com.ebuddy.cassandra.cql;

import org.springframework.dao.DataAccessException;

import com.datastax.driver.core.Cluster;

/**
 * Generic callback interface for code that operates on a Cassandra Cluster.
 * Allows to execute any number of operations on a single Cluster.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public interface ClusterCallback<T> {
    T doInCluster(Cluster cluster) throws DataAccessException;
}
