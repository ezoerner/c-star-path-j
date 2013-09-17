/*
 * Copyright 2013 eBuddy B.V.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.ebuddy.cassandra;

import java.util.concurrent.TimeUnit;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.cassandra.service.KeyspaceService;
import me.prettyprint.cassandra.service.KeyspaceServiceImpl;
import me.prettyprint.cassandra.service.clock.MicrosecondsClockResolution;
import me.prettyprint.cassandra.service.clock.MicrosecondsSyncClockResolution;
import me.prettyprint.cassandra.service.clock.MillisecondsClockResolution;
import me.prettyprint.cassandra.service.clock.SecondsClockResolution;
import me.prettyprint.hector.api.ClockResolution;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.exceptions.HectorException;

/**
 * A template used for accessing Cassandra using a Hector client.
 * This template handles borrowing and releasing
 * a client from the connection pool, and has a central location for handling HectorExceptions.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 * @deprecated User the DAO classes instead
 * @see com.ebuddy.cassandra.dao.KeyspaceOperations
 */
@Deprecated
public class CassandraTemplate {

    private final Cluster cluster;

    private final CassandraHostConfigurator hostConfigurator;

    private final ConfigurableConsistencyLevel defaultReadConsistency = new ConfigurableConsistencyLevel();
    private final ConfigurableConsistencyLevel defaultWriteConsistency = new ConfigurableConsistencyLevel();
    private final ConfigurableConsistencyLevel defaultAllConsistency = new ConfigurableConsistencyLevel();

    private FailoverPolicy failoverPolicy = FailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE;

    /**
     * Construct an instance.
     * @param cluster the Cassandra Cluster
     * @param hostConfigurator the host configurator
     */
    public CassandraTemplate(Cluster cluster, CassandraHostConfigurator hostConfigurator) {
        this.cluster = cluster;
        this.hostConfigurator = hostConfigurator;
    }


    public FailoverPolicy getFailoverPolicy() {
        return failoverPolicy;
    }

    public void setFailoverPolicy(FailoverPolicy failoverPolicy) {
        this.failoverPolicy = failoverPolicy;
    }

    /**
     * Shortcut to create a timestamp without borrowing a client,
     * using the resolution configured in the
     * CassandraHostConfigurator. This assumes the resolution is uniform across
     * all the hosts, which is should always be true.
     *
     * @return a newly created timestamp for "now"
     */
    public long createTimestamp() {
        return cluster.getConnectionManager().createClock();
    }

    /**
     * Set the default read consistency level.
     *
     * @param consistency the consistency level
     */
    public void setDefaultReadConsistency(HConsistencyLevel consistency) {
        defaultReadConsistency.setDefaultReadConsistencyLevel(consistency);
    }

    /**
     * Set the default write consistency level.
     *
     * @param consistency the write consistency level
     */
    public void setDefaultWriteConsistency(HConsistencyLevel consistency) {
        defaultWriteConsistency.setDefaultWriteConsistencyLevel(consistency);
    }

    /**
     * Execute a callback for a specific Keyspace for a read operation.
     * 
     * @param keyspaceName the name of the Keyspace
     * @param keyspaceCallback the callback to execute
     * @param <T> the return type of the execution
     * @return the result of the execution
     */
    public <T> T read(String keyspaceName, KeyspaceCallback<T> keyspaceCallback) {
        return execute(keyspaceName, keyspaceCallback, defaultReadConsistency, true);
    }

    /**
     * Execute a callback for a specific Keyspace for a write operation.
     *
     * @param keyspaceName the name of the Keyspace
     * @param keyspaceCallback the callback to execute
     * @param <T> the return type of the execution
     * @return the result of the execution
     */
    public <T> T write(String keyspaceName, KeyspaceCallback<T> keyspaceCallback) {
        return execute(keyspaceName, keyspaceCallback, defaultWriteConsistency, false);
    }


    /**
     * Execute a callback for a specific Keyspace for a read or write operation,
     * with a specified consistency level.
     *
     * @param keyspaceName the name of the Keyspace
     * @param keyspaceCallback the callback with an execute method
     * @param consistency the consistency level to use
     * @param readOnly whether this should be enforced as a read-only execution
     * @param <T> the return type of the execute method
     * @return object of type T
     */
    public <T> T execute(String keyspaceName,
                         KeyspaceCallback<T> keyspaceCallback,
                         ConsistencyLevelPolicy consistency,
                         boolean readOnly) {
        if (consistency == null) {
            consistency = defaultAllConsistency;
        }
        KeyspaceService ks = new KeyspaceServiceImpl(keyspaceName,
                                                     consistency,
                                                     cluster.getConnectionManager(),
                                                     failoverPolicy);

        if (readOnly) {
            ks = new ReadOnlyKeyspace(ks);
        }
        T result = null;
        try {
            result = keyspaceCallback.execute(ks);
        } catch (HectorException he) {
            handleHectorException(he);
        }
        return result;
    }


    private void handleHectorException(HectorException he) {
        // can translate the hector exception here to a library specific exception.
        // we used to translate to spring exceptions, but spring dependency has been removed
        throw he;
    }

    public TimeUnit getClockResolution() {
        // The ClockResolution class in Hector is quite lame.
        // No equals() or any way to actually query it to find out
        // its resolution other than its class
        ClockResolution clockResolution = hostConfigurator.getClockResolution();
        if ((clockResolution instanceof MicrosecondsClockResolution)
            || (clockResolution instanceof MicrosecondsSyncClockResolution)) {
            return TimeUnit.MICROSECONDS;
        }
        if (clockResolution instanceof MillisecondsClockResolution) {
            return TimeUnit.MILLISECONDS;
        }
        if (clockResolution instanceof SecondsClockResolution) {
            return TimeUnit.SECONDS;
        }
        throw new AssertionError("Unexpected implementation class for clockResolution: "
                                         + clockResolution.getClass().getName());
    }
}
