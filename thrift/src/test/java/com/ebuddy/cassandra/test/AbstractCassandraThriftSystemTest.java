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

package com.ebuddy.cassandra.test;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.testng.annotations.BeforeMethod;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * Common code for system tests that use Cassandra with Thrift.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public abstract class AbstractCassandraThriftSystemTest {
    private static final Logger log = Logger.getLogger(AbstractCassandraThriftSystemTest.class);
    private static final String TEST_CLUSTER = "Test Cluster";
    private static final String EMBEDDED_CASSANDRA_HOST = "localhost:9171";
    private static final String CASSANDRA_HOSTS_SYSTEM_PROPERTY = "cassandra.hosts";
    private static final String TEST_KEYSPACE = "system_test";
    private static final String CLUSTER_NAME_PROPERTY = "cassandra.cluster";
    private static final String SCHEMA_CHANGE_SLEEP_PROPERTY = "schema.change.sleep.ms";
    private static final long DEFAULT_SCHEMA_CHANGE_SLEEP = 5000L;
    private static final long SCHEMA_CHANGE_SLEEP = Long.getLong(SCHEMA_CHANGE_SLEEP_PROPERTY,
                                                                 DEFAULT_SCHEMA_CHANGE_SLEEP);

    protected Cluster cluster;
    protected CassandraHostConfigurator hostConfigurator;
    protected Keyspace keyspace;

    private int numHosts;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        // default to using cassandra on localhost, but can be overridden with a system property
        String cassandraHosts = System.getProperty(CASSANDRA_HOSTS_SYSTEM_PROPERTY, EMBEDDED_CASSANDRA_HOST);
        String[] hosts = StringUtils.split(cassandraHosts, ",");
        numHosts = hosts.length;

        if (EMBEDDED_CASSANDRA_HOST.equals(cassandraHosts)) {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        }

        // default cluster name can also be overridden with a system property
        String clusterName = System.getProperty(CLUSTER_NAME_PROPERTY, TEST_CLUSTER);

        hostConfigurator = new CassandraHostConfigurator(cassandraHosts);
        cluster = HFactory.getOrCreateCluster(clusterName, hostConfigurator);
        keyspace = HFactory.createKeyspace(TEST_KEYSPACE, cluster);

        dropAndCreateSchema();
        sleepAfterSchemaChangeIfNecessary();
    }

    @SuppressWarnings("LogStatementGuardedByLogCondition")
    private void sleepAfterSchemaChangeIfNecessary() throws InterruptedException {
        // only need to sleep after a schema change if there is more than one host in the cluster
        if (numHosts > 1) {
            log.info("Sleeping after schema change for " + SCHEMA_CHANGE_SLEEP + " ms");
            Thread.sleep(SCHEMA_CHANGE_SLEEP);
        }
    }

    // subclasses should override to create column families after calling super.dropAndCreateSchema()
    protected void dropAndCreateSchema() throws InterruptedException {
        dropKeyspaceIfExists();
        sleepAfterSchemaChangeIfNecessary();
        cluster.addKeyspace(HFactory.createKeyspaceDefinition(TEST_KEYSPACE), true);
    }



    protected void dropKeyspaceIfExists() {
        try {
            cluster.dropKeyspace(TEST_KEYSPACE, true);
        } catch (HInvalidRequestException ignored) {
            // doesn't exist
        }
    }

    protected void createColumnFamily(String columnFamilyName, ColumnType columnType) {
        ColumnFamilyDefinition columnFamilyDefinition = HFactory.createColumnFamilyDefinition(keyspace.getKeyspaceName(),

                                                                                              columnFamilyName,
                                                                                              ComparatorType.UTF8TYPE);
        columnFamilyDefinition.setDefaultValidationClass("UTF8Type");
        columnFamilyDefinition.setKeyValidationClass("UTF8Type");
        columnFamilyDefinition.setColumnType(columnType);
        if (columnType == ColumnType.SUPER) {
            columnFamilyDefinition.setSubComparatorType(ComparatorType.UTF8TYPE);
        }
        cluster.addColumnFamily(columnFamilyDefinition);
    }
}
