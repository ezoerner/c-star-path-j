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

import static org.testng.Assert.assertNotNull;

import java.util.UUID;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * // TODO: Add class description here.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class ThriftConsistencyLevelBugSystemTest {
    private static final String CASSANDRA_HOSTS = "cass-uds001.dev.ebuddy-office.net,cass-uds002.dev.ebuddy-office.net,cass-uds003.dev.ebuddy-office.net,cass-uds004.dev.ebuddy-office.net";
    private static final int REPLICATION_FACTOR = 3;
    private static final String TEST_KEYSPACE = "consistencylevelbugsystemtest";
    private static final UUID KEY = UUID.randomUUID();
    private static final long SLEEP_MS = 0L;
    private static final int REPETITIONS = 100;
    private static final String TEST_CLUSTER = "Test Cluster";

    private Cluster cluster;
    private Keyspace keyspace;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {

        cluster = HFactory.getOrCreateCluster(TEST_CLUSTER, CASSANDRA_HOSTS);
        keyspace = HFactory.createKeyspace(TEST_KEYSPACE, cluster);

        dropAndCreateSchema();
    }


    @Test(groups="system", enabled = false)
    public void testDeleteConsistency() throws Exception {
        for (int i = 0; i < REPETITIONS; i++) {
            System.out.println("i=" + i);

            HColumn<String,Integer> column = HFactory.createColumn("column",
                                                                   i,
                                                                   StringSerializer.get(),
                                                                   IntegerSerializer.get());
            Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());
            mutator.insert(KEY.toString(), "test", column);
            sleep();

            mutator.delete(KEY.toString(), "test", column.getName(), StringSerializer.get());
            sleep();

            ColumnQuery<String,String,Integer> query = HFactory.createColumnQuery(keyspace,
                                                                                  StringSerializer.get(),
                                                                                  StringSerializer.get(),
                                                                                  IntegerSerializer.get())
                    .setColumnFamily("test").setKey(KEY.toString()).setName("column");
            QueryResult<HColumn<String,Integer>> result = query.execute();
            HColumn<String,Integer> resultColumn = result.get();

            if (resultColumn != null) {
                // try waiting and then trying again
                Thread.sleep(2000L);

                result = query.execute();
                resultColumn = result.get();
                if (resultColumn == null) {
                    throw new AssertionError("Got an inconsistent read, but with eventual consistency");
                }
                throw new AssertionError("Got an inconsistent read, WITHOUT eventual consistency");
            }
        }
    }


    @Test(groups="system", enabled = false)
    public void testWriteConsistency() throws Exception {

        for (int i = 0; i < REPETITIONS; i++) {
            System.out.println("i=" + i);


            HColumn<String,Integer> column = HFactory.createColumn("column", i, StringSerializer.get(), IntegerSerializer.get());
            HFactory.createMutator(keyspace, StringSerializer.get()).insert(KEY.toString(), "test", column);
            sleep();


            ColumnQuery<String,String,Integer> query = HFactory.createColumnQuery(keyspace,
                                                                                  StringSerializer.get(),
                                                                                  StringSerializer.get(),
                                                                                  IntegerSerializer.get())
                    .setColumnFamily("test").setKey(KEY.toString()).setName("column");
            QueryResult<HColumn<String,Integer>> result = query.execute();


            HColumn<String,Integer> resultColumn = result.get();
            assertNotNull(resultColumn);

            if (resultColumn.getValue() !=  i) {
                // try waiting and then trying again
                Thread.sleep(2000L);

                result = query.execute();
                resultColumn = result.get();
                assertNotNull(resultColumn);
                if (resultColumn.getValue() ==  i) {
                    throw new AssertionError("Got an inconsistent read, but with eventual consistency");
                }
                throw new AssertionError("Got an inconsistent read, WITHOUT eventual consistency, value=" + resultColumn.getValue());
            }
        }
    }

    @SuppressWarnings("ConstantConditions")
    private void sleep() throws InterruptedException {
        if (SLEEP_MS > 0L) {
            Thread.sleep(SLEEP_MS);
        }
    }


    private void dropAndCreateSchema() throws InterruptedException {
        dropKeyspaceIfExists();
        cluster.addKeyspace(HFactory.createKeyspaceDefinition(TEST_KEYSPACE), true);

        ColumnFamilyDefinition columnFamilyDefinition = HFactory.createColumnFamilyDefinition(TEST_KEYSPACE,
                                                                                              "test",
                                                                                              ComparatorType.UTF8TYPE);
        columnFamilyDefinition.setDefaultValidationClass("IntegerType");
        columnFamilyDefinition.setKeyValidationClass("UTF8Type");
        cluster.addColumnFamily(columnFamilyDefinition);

        // give some time to let the schema changes propagate
        Thread.sleep(200);
    }

    private void dropKeyspaceIfExists() {
        try {
            cluster.dropKeyspace(TEST_KEYSPACE, true);
        } catch (HInvalidRequestException ignored) {
            // doesn't exist
        }
    }
}
