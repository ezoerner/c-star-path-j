
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

package com.ebuddy.cassandra.cql;

import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.ebuddy.cassandra.cql.dao.CqlStructuredDataSupport;

/**
 * Test to reproduce the problem with delete consistency in Cassandra using CQL.
 * Seems the tests fail only if "USING TIMESTAMP" is not used.
 * See <a href="https://datastax-oss.atlassian.net/browse/JAVA-164">JAVA-164</a>
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class ConsistencyLevelBugSystemTest {
    private static final String CASSANDRA_HOSTS = "cass-uds001.dev.ebuddy-office.net,cass-uds002.dev.ebuddy-office.net,cass-uds003.dev.ebuddy-office.net,cass-uds004.dev.ebuddy-office.net";
    private static final int REPLICATION_FACTOR = 3;
    private static final String TEST_KEYSPACE = "consistencylevelbugsystemtest";
    private static final UUID KEY = UUID.randomUUID();
    private static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
    private static final long SLEEP_MS = 0L;
    private static final int REPETITIONS = 30;

    private Cluster cluster;
    private Session session;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {

        String[] cassandraHosts = StringUtils.split(CASSANDRA_HOSTS, ',');
        cluster = Cluster.builder().addContactPoints(cassandraHosts).build();
        dropAndCreateSchema();

        session = cluster.connect(TEST_KEYSPACE);
    }

    @Test(groups="system", enabled = false)
    public void testDeleteConsistency() throws Exception {
        for (int i = 0; i < REPETITIONS; i++) {
            System.out.println("i=" + i);
            PreparedStatement statement = session.prepare("INSERT INTO test (key, column, value) VALUES (?,?,?) " +
                "USING TIMESTAMP " + CqlStructuredDataSupport.getCurrentMicros());
            statement.setConsistencyLevel(CONSISTENCY_LEVEL);
            session.execute(statement.bind(KEY, "column", i));
            sleep();

            statement = session.prepare("DELETE FROM test USING TIMESTAMP " +
                                                CqlStructuredDataSupport.getCurrentMicros() +
                                                " where key=? and column = ?");
            statement.setConsistencyLevel(CONSISTENCY_LEVEL);
            session.execute(statement.bind(KEY, "column"));
            sleep();

            statement = session.prepare("SELECT * FROM test where key=? and column =?");
            statement.setConsistencyLevel(CONSISTENCY_LEVEL);
            ResultSet results = session.execute(statement.bind(KEY, "column"));

            if (!results.isExhausted()) {
                // try waiting and then trying again
                Thread.sleep(10000L);

                statement = session.prepare("SELECT * FROM test where key=? and column=?");
                statement.setConsistencyLevel(CONSISTENCY_LEVEL);
                results = session.execute(statement.bind(KEY, "column"));
                if (results.isExhausted()) {
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
            PreparedStatement statement = session.prepare("INSERT INTO test (key, column, value) VALUES (?,?,?) " +
                "USING TIMESTAMP " + CqlStructuredDataSupport.getCurrentMicros());
            statement.setConsistencyLevel(CONSISTENCY_LEVEL);
            session.execute(statement.bind(KEY, "column", i));
            sleep();


            statement = session.prepare("SELECT * FROM test where key=? and column =?");
            statement.setConsistencyLevel(CONSISTENCY_LEVEL);
            ResultSet results = session.execute(statement.bind(KEY,"column"));
            List<Row> rows = results.all();
            assertEquals(rows.size(), 1);
            if (rows.get(0).getInt("value") !=  i) {
                // try waiting and then trying again
                Thread.sleep(10000L);

                statement = session.prepare("SELECT * FROM test where key=? and column=?");
                statement.setConsistencyLevel(CONSISTENCY_LEVEL);
                results = session.execute(statement.bind(KEY,"column"));
                rows = results.all();
                assertEquals(rows.size(), 1);
                if (rows.get(0).getInt("value") ==  i) {
                    throw new AssertionError("Got an inconsistent read, but with eventual consistency");
                }
                throw new AssertionError("Got an inconsistent read, WITHOUT eventual consistency, value=" + rows.get(0).getInt("value"));
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
        Session localSession = cluster.connect();
        try {
            dropKeyspaceIfExists(localSession);
            localSession.execute("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication " + "= {'class':'SimpleStrategy', " +
                                         "'replication_factor':" + REPLICATION_FACTOR + "};");

            localSession.execute("CREATE TABLE " + TEST_KEYSPACE +
                                         ".test (key uuid, column text, value int, PRIMARY KEY (key,column));");
        } finally {
            localSession.shutdown();
        }

        // give some time to let the schema changes propagate
        Thread.sleep(200);
    }

    private void dropKeyspaceIfExists(Session localSession) {
        try {
            localSession.execute("drop keyspace " + TEST_KEYSPACE);
        } catch (InvalidQueryException ignored) {
            // doesn't exist
        }
    }
}
