
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

/**
 * Test to reproduce the problem with delete consistency in Cassandra using CQL.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class ConsistencyLevelBugSystemTest {
    private static final String CASSANDRA_HOSTS = "cass-uds001.dev.ebuddy-office.net,cass-uds002.dev.ebuddy-office.net,cass-uds003.dev.ebuddy-office.net,cass-uds004.dev.ebuddy-office.net";
    //private static final String CASSANDRA_HOSTS = "cass-uds001.dev.ebuddy-office.net";
    //private static final String CASSANDRA_HOSTS = "cass-uds002.dev.ebuddy-office.net";
    //private static final String CASSANDRA_HOSTS = "cass-uds003.dev.ebuddy-office.net";
    //private static final String CASSANDRA_HOSTS = "cass-uds004.dev.ebuddy-office.net";
    private static final int REPLICATION_FACTOR = 3;
    private static final String TEST_KEYSPACE = "consistencylevelbugsystemtest";
    private static final UUID KEY = UUID.randomUUID();
    private static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ALL;
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

    @Test(groups="system")
    public void testDeleteConsistency() throws Exception {
        for (int i = 0; i < REPETITIONS; i++) {
            System.out.println("i=" + i);
            PreparedStatement statement = session.prepare("INSERT INTO test (key, column, value) VALUES (?,?,?);");
            statement.setConsistencyLevel(CONSISTENCY_LEVEL);
            session.execute(statement.bind(KEY, "column", i));
            sleep();

            statement = session.prepare("DELETE FROM test where key=? and column = ?");
            statement.setConsistencyLevel(CONSISTENCY_LEVEL);
            session.execute(statement.bind(KEY, "column"));
            sleep();

            statement = session.prepare("SELECT * FROM test where key=? and column =?");
            statement.setConsistencyLevel(CONSISTENCY_LEVEL);
            ResultSet results = session.execute(statement.bind(KEY, "column"));

            if (!results.isExhausted()) {
                // try waiting and then trying again
                Thread.sleep(2000L);

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

    @Test(groups="system")
    public void testWriteConsistency() throws Exception {

        for (int i = 0; i < REPETITIONS; i++) {
            System.out.println("i=" + i);
            PreparedStatement statement = session.prepare("INSERT INTO test (key, column, value) VALUES (?,?,?);");
            statement.setConsistencyLevel(CONSISTENCY_LEVEL);
            session.execute(statement.bind(KEY,  "column", i));
            sleep();


            statement = session.prepare("SELECT * FROM test where key=? and column =?");
            statement.setConsistencyLevel(CONSISTENCY_LEVEL);
            ResultSet results = session.execute(statement.bind(KEY,"column"));
            List<Row> rows = results.all();
            assertEquals(rows.size(), 1);
            if (rows.get(0).getInt("value") !=  i) {
                // try waiting and then trying again
                Thread.sleep(2000L);

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
