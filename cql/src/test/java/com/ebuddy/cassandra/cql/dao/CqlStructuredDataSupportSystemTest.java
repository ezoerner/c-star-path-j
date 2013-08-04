package com.ebuddy.cassandra.cql.dao;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;

import java.util.Arrays;
import java.util.UUID;

import org.jboss.netty.util.internal.StringUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.ebuddy.cassandra.StructuredDataSupport;
import com.ebuddy.cassandra.TypeReference;

/**
 * System tests for CqlStructuredDataSupport.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class CqlStructuredDataSupportSystemTest {
    private static final String LOCALHOST_IP = "localhost";
    private static final String CASSANDRA_HOSTS_SYSTEM_PROPERTY = "cassandra.hosts";
    private static final String TEST_KEYSPACE = "cqlstructureddatasupportsystemtest";

    private Cluster cluster;
    private Session session;
    private final String tableName = "testpojo";

    private StructuredDataSupport<UUID> dao;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        // default to using cassandra on localhost, but can be overridden with a system property
        String cassandraHostsString = System.getProperty(CASSANDRA_HOSTS_SYSTEM_PROPERTY, LOCALHOST_IP);
        String[] cassandraHosts = StringUtil.split(cassandraHostsString, ',');
        Cluster.Builder clusterBuilder = Cluster.builder();
        for (String host : cassandraHosts) {
            clusterBuilder.addContactPoint(host);
        }
        cluster = clusterBuilder.build();
        session = cluster.connect(TEST_KEYSPACE);

        dao = new CqlStructuredDataSupport<UUID>(tableName, session);
        dropAndCreateSchema();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        cluster.shutdown();
    }


    @Test(groups = {"system"})
    public void shouldWriteReadDeleteTestPojo() throws Exception {
        TestPojo testObject = new TestPojo("v1", 42L, true, Arrays.asList("e1", "e2"));
        UUID rowKey = UUID.randomUUID();
        String pathString = "a/b/c";
        TypeReference<TestPojo> typeReference = new TypeReference<TestPojo>() { };

        dao.writeToPath(rowKey, pathString, testObject);
        TestPojo result = dao.readFromPath(rowKey, pathString, typeReference);
        assertNotSame(result, testObject);
        assertEquals(result, testObject);

        dao.deletePath(rowKey, pathString);
        TestPojo result2 = dao.readFromPath(rowKey, pathString, typeReference);
        assertNull(result2);
    }

    private void dropAndCreateSchema() {
        dropKeyspaceIfExists();
        session.execute("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication " + "= {'class':'SimpleStrategy', " +
                                "'replication_factor':1};");

        session.execute("CREATE TABLE " + tableName + " (key uuid, column1 text, value text, PRIMARY KEY (key, column1));");
    }

    private void dropKeyspaceIfExists() {
        try {
            session.execute("drop keyspace " + TEST_KEYSPACE);
        } catch (InvalidQueryException ignored) {
            // doesn't exist
        }
    }
}
