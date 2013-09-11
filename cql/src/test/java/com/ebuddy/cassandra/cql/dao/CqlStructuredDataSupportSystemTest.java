package com.ebuddy.cassandra.cql.dao;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.ebuddy.cassandra.BatchContext;
import com.ebuddy.cassandra.Path;
import com.ebuddy.cassandra.StructuredDataSupport;
import com.ebuddy.cassandra.TypeReference;
import com.ebuddy.cassandra.databind.CustomTypeResolverBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;

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
    private final String tableName = "testpojo";

    private StructuredDataSupport<UUID> daoSupport;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();

        // default to using cassandra on localhost, but can be overridden with a system property
        String cassandraHostsString = System.getProperty(CASSANDRA_HOSTS_SYSTEM_PROPERTY, LOCALHOST_IP);
        String[] cassandraHosts = StringUtils.split(cassandraHostsString, ',');
        Cluster.Builder clusterBuilder = Cluster.builder();
        for (String host : cassandraHosts) {
            clusterBuilder.addContactPoint(host);
        }
        cluster = clusterBuilder.withPort(9142).build();
        dropAndCreateSchema();

        // get new session using a default keyspace that we now know exists
        Session session = cluster.connect(TEST_KEYSPACE);
        daoSupport = new CqlStructuredDataSupport<UUID>(tableName, session);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        cluster.shutdown();
    }


    @Test(groups = {"system"})
    public void shouldWriteReadDeleteTestPojo() throws Exception {
        TestPojo testObject = new TestPojo("v1", 42L, true, Arrays.asList("e1", "e2"));
        UUID rowKey = UUID.randomUUID();
        Path path = daoSupport.createPath("a","b","c");
        TypeReference<TestPojo> typeReference = new TypeReference<TestPojo>() { };

        daoSupport.writeToPath(rowKey, path, testObject);
        TestPojo result = daoSupport.readFromPath(rowKey, path, typeReference);
        assertNotSame(result, testObject);
        assertEquals(result, testObject);

        daoSupport.deletePath(rowKey, path);
        TestPojo result2 = daoSupport.readFromPath(rowKey, path, typeReference);
        assertNull(result2);
    }

    @Test(groups = {"system"})
    public void shouldWriteInBatch() throws Exception {
        TestPojo testObject1 = new TestPojo("v1", 42L, true, Arrays.asList("e1", "e2"));
        TestPojo testObject2 = new TestPojo("v2", 43L, false, Arrays.asList("e3", "e4"));

        UUID rowKey1 = UUID.randomUUID();
        UUID rowKey2 = UUID.randomUUID();
        Path path = daoSupport.createPath("test");
        TypeReference<TestPojo> typeReference = new TypeReference<TestPojo>() { };

        BatchContext batchContext = daoSupport.beginBatch();
        daoSupport.writeToPath(rowKey1, path, testObject1, batchContext);
        daoSupport.writeToPath(rowKey2, path, testObject2, batchContext);
        daoSupport.applyBatch(batchContext);

        TestPojo result1 = daoSupport.readFromPath(rowKey1, path, typeReference);
        assertNotSame(result1, testObject1);
        assertEquals(result1, testObject1);

        TestPojo result2 = daoSupport.readFromPath(rowKey2, path, typeReference);
        assertNotSame(result2, testObject2);
        assertEquals(result2, testObject2);
    }

    @Test(groups = {"system"})
    public void shouldShrinkList() throws Exception {
        List<String> longList = Arrays.asList("1", "2", "3", "4", "5", "6");
        UUID rowKey = UUID.randomUUID();
        TypeReference<List<String>> typeReference = new TypeReference<List<String>>() { };

        Path path = daoSupport.createPath("x");

        daoSupport.writeToPath(rowKey, path, longList);
        List<String> resultLongList = daoSupport.readFromPath(rowKey, path, typeReference);
        assertNotSame(resultLongList, longList);
        assertEquals(resultLongList, longList);

        List<String> shortList = Arrays.asList("1", "2", "3");

        daoSupport.writeToPath(rowKey, path, shortList);
        List<String> resultShortList = daoSupport.readFromPath(rowKey, path, typeReference);
        assertNotSame(resultShortList, shortList);
        assertEquals(resultShortList, shortList);
    }

    @Test(groups = {"system"})
    public void shouldRemoveListCruftWhenDeleting() throws Exception {
        List<String> longList = Arrays.asList("1", "2", "3", "4", "5", "6");
        UUID rowKey = UUID.randomUUID();
        TypeReference<List<String>> typeReference = new TypeReference<List<String>>() { };

        Path path = daoSupport.createPath("x");

        daoSupport.writeToPath(rowKey, path, longList);
        List<String> resultLongList = daoSupport.readFromPath(rowKey, path, typeReference);
        assertNotSame(resultLongList, longList);
        assertEquals(resultLongList, longList);

        List<String> shortList = Arrays.asList("1", "2", "3");

        daoSupport.writeToPath(rowKey, path, shortList);
        List<String> resultShortList = daoSupport.readFromPath(rowKey, path, typeReference);
        assertNotSame(resultShortList, shortList);
        assertEquals(resultShortList, shortList);

        Path indexPath = daoSupport.createPath("x").withIndices(4);
        String s = daoSupport.readFromPath(rowKey,indexPath, new TypeReference<String>() {});
        assertEquals(s, "5"); // cruft

        daoSupport.deletePath(rowKey, path);

        s = daoSupport.readFromPath(rowKey,indexPath, new TypeReference<String>() {});
        assertNull(s); // cruft gone
    }


    @SuppressWarnings("unchecked")
    @Test(groups = {"system"})
    public void convertValueShouldRetainOrderingInMaps() throws Exception {
        SortedMap<String,String> map = new TreeMap<String,String>();
        map.put("b", "1");
        map.put("a", "2");

        ObjectMapper mapper = new ObjectMapper();
        mapper.setDefaultTyping(new CustomTypeResolverBuilder());
        Object converted = mapper.convertValue(map, Object.class);
        assertTrue(converted instanceof LinkedHashMap);
        // keys are sorted by Cassandra as UTF8
        assertEquals(((Map<String,String>)converted).keySet().iterator().next(), "a");
    }

    private void dropAndCreateSchema() {
        Session localSession = cluster.connect();
        try {
            dropKeyspaceIfExists(localSession);
            localSession.execute("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication " + "= {'class':'SimpleStrategy', " +
                                    "'replication_factor':1};");

            localSession.execute("CREATE TABLE " + TEST_KEYSPACE + "." + tableName + " (key uuid, column1 text, " +
                                         "value text, PRIMARY KEY (key, column1));");
        } finally {
            localSession.shutdown();
        }
    }

    private void dropKeyspaceIfExists(Session localSession) {
        try {
            localSession.execute("drop keyspace " + TEST_KEYSPACE);
        } catch (InvalidQueryException ignored) {
            // doesn't exist
        }
    }
}
