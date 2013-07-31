package com.ebuddy.cassandra.dao;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.TreeSet;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ebuddy.cassandra.StructuredDataSupport;
import com.ebuddy.cassandra.TypeReference;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * System tests for ThriftStructuredDataSupport.
 * Requires a Cassandra 1.2.x+ instance
 *
 * @author EOric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class ThriftStructuredDataSupportSystemTest {

    private static final String TEST_CLUSTER = "Test Cluster";
    private static final String LOCALHOST_IP = "localhost";
    private static final String CASSANDRA_HOSTS_SYSTEM_PROPERTY = "cassandra.hosts";
    private static final String TEST_KEYSPACE = "ThriftStructuredDataSupportSystemTest";
    private static final String CLUSTER_NAME_PROPERTY = "cassandra.cluster";
    private final String columnFamily = "testpojo";

    private Cluster cluster;
    private StructuredDataSupport<String> dao;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        // default to using cassandra on localhost, but can be overridden with a system property
        String cassandraHosts = System.getProperty(CASSANDRA_HOSTS_SYSTEM_PROPERTY, LOCALHOST_IP);

        // default cluster name can also be overridden with a system property
        String clusterName = System.getProperty(CLUSTER_NAME_PROPERTY, TEST_CLUSTER);

        cluster = HFactory.getOrCreateCluster(clusterName, cassandraHosts);
        Keyspace keyspace = HFactory.createKeyspace(TEST_KEYSPACE, cluster);
        Serializer<String> keySerializer = StringSerializer.get();
        Serializer<String> columnNameSerializer = StringSerializer.get();
        Serializer<Object> valueSerializer = StructureSerializer.get();
        ColumnFamilyOperations<String,String,Object> operations = new ColumnFamilyTemplate<String,String,Object>(
                keyspace,
                columnFamily,
                keySerializer,
                columnNameSerializer,
                valueSerializer);

        dao = new ThriftStructuredDataSupport<String>(operations);

        dropAndCreateSchema();
    }

    @Test(groups = {"system"})
    public void shouldWriteReadDeleteTestPojo() throws Exception {
        TestPojo testObject = new TestPojo("v1", 42L, true, Arrays.asList("e1", "e2"));
        String rowKey = "pojo0";
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

    @Test(groups = {"system"})
    public void shouldWriteReadDeleteTestPojoWithSet() throws Exception {
        TestPojoWithSet testObject = getTestPojoWithSubclassedSets();
        String rowKey = "pojo1";
        String pathString = "a/b/c";
        TypeReference<TestPojoWithSet> typeReference = new TypeReference<TestPojoWithSet>() { };

        dao.writeToPath(rowKey, pathString, testObject);
        TestPojoWithSet result = dao.readFromPath(rowKey, pathString, typeReference);
        assertNotSame(result, testObject);
        assertEquals(result, testObject);

        dao.deletePath(rowKey, pathString);
        TestPojoWithSet result2 = dao.readFromPath(rowKey, pathString, typeReference);
        assertNull(result2);
    }



    private void dropAndCreateSchema() {
        dropKeyspaceIfExists();
        cluster.addKeyspace(HFactory.createKeyspaceDefinition(TEST_KEYSPACE), true);
        ColumnFamilyDefinition columnFamilyDefinition = HFactory.createColumnFamilyDefinition(TEST_KEYSPACE,
                                                                                              columnFamily,
                                                                                              ComparatorType.UTF8TYPE);
        columnFamilyDefinition.setDefaultValidationClass("UTF8Type");
        cluster.addColumnFamily(columnFamilyDefinition);
    }



    private void dropKeyspaceIfExists() {
        try {
            cluster.dropKeyspace(TEST_KEYSPACE, true);
        } catch (HInvalidRequestException ignored) {
            // doesn't exist
        }
    }

    @SuppressWarnings("CloneableClassWithoutClone")
    private TestPojoWithSet getTestPojoWithSubclassedSets() {
        return new TestPojoWithSet("string",
                                   42L,
                                   true,
                                   Arrays.asList("l2", "l1", "l3"),
                                   new HashSet<Object>() {{
                                       add(1);
                                       add("X");
                                   }},
                                   new HashSet<String>() {{
                                       add("a");
                                       add("b");
                                   }},
                                   new TreeSet<String>() {{
                                       add("x");
                                       add("y");
                                   }});
    }
}