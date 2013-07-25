package com.ebuddy.cassandra.dao;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ebuddy.cassandra.StructuredDataSupport;
import com.ebuddy.cassandra.TypeReference;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * System tests for ThriftStructuredDataSupport.
 * Requires local Cassandra 1.2.x+ to be running with a default cluster ("Test Cluster")
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class ThriftStructuredDataSupportSystemTest {

    private static final String TEST_CLUSTER = "Test Cluster";
    private static final String LOCALHOST_IP = "localhost";
    private static final String TEST_KEYSPACE = "ThriftStructuredDataSupportSystemTest";
    private final String columnFamily = "testpojo";

    private Cluster cluster;
    private ColumnFamilyOperations<String,String,Object> operations;
    private StructuredDataSupport<String> dao;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        cluster = HFactory.getOrCreateCluster(TEST_CLUSTER, LOCALHOST_IP);
        Keyspace keyspace = HFactory.createKeyspace(TEST_KEYSPACE, cluster);
        Serializer<String> keySerializer = StringSerializer.get();
        Serializer<String> columnNameSerializer = StringSerializer.get();
        Serializer<Object> valueSerializer = StructureSerializer.get();
        operations = new ColumnFamilyTemplate<String,String,Object>(keyspace,
                                                                    keySerializer,
                                                                    columnNameSerializer,
                                                                    valueSerializer);

        dao = new ThriftStructuredDataSupport<String>(operations);

        dropAndCreateSchema();
    }

    @Test(groups = {"system"})
    public void shouldReadAndWriteTestPojo() throws Exception {
        TestPojo testObject = new TestPojo("v1", 42L, true, Arrays.asList("e1", "e2"));
        String rowKey = "pojo0";
        String pathString = "a/b/c";
        TypeReference<TestPojo> typeReference = new TypeReference<TestPojo>() { };

        dao.writeToPath(columnFamily, rowKey, pathString, testObject);
        TestPojo result = dao.readFromPath(columnFamily, rowKey, pathString, typeReference);

        assertEquals(result, testObject);
    }

    private void dropAndCreateSchema() {
        dropKeyspaceIfExists();
        cluster.addKeyspace(HFactory.createKeyspaceDefinition(TEST_KEYSPACE), true);
        cluster.addColumnFamily(HFactory.createColumnFamilyDefinition(TEST_KEYSPACE,
                                                                      columnFamily,
                                                                      ComparatorType.UTF8TYPE));
    }

    private void dropKeyspaceIfExists() {
        try {
            cluster.dropKeyspace(TEST_KEYSPACE, true);
        } catch (HInvalidRequestException ignored) {
            // doesn't exist
        }
    }
}
