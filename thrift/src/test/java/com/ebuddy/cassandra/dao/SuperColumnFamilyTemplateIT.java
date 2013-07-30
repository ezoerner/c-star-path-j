package com.ebuddy.cassandra.dao;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.yaml.ClassPathYamlDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ebuddy.cassandra.dao.mapper.SuperColumnsMapper;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * @author Aliaksandr Kazlou
 */

// TODO: Create abstract TestNG class for IT support (which will start the server, load the data set and provide access to keyspace)
public class SuperColumnFamilyTemplateIT {
    private static final Logger LOG = Logger.getLogger(SuperColumnFamilyTemplateIT.class);

    private static final String CLUSTER_NAME = "Test Cluster";
    private static final String HOST = "localhost:9171";

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        // It starts only once. If this method has been already called, nothing will happen, Cassandra still be started
        // We can clean the whole database and stop ;)
        // For advanced usage: see https://github.com/jsevellec/cassandra-unit/wiki/How-to-use-it-in-your-code
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();

        DataLoader dataLoader = new DataLoader(CLUSTER_NAME, HOST);
        ClassPathYamlDataSet dataSet = new ClassPathYamlDataSet("super-column-family-template-data.yaml");
        dataLoader.load(dataSet);
    }

    @Test(groups = "system")
    public void shouldMultiGetAllSuperColumns() throws Exception {
        Keyspace keyspace = getKeyspace();
        SuperColumnFamilyTemplate<String,String,String,String> template = new
                SuperColumnFamilyTemplate<String,String,String,String>(
                keyspace,
                "SuperCF1",
                StringSerializer.get(),
                StringSerializer.get(),
                StringSerializer.get(),
                StringSerializer.get());

        final CountDownLatch latch = new CountDownLatch(2);
        template.multiGetAllSuperColumns(Arrays.asList("27e988f7-6d60-4410-ada5-fb3ebf884c68",
                                                       "9081707c-82cb-4d32-948d-25c4733453fc"),
                                         new SuperColumnsMapper<Object,String,String,String,String>() {

            @Override
            public List<Object> mapSuperColumns(String rowKey, List<HSuperColumn<String,String,String>> hColumns) {

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Mapping SuperColumns: rowKey=" + rowKey +
                                      ";hColumns=" + hColumns);
                }

                if (rowKey.equals("27e988f7-6d60-4410-ada5-fb3ebf884c68")) {
                    assertEquals(hColumns.size(), 2);
                    assertEquals(hColumns.get(0).getName(), "alex");
                    assertEquals(hColumns.get(1).getName(), "tom");
                }

                if (rowKey.equals("9081707c-82cb-4d32-948d-25c4733453fc")) {
                    assertEquals(hColumns.size(), 1);
                    assertEquals(hColumns.get(0).getName(), "mike");
                }


                if (rowKey.equals("27e988f7-6d60-4410-ada5-fb3ebf884c68") ||
                        rowKey.equals("9081707c-82cb-4d32-948d-25c4733453fc")) {
                    latch.countDown();
                }

                return new ArrayList<Object>();
            }
        });

        boolean timeout = !latch.await(10, TimeUnit.SECONDS);
        if (timeout) {
            fail("Expect callbacks to be called 2 times, so await should be completed before the timeout!");
        }
    }

    private Keyspace getKeyspace() {
        Cluster cluster = HFactory.getOrCreateCluster(CLUSTER_NAME, HOST);
        Keyspace keyspace = HFactory.createKeyspace("SuperColumnFamilyTemplate", cluster);

        return keyspace;
    }
}
