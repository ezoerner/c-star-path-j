package com.ebuddy.cassandra.dao;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.yaml.ClassPathYamlDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ebuddy.cassandra.dao.mapper.SuperColumnFamilyRowMapper;

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

    private static final String ALEX_ROW_KEY = "27e988f7-6d60-4410-ada5-fb3ebf884c68";
    private static final String MIKE_ROW_KEY = "9081707c-82cb-4d32-948d-25c4733453fc";
    private static final String SUPER_COLUMN_FAMILY = "SuperCF1";
    private static final String KEYSPACE_NAME = "SuperColumnFamilyTemplateIT";

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        // It starts only once. If this method has been already called, nothing will happen, Cassandra still be started
        // We can also if necessary clean the whole database and/or stop during the tests ;)
        // For advanced usage: see https://github.com/jsevellec/cassandra-unit/wiki/How-to-use-it-in-your-code
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();

        DataLoader dataLoader = new DataLoader(CLUSTER_NAME, HOST);
        ClassPathYamlDataSet dataSet = new ClassPathYamlDataSet("super-column-family-template-it-data.yaml");
        dataLoader.load(dataSet);
    }

    @Test(groups = "system")
    public void shouldMultiGetAllSuperColumns() throws Exception {
        Keyspace keyspace = getKeyspace();
        SuperColumnFamilyTemplate<String,String,String,String> template = new
                SuperColumnFamilyTemplate<String,String,String,String>(
                keyspace,
                SUPER_COLUMN_FAMILY,
                StringSerializer.get(),
                StringSerializer.get(),
                StringSerializer.get(),
                StringSerializer.get());

        template.multiGetAllSuperColumns(Arrays.asList(ALEX_ROW_KEY, MIKE_ROW_KEY),
                                         new SuperColumnFamilyRowMapper<Object,String,String,String,String>() {

            @Override
            public Void mapRow(String rowKey, List<HSuperColumn<String,String,String>> hColumns) {

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Mapping SuperColumns: rowKey=" + rowKey +
                                      ";hColumns=" + hColumns);
                }

                if (rowKey.equals(ALEX_ROW_KEY)) {
                    assertEquals(hColumns.size(), 2);
                    assertEquals(hColumns.get(0).getName(), "alex");
                    assertEquals(hColumns.get(1).getName(), "tom");
                }

                if (rowKey.equals(MIKE_ROW_KEY)) {
                    assertEquals(hColumns.size(), 1);
                    assertEquals(hColumns.get(0).getName(), "mike");
                }

                return null;
            }
        });
    }

    private Keyspace getKeyspace() {
        Cluster cluster = HFactory.getOrCreateCluster(CLUSTER_NAME, HOST);
        Keyspace keyspace = HFactory.createKeyspace(KEYSPACE_NAME, cluster);

        return keyspace;
    }
}
