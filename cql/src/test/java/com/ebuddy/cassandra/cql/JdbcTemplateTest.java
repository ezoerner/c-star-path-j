package com.ebuddy.cassandra.cql;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.datastax.driver.core.Cluster;

/**
 * // TODO: Class description.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class JdbcTemplateTest {
    private static final Logger LOG = Logger.getLogger(JdbcTemplateTest.class);
    private static final String NODE = "127.0.0.1";
    private Cluster cluster;

    private JdbcTemplate template;

    @BeforeMethod
    public void setUp() {
        cluster = Cluster.builder().addContactPoint(NODE).build();
        template = new JdbcTemplate(new DataStaxDataSource(cluster));
        createSchema();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        template.execute("drop keyspace simplex");
        cluster.shutdown();
    }

    @Test
    public void testLoadData() throws Exception {
        loadData();
    }

    ///////////// Private Methods /////////////

    private void createSchema() {
        //To change body of created methods use File | Settings | File Templates.
    }

    private void loadData() {
        template.execute("INSERT INTO simplex.songs (id, title, album, artist, tags) " +
                                 "VALUES (" +
                                 "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                                 "'La Petite Tonkinoise'," +
                                 "'Bye Bye Blackbird'," +
                                 "'Joséphine Baker'," +
                                 "{'jazz', '2013'})" +
                                 ";");
        template.execute("INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
                                 "VALUES (" +
                                 "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
                                 "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                                 "'La Petite Tonkinoise'," +
                                 "'Bye Bye Blackbird'," +
                                 "'Joséphine Baker'" +
                                 ");");
    }
}
