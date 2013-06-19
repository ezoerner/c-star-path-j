package com.ebuddy.cassandra.cql;

import com.datastax.driver.core.*;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;

/**
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class DriverTest {
    private static final Logger LOG = Logger.getLogger(DriverTest.class);

    private static final String NODE = "127.0.0.1";

    private Cluster cluster;
    private Session session;


    @BeforeMethod
    public void setUp() throws Exception {
        cluster = Cluster.builder().addContactPoint(NODE).build();
        session = cluster.connect();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        cluster.shutdown();
    }

    // execute once to initialize data in database
    @Test(enabled = false)
    public void initData() {
        createSchema();
        loadData();
    }

    @Test
    public void testMetadata() throws Exception {
        Metadata metadata = cluster.getMetadata();
        assertTrue(metadata.getClusterName().length() > 0);
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Connected to cluster: %s\n", metadata.getClusterName()));
        }
        assertTrue(metadata.getAllHosts().size() > 0);
        for ( Host host : metadata.getAllHosts() ) {
            assertTrue(host.getDatacenter().length() > 0);
            assertNotNull(host.getAddress());
            assertTrue(host.getRack().length() > 0);
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Datacenter: %s; Host: %s; Rack: %s\n",
                        host.getDatacenter(), host.getAddress(), host.getRack()));
            }
        }
    }

    @Test
    public void testQuery() throws Exception {
        ResultSet results = session.execute("SELECT * FROM simplex.playlists WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
        List<Row> rows = results.all();
        assertEquals(rows.size(), 1);
        assertTrue(results.isExhausted());

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("%-30s\t%-20s\t%-20s","title", "album", "artist"));
            LOG.debug("-------------------------------+-----------------------+--------------------");
        }
        for (Row row : rows) {
            assertEquals(row.getString("title"), "La Petite Tonkinoise");
            assertEquals(row.getString("album"), "Bye Bye Blackbird");
            assertEquals(row.getString("artist"), "Joséphine Baker");

            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("%-30s\t%-20s\t%-20s", row.getString("title"), row.getString("album"), row.getString("artist")));
            }
        }
    }


    private void createSchema() {
        session.execute("CREATE KEYSPACE simplex WITH replication " +
                "= {'class':'SimpleStrategy', 'replication_factor':1};");

        session.execute(
                "CREATE TABLE simplex.songs (" +
                        "id uuid PRIMARY KEY," +
                        "title text," +
                        "album text," +
                        "artist text," +
                        "tags set<text>," +
                        "data blob" +
                        ");");
        session.execute(
                "CREATE TABLE simplex.playlists (" +
                        "id uuid," +
                        "title text," +
                        "album text, " +
                        "artist text," +
                        "song_id uuid," +
                        "PRIMARY KEY (id, title, album, artist)" +
                        ");");
    }

    public void loadData() {
        session.execute(
                "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
                        "VALUES (" +
                        "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                        "'La Petite Tonkinoise'," +
                        "'Bye Bye Blackbird'," +
                        "'Joséphine Baker'," +
                        "{'jazz', '2013'})" +
                        ";");
        session.execute(
                "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
                        "VALUES (" +
                        "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
                        "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                        "'La Petite Tonkinoise'," +
                        "'Bye Bye Blackbird'," +
                        "'Joséphine Baker'" +
                        ");");

    }
}
