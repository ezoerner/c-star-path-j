package com.ebuddy.cassandra.cql;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class DriverTest {
    private static final Logger LOG = LoggerFactory.getLogger(DriverTest.class);

    private static final String NODE = "127.0.0.1";

    private Cluster cluster;
    private Session session;


    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();

        cluster = Cluster.builder().addContactPoint(NODE).withPort(9142).build();
        session = cluster.connect();
        createSchema();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        session.execute("drop keyspace simplex");
        cluster.shutdown();
    }

    @Test(groups = {"system"})
    public void testMetadata() throws Exception {
        Metadata metadata = cluster.getMetadata();
        assertTrue(metadata.getClusterName().length() > 0);
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Connected to cluster: %s\n", metadata.getClusterName()));
        }
        assertTrue(metadata.getAllHosts().size() > 0);
        for (Host host : metadata.getAllHosts()) {
            assertTrue(host.getDatacenter().length() > 0);
            assertNotNull(host.getAddress());
            assertTrue(host.getRack().length() > 0);
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Datacenter: %s; Host: %s; Rack: %s\n",
                                        host.getDatacenter(),
                                        host.getAddress(),
                                        host.getRack()));
            }
        }
    }

    @Test(groups = {"system"})
    public void testStaticSql() throws Exception {
        loadDataUsingStaticSql();
        testQueries();
    }


    @Test(groups = {"system"})
    public void testBoundStatements() throws Exception {
        loadDataUsingBoundStatements();
        testQueries();
    }

    @Test(groups = {"system"})
    public void testAsyncExecution() throws Exception {
        loadDataUsingBoundStatements();
        Query query = QueryBuilder.select().all().from("simplex", "songs");
        ResultSetFuture results = session.executeAsync(query);
        for (Row row : results.getUninterruptibly()) {
            String artist = row.getString("artist");
            String title = row.getString("title");
            String album = row.getString("album");
            assertEquals(title, "La Petite Tonkinoise'");
            assertEquals(album, "Bye Bye Blackbird'");
            assertEquals(artist, "Joséphine Baker");
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("%s: %s / %s\n", artist, title, album));
            }
        }
    }


///////////// Private Methods /////////////

    private void createSchema() {
        session.execute("CREATE KEYSPACE simplex WITH replication " + "= {'class':'SimpleStrategy', " +
                                "'replication_factor':1};");

        session.execute("CREATE TABLE simplex.songs (" +
                                "id uuid PRIMARY KEY," +
                                "title text," +
                                "album text," +
                                "artist text," +
                                "tags set<text>," +
                                "data blob" +
                                ");");
        session.execute("CREATE TABLE simplex.playlists (" +
                                "id uuid," +
                                "title text," +
                                "album text, " +
                                "artist text," +
                                "song_id uuid," +
                                "PRIMARY KEY (id, title, album, artist)" +
                                ");");
    }

    private void loadDataUsingStaticSql() {
        session.execute("INSERT INTO simplex.songs (id, title, album, artist, tags) " +
                                "VALUES (" +
                                "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                                "'La Petite Tonkinoise'," +
                                "'Bye Bye Blackbird'," +
                                "'Joséphine Baker'," +
                                "{'jazz', '2013'})" +
                                ";");
        session.execute("INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
                                "VALUES (" +
                                "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
                                "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                                "'La Petite Tonkinoise'," +
                                "'Bye Bye Blackbird'," +
                                "'Joséphine Baker'" +
                                ");");

    }

    private void loadDataUsingBoundStatements() {
        PreparedStatement statement = session.prepare(
                "INSERT INTO simplex.songs (id, title, album, artist, tags)  VALUES (?, ?, ?, ?, ?);");
        BoundStatement boundStatement = new BoundStatement(statement);
        Set<String> tags = new HashSet<String>();
        tags.add("jazz");
        tags.add("2013");
        session.execute(boundStatement.bind(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
                                            "La Petite Tonkinoise'",
                                            "Bye Bye Blackbird'",
                                            "Joséphine Baker",
                                            tags));
        statement = session.prepare("INSERT INTO simplex.playlists " +
                                            "(id, song_id, title, album, artist) " +
                                            "VALUES (?, ?, ?, ?, ?);");
        boundStatement = new BoundStatement(statement);
        session.execute(boundStatement.bind(UUID.fromString("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
                                            UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
                                            "La Petite Tonkinoise",
                                            "Bye Bye Blackbird",
                                            "Joséphine Baker"));

    }

    private void testQueries() {
        ResultSet results = session.execute(
                "SELECT * FROM simplex.playlists WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
        List<Row> rows = results.all();
        assertEquals(rows.size(), 1);
        assertTrue(results.isExhausted());

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("%-30s\t%-20s\t%-20s", "title", "album", "artist"));
            LOG.debug("-------------------------------+-----------------------+--------------------");
        }
        for (Row row : rows) {
            assertEquals(row.getString("title"), "La Petite Tonkinoise");
            assertEquals(row.getString("album"), "Bye Bye Blackbird");
            assertEquals(row.getString("artist"), "Joséphine Baker");

            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("%-30s\t%-20s\t%-20s",
                                        row.getString("title"),
                                        row.getString("album"),
                                        row.getString("artist")));
            }
        }
    }
}
