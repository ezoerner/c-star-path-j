package com.ebuddy.cassandra.cql;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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

    @Test(groups = {"system"})
    public void testLoadData() throws Exception {
        loadDataStaticSql();
    }

    @Test(groups = {"system"})
    public void testStaticSqlQueries() throws Exception {
        loadDataStaticSql();
        testQueryForMap();
    }

    @Test(groups = {"system"})
    public void testBoundStatements() throws Exception {
        loadDataUsingBoundStatements();
        testQueryForMap();
    }

    ///////////// Private Methods /////////////

    private void createSchema() {
        template.execute("CREATE KEYSPACE simplex WITH replication " + "= {'class':'SimpleStrategy', " +
                                "'replication_factor':1};");

        template.execute("CREATE TABLE simplex.songs (" +
                                "id uuid PRIMARY KEY," +
                                "title text," +
                                "album text," +
                                "artist text," +
                                "tags set<text>," +
                                "data blob" +
                                ");");
        template.execute("CREATE TABLE simplex.playlists (" +
                                "id uuid," +
                                "title text," +
                                "album text, " +
                                "artist text," +
                                "song_id uuid," +
                                "PRIMARY KEY (id, title, album, artist)" +
                                ");");
    }

    private void loadDataStaticSql() {
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

    private void loadDataUsingBoundStatements() {
        Set<String> tags = new HashSet<String>();
        tags.add("jazz");
        tags.add("2013");

        // use queryForList since for some reason JdbcTemplate lacks a parameter binding method for execute
        List<Void> result1 = template.queryForList(
                "INSERT INTO simplex.songs (id, title, album, artist, tags)  VALUES (?, ?, ?, ?, ?);",
                Void.class,
                UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
                "La Petite Tonkinoise'",
                "Bye Bye Blackbird'",
                "Joséphine Baker",
                tags);
        assertTrue(result1.isEmpty());

        List<Void> result2 = template.queryForList("INSERT INTO simplex.playlists " +
                                                          "(id, song_id, title, album, artist) " +
                                                          "VALUES (?, ?, ?, ?, ?);",
                                                  Void.class,
                                                  UUID.fromString("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
                                                  UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"),
                                                  "La Petite Tonkinoise",
                                                  "Bye Bye Blackbird",
                                                  "Joséphine Baker");
        assertTrue(result2.isEmpty());
    }


    private void testQueryForMap() {
        Map<String,Object> results = template.queryForMap(
                "SELECT * FROM simplex.playlists WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("%-30s\t%-20s\t%-20s", "title", "album", "artist"));
            LOG.debug("-------------------------------+-----------------------+--------------------");
        }
        String title = (String)results.get("title");
        String album = (String)results.get("album");
        String artist = (String)results.get("artist");
        assertEquals(title, "La Petite Tonkinoise");
        assertEquals(album, "Bye Bye Blackbird");
        assertEquals(artist, "Joséphine Baker");

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("%-30s\t%-20s\t%-20s",
                                    title,
                                    album,
                                    artist));
        }
    }
}
