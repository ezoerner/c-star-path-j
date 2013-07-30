package com.ebuddy.cassandra.cql.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import org.springframework.jdbc.datasource.AbstractDataSource;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * DataSource implementation for the DataStax Java Driver for Cassandra.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class DataStaxDataSource extends AbstractDataSource {
    private final Cluster cluster;
    private String defaultKeyspace;

    public DataStaxDataSource(Cluster cluster, String defaultKeyspace) {
        this.cluster = cluster;
        this.defaultKeyspace = defaultKeyspace;
    }

    public DataStaxDataSource(Cluster cluster) {
        this(cluster, null);
    }

    public void setDefaultKeyspace(String defaultKeyspace) {
        this.defaultKeyspace = defaultKeyspace;
    }

    @Override
    public Connection getConnection() throws SQLException {
        Session session = defaultKeyspace == null ? cluster.connect() : cluster.connect(defaultKeyspace);
        return new CqlConnection(session);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }
}
