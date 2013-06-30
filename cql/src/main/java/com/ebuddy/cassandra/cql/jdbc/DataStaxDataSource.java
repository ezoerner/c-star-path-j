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

    public DataStaxDataSource(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public Connection getConnection() throws SQLException {
        Session session = cluster.connect();
        return new CqlConnection(session);
    }

    @Override
    public Connection getConnection(String s, String s2) throws SQLException {
        return getConnection();
    }
}
