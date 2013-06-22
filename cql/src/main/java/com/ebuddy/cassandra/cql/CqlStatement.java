package com.ebuddy.cassandra.cql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import com.datastax.driver.core.Session;

/**
 * // TODO: Class description.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class CqlStatement implements Statement {
    private final Session session;
    private ResultSet resultSet;

    public CqlStatement(Session session) {
        this.session = session;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        resultSet = new CqlResultSet(session.execute(sql));
        return resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void close() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int getMaxRows() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void cancel() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        resultSet = new CqlResultSet(session.execute(sql));
        return true;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int getResultSetType() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
