package com.ebuddy.cassandra.cql.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

import com.datastax.driver.core.Session;

/**
 * java.sql.Connection wrapper around a datastax Connection.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class CqlConnection implements Connection {
    private final Session session;

    public CqlConnection(Session session) {
        this.session = session;
    }

    @Override
    public Statement createStatement() {
        return new CqlStatement(session);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new CqlPreparedStatement(session.prepare(sql), session);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void commit() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void rollback() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Map<String,Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setTypeMap(Map<String,Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public PreparedStatement prepareStatement(String sql,
                                              int resultSetType,
                                              int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public CallableStatement prepareCall(String sql,
                                         int resultSetType,
                                         int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("not yet implemented");  //To change body of implemented methods use File | Settings | File Templates.
    }
}