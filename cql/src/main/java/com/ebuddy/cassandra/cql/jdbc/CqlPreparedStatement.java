package com.ebuddy.cassandra.cql.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * // TODO: Add class description here.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class CqlPreparedStatement implements PreparedStatement {
    private final com.datastax.driver.core.PreparedStatement dataStaxPreparedStatement;
    private final BoundStatement boundStatement;
    private final Session session;

    public CqlPreparedStatement(com.datastax.driver.core.PreparedStatement dataStaxPreparedStatement,
                                Session session) {
        this.dataStaxPreparedStatement = dataStaxPreparedStatement;
        this.session = session;
        boundStatement = new BoundStatement(dataStaxPreparedStatement);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return new CqlResultSet(session.execute(boundStatement));
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        boundStatement.setString(parameterIndex - 1, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void clearParameters() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {

    }

    @SuppressWarnings("fallthrough")
    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        int index = parameterIndex - 1;  // convert from 1-based to 0-based

        if (x == null) {
            boundStatement.setBytesUnsafe(index, null);
            return;
        }

        DataType columnType = dataStaxPreparedStatement.getVariables().getType(index);

        Class<?> providedClass = x.getClass();
        Class<?> expectedClass = columnType.getName().asJavaClass();
        if (!expectedClass.isAssignableFrom(providedClass)) {
            throw new InvalidTypeException(String.format("Invalid type for CQL type %s, expecting %s but %s provided",
                                                         columnType,
                                                         expectedClass,
                                                         providedClass));
        }

        switch (columnType.getName()) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                boundStatement.setString(index, (String)x);
                break;
            case BIGINT:
            case COUNTER:
                //noinspection ConstantConditions
                boundStatement.setLong(index, ((Number)x).longValue());
                break;
            case BLOB:
                boundStatement.setBytes(index, (ByteBuffer)x);
                break;
            case BOOLEAN:
                boundStatement.setBool(index, (Boolean)x);
                break;
            case DECIMAL:
                boundStatement.setDecimal(index, (BigDecimal)x);
                break;
            case DOUBLE:
                boundStatement.setDouble(index, ((Number)x).doubleValue());
                break;
            case FLOAT:
                boundStatement.setFloat(index, ((Number)x).floatValue());
                break;
            case INET:
                boundStatement.setInet(index, (InetAddress)x);
                break;
            case INT:
                boundStatement.setInt(index, ((Number)x).intValue());
                break;
            case TIMESTAMP:
                boundStatement.setDate(index, (java.util.Date)x);
                break;
            case UUID:
            case TIMEUUID:
                boundStatement.setUUID(index, (UUID)x);
                break;
            case VARINT:
                boundStatement.setVarint(index, (BigInteger)x);
                break;
            case LIST:
                boundStatement.setList(index, (List<?>)x);
                break;
            case SET:
                boundStatement.setSet(index, (Set<?>)x);
                break;
            case MAP:
                boundStatement.setMap(index, (Map<?,?>)x);
                break;
            case CUSTOM:
                boundStatement.setBytesUnsafe(index, (ByteBuffer)x);
            default:
                // some new type we don't know about?
                throw new UnsupportedOperationException("Don't know about type " + columnType.getName());
        }
    }

    @Override
    public boolean execute() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void addBatch() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void close() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getMaxRows() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void cancel() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getUpdateCount() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getResultSetType() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
