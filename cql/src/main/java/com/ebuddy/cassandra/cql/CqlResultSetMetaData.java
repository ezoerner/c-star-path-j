package com.ebuddy.cassandra.cql;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;

/**
 * // TODO: Class description.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class CqlResultSetMetaData implements ResultSetMetaData {
    private final ColumnDefinitions columnDefinitions;

    public CqlResultSetMetaData(ResultSet dataStaxResultSet) {
        this.columnDefinitions = dataStaxResultSet.getColumnDefinitions();
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnDefinitions.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int isNullable(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return columnDefinitions.getName(column - 1);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return columnDefinitions.getName(column - 1);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getScale(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public String getTableName(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T)this;
        }
        if (iface.isInstance(columnDefinitions)) {
            return (T)columnDefinitions;
        }
        throw new SQLException("ResultSetMetaData of type [" + getClass().getName() +
                                       "] cannot be unwrapped as [" + iface.getName() + "]");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this) || iface.isInstance(columnDefinitions);
    }
}
