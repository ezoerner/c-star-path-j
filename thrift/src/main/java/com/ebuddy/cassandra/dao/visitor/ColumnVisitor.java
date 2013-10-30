package com.ebuddy.cassandra.dao.visitor;

/**
 * Visit columns and perform some internal logic on the column data.
 *
 * @param <T> The result type.
 * @param <N> The type of a column name.
 * @param <V> The type of a column value.
 */
public interface ColumnVisitor<N, V> {

    /**
     * Visit easy column and evaluates a logic. 
     * @param columnName the name of the column
     * @param columnValue the value of the column
     * @param timestamp the cassandra timestamp when the column was written
     * @param ttl the time to live for that column value
     */
    void visit(N columnName, V columnValue, long timestamp, int ttl);

}
