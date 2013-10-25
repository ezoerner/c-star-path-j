package com.ebuddy.cassandra.dao.visitor;

/**
 * Visit columns and return some value based on the internal logic
 *
 * @param <T> The result type.
 * @param <N> The type of a column name.
 * @param <V> The type of a column value.
 */
public interface ColumnVisitor<T, N, V> {

    /**
     * Visit easy column and evaluates a logic. 
     * @param columnName the name of the column
     * @param columnValue the value of the column
     * @param timestamp the cassandra timestamp when the column was written
     * @param ttl the time to live for that column value
     */
    void visit(N columnName, V columnValue, long timestamp, int ttl);
    
    /**
     * The value to be returned based on the logic
     * @return
     */
    T getResult();
}
