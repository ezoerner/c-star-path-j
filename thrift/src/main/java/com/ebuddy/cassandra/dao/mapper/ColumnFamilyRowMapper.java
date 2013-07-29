package com.ebuddy.cassandra.dao.mapper;

import java.util.List;

import me.prettyprint.hector.api.beans.HColumn;

/**
 * Maps rows into a parameterized type from a supercolumn family.
 *
 * @param <T> The result type.
 * @param <K> The type of a row key
 * @param <N> The type of a column name.
 * @param <V> The type of a column value.
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public interface ColumnFamilyRowMapper<T,K,N,V> {

    T mapRow(K rowKey, List<HColumn<N,V>> columns);

}
