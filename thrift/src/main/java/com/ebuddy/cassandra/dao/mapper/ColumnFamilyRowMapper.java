package com.ebuddy.cassandra.dao.mapper;

import me.prettyprint.hector.api.beans.HColumn;

import java.util.List;

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
