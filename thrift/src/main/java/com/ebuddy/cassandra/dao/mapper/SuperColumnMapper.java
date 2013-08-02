package com.ebuddy.cassandra.dao.mapper;

import java.util.List;

import me.prettyprint.hector.api.beans.HColumn;

/**
 * Maps supercolumns into a parameterized type.
 *
 * @param <T> The result type.
 * @param <K> The row key type.
 * @param <SN> The type of a supercolumn name
 * @param <N> The type of a column name.
 * @param <V> The type of a column value.
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public interface SuperColumnMapper<T,K,SN,N,V> {

    T mapSuperColumn(K rowKey, SN superColumnName, List<HColumn<N,V>> columns);

}
