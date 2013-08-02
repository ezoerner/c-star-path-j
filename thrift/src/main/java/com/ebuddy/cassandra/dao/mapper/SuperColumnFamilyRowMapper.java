package com.ebuddy.cassandra.dao.mapper;

import java.util.List;

import me.prettyprint.hector.api.beans.HSuperColumn;

/**
 * @author Aliaksandr Kazlou
 */
public interface SuperColumnFamilyRowMapper<T,K,SN,N,V> {
    T mapRow(K rowKey, List<HSuperColumn<SN,N,V>> superColumns);
}
