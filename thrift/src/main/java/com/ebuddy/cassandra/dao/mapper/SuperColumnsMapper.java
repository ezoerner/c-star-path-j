package com.ebuddy.cassandra.dao.mapper;

import java.util.List;

import me.prettyprint.hector.api.beans.HSuperColumn;

/**
 * @author Aliaksandr Kazlou
 */
public interface SuperColumnsMapper<T,K,SN,N,V> {
    T mapSuperColumns(K rowKey, List<HSuperColumn<SN,N,V>> superColumns);
}
