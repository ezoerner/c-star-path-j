package com.ebuddy.cassandra.dao;

import java.util.Collection;

import javax.annotation.Nullable;

/**
 * Provides data access for a set of Strings to Cassandra, with optional support for a single
 * designated default element in the set. The column names and column values are required to be of type string.
 *
 * @param <K> type of row key
 * @param <SN> type of supercolumn name
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public interface StringSetDao<K,SN> {

    /**
     * Write an element in the set.
     * @param rowKey the row key
     * @param clusteringKey an optional clustering key. In legacy cassandra this is generally the supercolumn name
     * @param element the set element to write
     * @param isDefault true if this is the (new) default element
     */
    void writeElement(K rowKey, @Nullable SN clusteringKey, String element, boolean isDefault);

    /**
     * Read the default element of the set.
     *
     * @param rowKey the row key
     * @param clusteringKey an optional clustering key. In legacy cassandra this is generally the supercolumn name
     * @return the default element or null if there is no default element found.
     */
    String readDefaultElement(K rowKey, @Nullable SN clusteringKey);

    /**
     * Read all set elements.
     * @param rowKey the row key
     * @param clusteringKey an optional clustering key. In legacy cassandra this is generally the supercolumn name
     * @return the collection of String elements in the set
     */
    Collection<String> readElements(K rowKey, @Nullable SN clusteringKey);
}
