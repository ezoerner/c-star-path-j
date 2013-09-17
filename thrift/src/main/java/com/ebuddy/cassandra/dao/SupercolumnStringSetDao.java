/*
 * Copyright 2013 eBuddy B.V.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.ebuddy.cassandra.dao;

import java.util.Collection;

import org.apache.commons.lang3.Validate;

import com.ebuddy.cassandra.BatchContext;
import com.ebuddy.cassandra.dao.mapper.ColumnMapper;

/**
 * Implementation of StringSetDao that stores a set of values in a supercolumn.
 * This implementation requires the clusteringKey to be the supercolumn name and to
 * not be null.
 *
 * @param <K> the row key type
 * @param <SN> the supercolumn name type
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class SupercolumnStringSetDao<K,SN> implements StringSetDao<K,SN> {

    private final SuperColumnFamilyOperations<K,SN,String,String> operations;
    private final String tag;
    private final String tagEnd;
    private final String defaultTag;
    private final ColumnMapper<String,String,String> columnMapper;


    public SupercolumnStringSetDao(String tag,
                                   String defaultTag,
                                   SuperColumnFamilyOperations<K,SN,String,String> operations) {
        Validate.notNull(operations);
        Validate.notEmpty(tag);
        this.operations = operations;
        this.tag = tag + ":";
        tagEnd = this.tag + Character.MAX_VALUE;
        this.defaultTag = defaultTag;

        columnMapper = new ColumnMapper<String,String,String>() {
            @Override
            public String mapColumn(String columnName, String columnValue) {
                return columnName.substring(SupercolumnStringSetDao.this.tag.length());
            }
        };
    }

    @Override
    public void writeElement(K rowKey, SN clusteringKey, String element, boolean isDefault) {
        Validate.notNull(clusteringKey);
        BatchContext batchContext = operations.begin();
        String propertyName = tag + element;
        operations.writeColumn(rowKey, clusteringKey, propertyName, "", batchContext);
        if (isDefault) {
            operations.writeColumn(rowKey, clusteringKey, defaultTag, element, batchContext);
        }
        operations.commit(batchContext);
    }

    @Override
    public String readDefaultElement(K rowKey, SN clusteringKey) {
        Validate.notNull(clusteringKey);
        return operations.readColumnValue(rowKey, clusteringKey, defaultTag);
    }

    @Override
    public Collection<String> readElements(K rowKey, SN clusteringKey) {
        Validate.notNull(clusteringKey);
        return operations.readColumns(rowKey,
                                      clusteringKey,
                                      tag,
                                      tagEnd,
                                      Integer.MAX_VALUE,
                                      false,
                                      columnMapper);
    }
}
