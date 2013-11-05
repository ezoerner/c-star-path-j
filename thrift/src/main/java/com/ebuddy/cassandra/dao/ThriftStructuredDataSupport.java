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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import com.ebuddy.cassandra.BatchContext;
import com.ebuddy.cassandra.Path;
import com.ebuddy.cassandra.TypeReference;
import com.ebuddy.cassandra.structure.Composer;
import com.ebuddy.cassandra.structure.Decomposer;
import com.ebuddy.cassandra.structure.JacksonTypeReference;

/**
 * Implementation of StructuredDataSupport for the Thrift API access to a standard ColumnFamily.
 *
 * @param <K> the type of the row key
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class ThriftStructuredDataSupport<K> extends AbstractThriftStructuredDataSupport<K> {

    private final ColumnFamilyOperations<K,String,Object> operations;

    /**
     * Create and configure an instance with a ColumnFamilyOperations.
     * @param operations a ColumnFamilyOperations that has a String column name and a StructureSerializer for the
     *                   valueSerializer.
     */
    public ThriftStructuredDataSupport(ColumnFamilyOperations<K,String,Object> operations) {
        this.operations = operations;
    }

    @Override
    public BatchContext beginBatch() {
        return operations.begin();
    }

    @Override
    public void applyBatch(BatchContext batchContext) {
        operations.commit(batchContext);
    }

    @Override
    public <T> T readFromPath(K rowKey, Path path, TypeReference<T> type) {
        validateArgs(rowKey, path);
        int count = Integer.MAX_VALUE;
        boolean reversed = false;

        // converting from a string and back normalizes the path, e.g. makes sure ends with the delimiter character
        String start = path.toString();
        String finish = getFinishString(start);
        Map<String,Object> columnsMap = operations.readColumnsAsMap(rowKey, start, finish, count, reversed);
        if (columnsMap.isEmpty()) {
            return null;
        }

        Map<Path,Object> pathMap = getTerminalPathMap(path, columnsMap);
        Object structure = Composer.get().compose(pathMap);

        // convert object structure into POJO of type referred to by TypeReference
        return readMapper.convertValue(structure, new JacksonTypeReference<T>(type));
    }

    @Override
    public void writeToPath(K rowKey,
                            Path path,
                            Object value,
                            @Nullable BatchContext batchContext) {
        validateArgs(rowKey, path);

        Object structure = writeMapper.convertValue(value, Object.class);

        Map<Path,Object> pathMap = Collections.singletonMap(path, structure);
        Map<Path,Object> objectMap = Decomposer.get().decompose(pathMap);

        Map<String,Object> stringMap = new HashMap<String,Object>();
        for (Map.Entry<Path,Object> entry : objectMap.entrySet()) {
            stringMap.put(entry.getKey().toString(), entry.getValue());
        }
        if (batchContext == null) {
            operations.writeColumns(rowKey, stringMap);
        } else {
            operations.writeColumns(rowKey, stringMap, batchContext);
        }
    }

    @Override
    public void deletePath(K rowKey, Path path, @Nullable BatchContext batchContext) {
        String start = path.toString();
        String finish = getFinishString(start);
        if (batchContext == null) {
            operations.deleteColumns(rowKey, start, finish);
        } else {
            operations.deleteColumns(rowKey, start, finish, batchContext);
        }
    }
}
