package com.ebuddy.cassandra.dao;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.commons.lang.Validate;

import com.ebuddy.cassandra.BatchContext;
import com.ebuddy.cassandra.StructureDao;
import com.ebuddy.cassandra.structure.Composer;
import com.ebuddy.cassandra.structure.Decomposer;
import com.ebuddy.cassandra.structure.Path;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Implementation of StructureDao for the Thrift API access to a standard ColumnFamily.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class ThriftStructureDao<K> implements StructureDao<K> {

    private final ColumnFamilyOperations<K,String,Object> operations;
    private final ObjectMapper mapper = new ObjectMapper();

    public ThriftStructureDao(ColumnFamilyOperations<K,String,Object> operations) {
        this.operations = operations;
    }

    @Override
    public <T> T readFromPath(String columnFamily, K rowKey, String pathString, TypeReference<T> type) {
        validateArgs(columnFamily, rowKey, pathString);
        Path path = Path.fromString(pathString);
        int count = Integer.MAX_VALUE;
        boolean reversed = false;

        // normalizes the path, e.g. makes sure ends with the delimiter character
        String start = path.toString();
        String finish = start + Character.MAX_VALUE;
        Map<String,Object> columnsMap = operations.readColumnsAsMap(columnFamily, rowKey, start, finish, count, reversed);

        // convert strings to paths

        // TODO: (performance) Adding support to ColumnFamilyOperations for a ColumnMapper
        // that returns a map of N,V objects
        // (instead of just List<T>) so we don't have to do another pass on the map entries here.

        Map<Path,Object> pathMap = new HashMap<Path,Object>(columnsMap.size());
        for (Map.Entry<String,Object> entry : columnsMap.entrySet()) {
            pathMap.put(Path.fromString(entry.getKey()), entry.getValue());
        }
        Object structure = Composer.get().compose(pathMap);

        // convert object structure into POJO of type referred to by TypeReference
        return mapper.convertValue(structure, type);
    }

    @Override
    public void writeToPath(String columnFamily, K rowKey, String pathString, Object value) {
        writeToPath(columnFamily, rowKey, pathString, value, null);
    }

    @Override
    public void writeToPath(String columnFamily,
                            K rowKey,
                            String pathString,
                            Object value,
                            @Nullable BatchContext batchContext) {
        validateArgs(columnFamily, rowKey, pathString);

        Map<Path,Object> structures = Collections.singletonMap(Path.fromString(pathString), value);
        Map<Path,Object> objectMap = Decomposer.get().decompose(structures);

        Map<String,Object> stringObjectMap = new HashMap<String,Object>();
        for (Map.Entry<Path,Object> entry : objectMap.entrySet()) {
            stringObjectMap.put(entry.getKey().toString(), entry.getValue());
        }
        if (batchContext == null) {
            operations.writeColumns(columnFamily, rowKey, stringObjectMap);
        } else {
            operations.writeColumns(columnFamily, rowKey, stringObjectMap, batchContext);
        }
    }

    private void validateArgs(String columnFamily, K rowKey, String pathString) {
        Validate.notEmpty(columnFamily);
        Validate.notEmpty(pathString);
        Validate.notNull(rowKey);
    }
}
