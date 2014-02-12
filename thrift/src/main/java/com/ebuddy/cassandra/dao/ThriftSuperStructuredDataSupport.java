package com.ebuddy.cassandra.dao;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.ebuddy.cassandra.BatchContext;
import com.ebuddy.cassandra.Path;
import com.ebuddy.cassandra.TypeReference;
import com.ebuddy.cassandra.structure.Composer;
import com.ebuddy.cassandra.structure.Decomposer;
import com.ebuddy.cassandra.structure.JacksonTypeReference;

/**
 * Implementation of StructuredDataSupport for a Thrift SuperColumnFamily.
 *
 * @param<K> type of row key
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class ThriftSuperStructuredDataSupport<K> extends AbstractThriftStructuredDataSupport<K> {

    private final SuperColumnFamilyOperations<K,String,String,Object> operations;

    /**
     * Create and configure an instance with a SuperColumnFamilyOperations.
     * @param operations a SuperColumnFamilyOperations that has String column and supercolumn names and a
     *                   StructureSerializer for the valueSerializer.
     */
    public ThriftSuperStructuredDataSupport(SuperColumnFamilyOperations<K,String,String,Object> operations) {
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
        String superColumnName = path.head();
        Path rest = path.tail();
        String start = rest.toString();
        String finish = getFinishString(start);
        Map<String,Object> columnsMap = operations.readColumnsAsMap(rowKey,
                                                                    superColumnName,
                                                                    start,
                                                                    finish,
                                                                    count,
                                                                    reversed);
        if (columnsMap.isEmpty()) {
            return null;
        }

        Map<Path,Object> pathMap = getTerminalPathMap(rest, columnsMap);
        Object structure = Composer.get().compose(pathMap);

        // convert object structure into POJO of type referred to by TypeReference
        return readMapper.convertValue(structure, new JacksonTypeReference<T>(type));
    }

    @Override
    public void writeToPath(K rowKey, Path path, Object value, BatchContext batchContext) {
        validateArgs(rowKey, path);

        Object structure = writeMapper.convertValue(value, Object.class);

        String superColumnName = path.head();
        Path rest = path.tail();

        Map<Path,Object> pathMap = Collections.singletonMap(rest, structure);
        Map<Path,Object> objectMap = Decomposer.get().decompose(pathMap);

        Map<String,Object> stringMap = new HashMap<String,Object>();
        for (Map.Entry<Path,Object> entry : objectMap.entrySet()) {
            stringMap.put(entry.getKey().toString(), entry.getValue());
        }

        if (batchContext == null) {
            operations.writeColumns(rowKey, superColumnName, stringMap);
        } else {
            operations.writeColumns(rowKey, superColumnName, stringMap, batchContext);
        }
    }

    @Override
    public void deletePath(K rowKey, Path path, BatchContext batchContext) {
        String superColumnName = path.head();

        String start = path.tail().toString();
        String finish = getFinishString(start);
        if (batchContext == null) {
            operations.deleteColumns(rowKey, superColumnName, start, finish);
        } else {
            operations.deleteColumns(rowKey, superColumnName, start, finish, batchContext);
        }
    }
}
