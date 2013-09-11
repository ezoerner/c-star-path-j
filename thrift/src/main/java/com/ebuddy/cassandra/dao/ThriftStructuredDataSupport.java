package com.ebuddy.cassandra.dao;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.commons.lang3.Validate;

import com.ebuddy.cassandra.BatchContext;
import com.ebuddy.cassandra.Path;
import com.ebuddy.cassandra.StructuredDataSupport;
import com.ebuddy.cassandra.TypeReference;
import com.ebuddy.cassandra.databind.CustomTypeResolverBuilder;
import com.ebuddy.cassandra.structure.Composer;
import com.ebuddy.cassandra.structure.Decomposer;
import com.ebuddy.cassandra.structure.DefaultPath;
import com.ebuddy.cassandra.structure.JacksonTypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Implementation of StructuredDataSupport for the Thrift API access to a standard ColumnFamily.
 *
 * @param <K> the type of the row key
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class ThriftStructuredDataSupport<K> implements StructuredDataSupport<K> {
    private static final int MAX_CODE_POINT = 0x10FFFF;

    private final ColumnFamilyOperations<K,String,Object> operations;
    private final ObjectMapper writeMapper;
    private final ObjectMapper readMapper;

    /**
     * Create and configure an instance with a ColumnFamilyOperations.
     * @param operations a ColumnFamilyOperations that has a String column name and a StructureSerializer for the
     *                   valueSerializer.
     */
    public ThriftStructuredDataSupport(ColumnFamilyOperations<K,String,Object> operations) {
        this.operations = operations;
        writeMapper = new ObjectMapper();
        writeMapper.setDefaultTyping(new CustomTypeResolverBuilder());
        readMapper = new ObjectMapper();
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
    public void writeToPath(K rowKey, Path path, Object value) {
        writeToPath(rowKey, path, value, null);
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
    public void deletePath(K rowKey, Path path) {
        deletePath(rowKey, path, null);
    }

    @Override
    public void deletePath(K rowKey, Path path, @Nullable BatchContext batchContext) {
        // normalize path
        String start = path.toString();
        String finish = getFinishString(start);
        if (batchContext == null) {
            operations.deleteColumns(rowKey, start, finish);
        } else {
            operations.deleteColumns(rowKey, start, finish, batchContext);
        }
    }

    @Override
    public Path createPath(String... elements) {
        return DefaultPath.fromElements(elements);
    }

    private String getFinishString(String start) {
        int startCodePointCount = start.codePointCount(0, start.length());
        int finishCodePointCount = startCodePointCount + 1;
        int[] finishCodePoints = new int[finishCodePointCount];
        for (int i = 0; i < startCodePointCount; i++) {
            finishCodePoints[i] = start.codePointAt(i);
        }
        finishCodePoints[finishCodePointCount - 1] = MAX_CODE_POINT;
        return new String(finishCodePoints, 0, finishCodePointCount);
    }

    /**
     * Convert strings to paths and remove the start of the paths that match the inputPath.
     */
    private Map<Path,Object> getTerminalPathMap(Path inputPath, Map<String,Object> columnsMap) {
        Map<Path,Object> pathMap = new HashMap<Path,Object>(columnsMap.size());
        for (Map.Entry<String,Object> entry : columnsMap.entrySet()) {
            Path path = DefaultPath.fromString(entry.getKey());
            if (!path.startsWith(inputPath)) {
                throw new IllegalStateException("unexpected path found in database:" + path);
            }
            path = path.tail(inputPath.size());
            pathMap.put(path, entry.getValue());
        }
        return pathMap;
    }

    private void validateArgs(K rowKey, Path path) {
        Validate.isTrue(!path.isEmpty(), "Path must not be empty");
        Validate.notNull(rowKey, "Row key must not be empty");
    }
}
