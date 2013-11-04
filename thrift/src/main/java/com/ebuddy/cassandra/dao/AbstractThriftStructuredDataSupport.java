package com.ebuddy.cassandra.dao;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.Validate;

import com.ebuddy.cassandra.Path;
import com.ebuddy.cassandra.StructuredDataSupport;
import com.ebuddy.cassandra.databind.CustomTypeResolverBuilder;
import com.ebuddy.cassandra.structure.DefaultPath;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Abstract implementation of StructuredDataSupport for Thrift.
 *
 * @param <K> the type of the row key
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public abstract class AbstractThriftStructuredDataSupport<K> implements StructuredDataSupport<K> {
    private  static final int MAX_CODE_POINT = 0x10FFFF;
    protected final ObjectMapper writeMapper;
    protected final ObjectMapper readMapper;

    protected AbstractThriftStructuredDataSupport() {
        readMapper = new ObjectMapper();
        writeMapper = new ObjectMapper();
        writeMapper.setDefaultTyping(new CustomTypeResolverBuilder());
    }

    protected void validateArgs(K rowKey, Path path) {
        Validate.isTrue(!path.isEmpty(), "Path must not be empty");
        Validate.notNull(rowKey, "Row key must not be empty");
    }

    protected final String getFinishString(String start) {
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
    protected final Map<Path,Object> getTerminalPathMap(Path inputPath, Map<String,Object> columnsMap) {
        Map<Path,Object> pathMap = new HashMap<Path,Object>(columnsMap.size());
        for (Map.Entry<String,Object> entry : columnsMap.entrySet()) {
            Path path = DefaultPath.fromEncodedPathString(entry.getKey());
            if (!path.startsWith(inputPath)) {
                throw new IllegalStateException("unexpected path found in database:" + path);
            }
            path = path.tail(inputPath.size());
            pathMap.put(path, entry.getValue());
        }
        return pathMap;
    }

    @Override
    public void writeToPath(K rowKey, Path path, Object value) {
        writeToPath(rowKey, path, value, null);
    }

    @Override
    public void deletePath(K rowKey, Path path) {
        deletePath(rowKey, path, null);
    }

    @Override
    public Path createPath(String... elements) {
        return DefaultPath.fromStrings(elements);
    }
}
