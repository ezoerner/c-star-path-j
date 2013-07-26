package com.ebuddy.cassandra.cql.dao;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.ebuddy.cassandra.BatchContext;
import com.ebuddy.cassandra.StructuredDataSupport;
import com.ebuddy.cassandra.TypeReference;
import com.ebuddy.cassandra.structure.Composer;
import com.ebuddy.cassandra.structure.Decomposer;
import com.ebuddy.cassandra.structure.JacksonTypeReference;
import com.ebuddy.cassandra.structure.StructureConverter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ebuddy.cassandra.structure.Path;

/**
 * Implementation of StructuredDataSupport for CQL.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class CqlStructuredDataSupport<K> implements StructuredDataSupport<K> {
    private static final String DEFAULT_VALUE_COLUMN = "value";
    private static final String DEFAULT_PATH_COLUMN = "column1";
    private final JdbcTemplate jdbcTemplate;
    private final String pathColumnName;
    private final String valueColumnName;
    private final String tableName;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String readQueryString;
    private final Insert insertStatement;

    /**
     * Used for tables that are upgraded from a thrift dynamic column family that still has the default column names.
     */
    public CqlStructuredDataSupport(String tableName, JdbcTemplate jdbcTemplate) {
        this(tableName, DEFAULT_PATH_COLUMN, DEFAULT_VALUE_COLUMN, jdbcTemplate);
    }

    public CqlStructuredDataSupport(String tableName,
                                    String pathColumnName,
                                    String valueColumnName,
                                    JdbcTemplate jdbcTemplate) {
        Validate.notEmpty(tableName);
        this.jdbcTemplate = jdbcTemplate;
        this.pathColumnName = pathColumnName;
        this.valueColumnName = valueColumnName;
        this.tableName = tableName;
        readQueryString = select(pathColumnName, valueColumnName)
                .from(tableName)
                .where(gte(pathColumnName, bindMarker()))
                .and(lte(pathColumnName, bindMarker()))
                .getQueryString();
        insertStatement = insertInto(tableName)
                .value(pathColumnName, bindMarker())
                .value(valueColumnName, bindMarker());
    }

    @Override
    public <T> T readFromPath(K rowKey, String pathString, TypeReference<T> type) {
        validateArgs(rowKey, pathString);
        Path inputPath = Path.fromString(pathString);

        // converting from a string and back normalizes the path, e.g. makes sure ends with the delimiter character
        String start = inputPath.toString();
        String finish = start + Character.MAX_VALUE;

        PathMapRowCallbackHandler rowCallbackHandler = new PathMapRowCallbackHandler(inputPath);

        // note: prepared statements should be cached and reused by the connection pooling component....

        Object[] args = {start,finish};
        jdbcTemplate.query(readQueryString, args, rowCallbackHandler);
        Map<Path,Object> pathMap = rowCallbackHandler.getResultMap();
        if (pathMap.isEmpty()) {
            // not found
            return null;
        }

        Object structure = Composer.get().compose(pathMap);

        // convert object structure into POJO of type referred to by TypeReference
        return mapper.convertValue(structure, new JacksonTypeReference<T>(type));
    }

    @Override
    public void writeToPath(K rowKey, String pathString, Object value) {
        writeToPath(rowKey, pathString, value, null);
    }

    @Override
    public void writeToPath(K rowKey,
                            String pathString,
                            Object structuredValue,
                            BatchContext batchContext) {
        if (batchContext != null) {
            throw new UnsupportedOperationException("Batch updates not yet implemented");
        }

        validateArgs(rowKey, pathString);
        Object simplifiedStructure = mapper.convertValue(structuredValue, Object.class);
        Map<Path,Object> pathMap = Collections.singletonMap(Path.fromString(pathString), simplifiedStructure);
        Map<Path,Object> objectMap = Decomposer.get().decompose(pathMap);

        Batch batch = batch();
        List<Object> bindArguments = new LinkedList<Object>();
        for (Map.Entry<Path,Object> entry : objectMap.entrySet()) {
            batch.add(insertStatement);

            String stringValue = StructureConverter.get().toString(entry.getValue());

            bindArguments.add(entry.getKey().toString());
            bindArguments.add(stringValue);
        }

        // use queryForList since for some reason JdbcTemplate lacks a parameter binding method for execute
        jdbcTemplate.queryForList(batch.getQueryString(), Void.class, bindArguments.toArray());
    }

    @Override
    public void deletePath(K rowKey, String path) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void deletePath(K rowKey, String path, BatchContext batchContext) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private void validateArgs(K rowKey, String pathString) {
        Validate.notEmpty(pathString);
        Validate.notNull(rowKey);
    }

    private class PathMapRowCallbackHandler implements RowCallbackHandler {
        private final Path pathPrefix;
        private final Map<Path,Object> resultMap = new HashMap<Path,Object>();

        private PathMapRowCallbackHandler(Path pathPrefix) {
            this.pathPrefix = pathPrefix;
        }

        private Map<Path,Object> getResultMap() {
            return resultMap;
        }

        @Override
        public void processRow(ResultSet rs) throws SQLException {
            Path path = Path.fromString(rs.getString(pathColumnName));
            String valueString = rs.getString(valueColumnName);

            if (!path.startsWith(pathPrefix)) {
                throw new IllegalStateException("unexpected path found in database:" + path);
            }
            path = path.tail(pathPrefix.size());
            Object value = StructureConverter.get().fromString(valueString);
            resultMap.put(path, value);
        }
    }
}
