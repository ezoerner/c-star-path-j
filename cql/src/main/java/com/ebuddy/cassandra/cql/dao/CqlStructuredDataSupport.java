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

    private static final String BEGIN_BATCH = "begin batch\n";

    // TODO: Remove tableName from method parameters and add as a field
    // Since we are configuring with column names, we might as well configure with table name as well

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Used for tables that are upgraded from a thrift dynamic column family that still has the default column names.
     */
    public CqlStructuredDataSupport(JdbcTemplate jdbcTemplate) {
        this(jdbcTemplate, DEFAULT_PATH_COLUMN, DEFAULT_VALUE_COLUMN);
    }

    public CqlStructuredDataSupport(JdbcTemplate jdbcTemplate,
                                    String pathColumnName,
                                    String valueColumnName) {
        this.jdbcTemplate = jdbcTemplate;
        this.pathColumnName = pathColumnName;
        this.valueColumnName = valueColumnName;
    }

    @Override
    public <T> T readFromPath(String tableName, K rowKey, String pathString, TypeReference<T> type) {
        validateArgs(tableName, rowKey, pathString);
        Path inputPath = Path.fromString(pathString);

        // converting from a string and back normalizes the path, e.g. makes sure ends with the delimiter character
        String start = inputPath.toString();
        String finish = start + Character.MAX_VALUE;

        PathMapRowCallbackHandler rowCallbackHandler = new PathMapRowCallbackHandler(inputPath);

        // this prepared statement should be cached and reused by the connection pooling component....
        // TODO: once tableName is configured as a field, this query string can also be made a field

        String queryString = select(pathColumnName, valueColumnName)
                                .from(tableName)
                                .where(gte(pathColumnName, bindMarker()))
                                    .and(lte(pathColumnName, bindMarker()))
                             .getQueryString();

        Object[] args = {start,finish};
        jdbcTemplate.query(queryString, args, rowCallbackHandler);
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
    public void writeToPath(String tableName, K rowKey, String pathString, Object value) {
        writeToPath(tableName, rowKey, pathString, value, null);
    }

    @Override
    public void writeToPath(String tableName,
                            K rowKey,
                            String pathString,
                            Object structuredValue,
                            BatchContext batchContext) {
        if (batchContext != null) {
            throw new UnsupportedOperationException("Batch updates not yet implemented");
        }

        validateArgs(tableName, rowKey, pathString);
        Object simplifiedStructure = mapper.convertValue(structuredValue, Object.class);
        Map<Path,Object> pathMap = Collections.singletonMap(Path.fromString(pathString), simplifiedStructure);
        Map<Path,Object> objectMap = Decomposer.get().decompose(pathMap);

        // TODO: once tableName is configured as a field, these query string can also be made a field

        Batch batch = batch();
        List<Object> bindArguments = new LinkedList<Object>();
        for (Map.Entry<Path,Object> entry : objectMap.entrySet()) {
            batch.add(insertInto(tableName).value(pathColumnName, bindMarker()).value(valueColumnName, bindMarker()));

            String stringValue = StructureConverter.get().toString(entry.getValue());

            bindArguments.add(entry.getKey().toString());
            bindArguments.add(stringValue);
        }

        // use queryForList since for some reason JdbcTemplate lacks a parameter binding method for execute
        jdbcTemplate.queryForList(batch.getQueryString(), Void.class, bindArguments.toArray());
    }

    @Override
    public void deletePath(String tableName, K rowKey, String path) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void deletePath(String tableName, K rowKey, String path, BatchContext batchContext) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private void validateArgs(String columnFamily, K rowKey, String pathString) {
        Validate.notEmpty(columnFamily);
        Validate.notEmpty(pathString);
        Validate.notNull(rowKey);
    }

    private static class PathMapRowCallbackHandler implements RowCallbackHandler {
        private final Path headPath;
        private final Map<Path,Object> resultMap = new HashMap<Path,Object>();

        private PathMapRowCallbackHandler(Path headPath) {
            this.headPath = headPath;
        }

        private Map<Path,Object> getResultMap() {
            return resultMap;
        }

        @Override
        public void processRow(ResultSet rs) throws SQLException {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }
}
