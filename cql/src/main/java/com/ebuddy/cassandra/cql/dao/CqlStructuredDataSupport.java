package com.ebuddy.cassandra.cql.dao;

import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

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
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.ebuddy.cassandra.BatchContext;
import com.ebuddy.cassandra.StructuredDataSupport;
import com.ebuddy.cassandra.TypeReference;
import com.ebuddy.cassandra.databind.CustomTypeResolverBuilder;
import com.ebuddy.cassandra.structure.Composer;
import com.ebuddy.cassandra.structure.Decomposer;
import com.ebuddy.cassandra.structure.JacksonTypeReference;
import com.ebuddy.cassandra.structure.Path;
import com.ebuddy.cassandra.structure.StructureConverter;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Implementation of StructuredDataSupport for CQL.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class CqlStructuredDataSupport<K> implements StructuredDataSupport<K> {
    private static final String DEFAULT_VALUE_COLUMN = "value";
    private static final String DEFAULT_PATH_COLUMN = "column1";
    private static final String DEFAULT_PARTITION_KEY_COLUMN = "key";
    private final JdbcTemplate jdbcTemplate;
    private final String pathColumnName;
    private final String valueColumnName;
    private final ObjectMapper writeMapper;
    private final ObjectMapper readMapper;
    private final String readPathQueryString;
    private final Insert insertStatement;
    private final String readForDeleteString;
    private final String tableName;
    private final String partitionKeyColumnName;

    /**
     * Used for tables that are upgraded from a thrift dynamic column family that still has the default column names.
     */
    public CqlStructuredDataSupport(String tableName, JdbcTemplate jdbcTemplate) {
        this(tableName, DEFAULT_PARTITION_KEY_COLUMN, DEFAULT_PATH_COLUMN, DEFAULT_VALUE_COLUMN, jdbcTemplate);
    }

    public CqlStructuredDataSupport(String tableName,
                                    String partitionKeyColumnName,
                                    String pathColumnName,
                                    String valueColumnName,
                                    JdbcTemplate jdbcTemplate) {
        Validate.notEmpty(tableName);
        this.jdbcTemplate = jdbcTemplate;
        this.pathColumnName = pathColumnName;
        this.valueColumnName = valueColumnName;

        writeMapper = new ObjectMapper();
        writeMapper.setDefaultTyping(new CustomTypeResolverBuilder());
        readMapper = new ObjectMapper();

        readPathQueryString = select(pathColumnName, valueColumnName)
                .from(tableName)
                .where(eq(partitionKeyColumnName, bindMarker()))
                .and(gte(pathColumnName, bindMarker()))
                .and(lte(pathColumnName, bindMarker()))
                .getQueryString();
        readForDeleteString = select(pathColumnName)
                .from(tableName)
                .where(eq(partitionKeyColumnName, bindMarker()))
                .and(gte(pathColumnName, bindMarker()))
                .and(lte(pathColumnName, bindMarker()))
                .getQueryString();
        insertStatement = insertInto(tableName)
                .value(partitionKeyColumnName, bindMarker())
                .value(pathColumnName, bindMarker())
                .value(valueColumnName, bindMarker());
        this.tableName = tableName;
        this.partitionKeyColumnName = partitionKeyColumnName;
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

        Object[] args = {rowKey,start,finish};
        jdbcTemplate.query(readPathQueryString, args, rowCallbackHandler);
        Map<Path,Object> pathMap = rowCallbackHandler.getResultMap();
        if (pathMap.isEmpty()) {
            // not found
            return null;
        }

        Object structure = Composer.get().compose(pathMap);

        // convert object structure into POJO of type referred to by TypeReference
        return readMapper.convertValue(structure, new JacksonTypeReference<T>(type));
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
        Object simplifiedStructure = writeMapper.convertValue(structuredValue, Object.class);
        Map<Path,Object> pathMap = Collections.singletonMap(Path.fromString(pathString), simplifiedStructure);
        Map<Path,Object> objectMap = Decomposer.get().decompose(pathMap);

        Batch batch = batch();
        List<Object> bindArguments = new LinkedList<Object>();
        for (Map.Entry<Path,Object> entry : objectMap.entrySet()) {
            batch.add(insertStatement);

            String stringValue = StructureConverter.get().toString(entry.getValue());

            bindArguments.add(rowKey);
            bindArguments.add(entry.getKey().toString());
            bindArguments.add(stringValue);
        }

        // use queryForList since for some reason JdbcTemplate lacks a parameter binding method for execute
        jdbcTemplate.queryForList(batch.getQueryString(), Void.class, bindArguments.toArray());
    }

    @Override
    public void deletePath(K rowKey, String path) {
        deletePath(rowKey, path, null);
    }

    @Override
    public void deletePath(K rowKey, String pathString, BatchContext batchContext) {
        if (batchContext != null) {
            throw new UnsupportedOperationException("Batch updates not yet implemented");
        }

        validateArgs(rowKey, pathString);
        Path inputPath = Path.fromString(pathString);

        // converting from a string and back normalizes the path, e.g. makes sure ends with the delimiter character
        String start = inputPath.toString();
        String finish = start + Character.MAX_VALUE;

        // would like to just do a delete with a where clause, but unfortunately Cassandra can't do that in CQL (either)
        // with >= and <=

        // Also, we can't delete primary key columns without deleting the whole row, so we delete here only the
        // value column. However, this leaves the row behind with a null value. So in our query for readFromPath
        // we filter out null values. Strange.

        Object[] args = {rowKey,start,finish};
        List<String> pathsToDelete = jdbcTemplate.queryForList(readForDeleteString, args, String.class);
        if (pathsToDelete.isEmpty()) {
            // not found
            return;
        }

        Delete deleteStatement = delete(valueColumnName)
                .from(tableName);
        deleteStatement
                .where(eq(partitionKeyColumnName, rowKey))
                .and(eq(pathColumnName, bindMarker()));

        Batch batch = batch();
        List<Object> bindArguments = new LinkedList<Object>();
        for (String pathToDelete : pathsToDelete) {
            batch.add(deleteStatement);
            bindArguments.add(pathToDelete);
        }

        jdbcTemplate.queryForList(batch.getQueryString(), Void.class, bindArguments.toArray());
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
            String valueString = rs.getString(valueColumnName);
            if (valueString == null) {
                // a "real" null here means that this is a deleted row!! See comment in deletePath
                return;
            }

            Path path = Path.fromString(rs.getString(pathColumnName));

            if (!path.startsWith(pathPrefix)) {
                throw new IllegalStateException("unexpected path found in database:" + path);
            }
            path = path.tail(pathPrefix.size());
            Object value = StructureConverter.get().fromString(valueString);
            // this can be a null converted from a JSON null
            resultMap.put(path, value);
        }
    }
}
