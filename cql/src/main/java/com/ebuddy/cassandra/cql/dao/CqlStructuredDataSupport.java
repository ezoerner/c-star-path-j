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
package com.ebuddy.cassandra.cql.dao;

import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.Validate;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Delete;
import com.ebuddy.cassandra.BatchContext;
import com.ebuddy.cassandra.Path;
import com.ebuddy.cassandra.StructuredDataSupport;
import com.ebuddy.cassandra.TypeReference;
import com.ebuddy.cassandra.databind.CustomTypeResolverBuilder;
import com.ebuddy.cassandra.structure.Composer;
import com.ebuddy.cassandra.structure.Decomposer;
import com.ebuddy.cassandra.structure.DefaultPath;
import com.ebuddy.cassandra.structure.JacksonTypeReference;
import com.ebuddy.cassandra.structure.StructureConverter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;

/**
 * Implementation of StructuredDataSupport for CQL.
 *
 * To use structured data in CQL3, the following data modeling rules apply:
 * <ul>
 *     <li>There must be a designated path column and it must be the first clustering key, i.e. the next element of the
 *         primary key after the partition key.</li>
 *     <li>There must be a designated value column.</li>
 *     <li>There can only be one designated path and one designated value column per table.</li>
 *     <li>The designated path and value columns must be typed as a textual type.</li>
 * </ul>
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class CqlStructuredDataSupport<K> implements StructuredDataSupport<K> {
    private static final String DEFAULT_VALUE_COLUMN = "value";
    private static final String DEFAULT_PATH_COLUMN = "column1";
    private static final String DEFAULT_PARTITION_KEY_COLUMN = "key";

    private static final int MAX_CODE_POINT = 0x10FFFF;

    private static final AtomicLong lastTime = new AtomicLong();

    private final Session session;
    private final String pathColumnName;
    private final String valueColumnName;
    private final ObjectMapper writeMapper;
    private final ObjectMapper readMapper;

    private final PreparedStatement readPathQuery;
    private final PreparedStatement readForDeleteQuery;

    private final String tableName;
    private final String partitionKeyColumnName;

    /** The default consistency level for all operations. */
    private final ConsistencyLevel defaultConsistencyLevel;

    /**
     * Used for tables that are upgraded from a thrift dynamic column family that still have the default column names.
     * @param session a Session configured with the keyspace
     */
    public CqlStructuredDataSupport(String tableName, ConsistencyLevel defaultConsistencyLevel, Session session) {
        this(tableName,
             DEFAULT_PARTITION_KEY_COLUMN,
             DEFAULT_PATH_COLUMN,
             DEFAULT_VALUE_COLUMN,
             defaultConsistencyLevel,
             session);
    }

    /**
     * Construct an instance of CqlStructuredDataSupport with the specified table and column names.
     * @param session a Session configured with the keyspace
     */
    public CqlStructuredDataSupport(String tableName,
                                    String partitionKeyColumnName,
                                    String pathColumnName,
                                    String valueColumnName,
                                    ConsistencyLevel defaultConsistencyLevel,
                                    Session session) {
        Validate.notEmpty(tableName);
        this.session = session;
        this.pathColumnName = pathColumnName;
        this.valueColumnName = valueColumnName;
        this.defaultConsistencyLevel = defaultConsistencyLevel;

        writeMapper = new ObjectMapper();
        writeMapper.setDefaultTyping(new CustomTypeResolverBuilder());
        readMapper = new ObjectMapper();
        this.tableName = tableName;
        this.partitionKeyColumnName = partitionKeyColumnName;

        readPathQuery = session.prepare(select(pathColumnName, valueColumnName)
                .from(tableName)
                .where(eq(partitionKeyColumnName, bindMarker()))
                    .and(gte(pathColumnName, bindMarker()))
                    .and(lte(pathColumnName, bindMarker()))
                .getQueryString());
        readPathQuery.setConsistencyLevel(defaultConsistencyLevel);

        readForDeleteQuery = session.prepare(select(pathColumnName)
                                                     .from(tableName)
                                                     .where(eq(partitionKeyColumnName, bindMarker()))
                                                        .and(gte(pathColumnName, bindMarker()))
                                                        .and(lte(pathColumnName, bindMarker()))
                                                     .getQueryString());
        readForDeleteQuery.setConsistencyLevel(defaultConsistencyLevel);
    }

    @Override
    public BatchContext beginBatch() {
        return new CqlBatchContext();
    }

    @Override
    public void applyBatch(BatchContext batchContext) {
        Batch batch = validateAndGetBatch(batchContext);
        List<Object> bindArguments = ((CqlBatchContext)batchContext).getBindArguments();
        Query query;
        if (bindArguments.isEmpty()) {
            query = new SimpleStatement(batch.getQueryString());
        } else {
            query = session.prepare(batch.getQueryString()).bind(bindArguments.toArray());
        }
        query.setConsistencyLevel(defaultConsistencyLevel);
        session.execute(query);
        ((CqlBatchContext)batchContext).reset();
    }

    @Override
    public <T> T readFromPath(K rowKey, Path path, TypeReference<T> type) {
        validateArgs(rowKey, path);

        String start = path.toString();
        // use the maximum unicode code point to terminate the range
        String finish = getFinishString(start);

        // note: prepared statements should be cached and reused by the connection pooling component....

        Object[] args = {rowKey,start,finish};
        ResultSet resultSet = session.execute(readPathQuery.bind(args));

        Map<Path,Object> pathMap = getPathMap(path, resultSet);

        if (pathMap.isEmpty()) {
            // not found
            return null;
        }

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
                            Object structuredValue,
                            BatchContext batchContext) {
        Batch batch = validateAndGetBatch(batchContext);

        validateArgs(rowKey, path);
        Object simplifiedStructure = writeMapper.convertValue(structuredValue, Object.class);
        Map<Path,Object> pathMap = Collections.singletonMap(path, simplifiedStructure);
        Map<Path,Object> objectMap = Decomposer.get().decompose(pathMap);

        batch = batchContext == null ? batch() : batch;
        List<Object> bindArguments = batchContext == null ?
                                        new ArrayList<Object>() :
                                        ((CqlBatchContext)batchContext).getBindArguments();
        Statement insertStatement = insertInto(tableName)
                .value(partitionKeyColumnName, bindMarker())
                .value(pathColumnName, bindMarker())
                .value(valueColumnName, bindMarker())
                .using(timestamp(getCurrentMicros()));
        insertStatement.setConsistencyLevel(defaultConsistencyLevel);


        for (Map.Entry<Path,Object> entry : objectMap.entrySet()) {
            batch.add(insertStatement);

            String stringValue = StructureConverter.get().toString(entry.getValue());

            bindArguments.add(rowKey);
            bindArguments.add(entry.getKey().toString());
            bindArguments.add(stringValue);
        }

        if (batchContext == null) {
            Query boundStatement = session.prepare(batch.getQueryString()).bind(bindArguments.toArray());
            boundStatement.setConsistencyLevel(defaultConsistencyLevel);
            session.execute(boundStatement);
        }
    }

    @Override
    public void deletePath(K rowKey, Path path) {
        deletePath(rowKey, path, null);
    }

    @Override
    public void deletePath(K rowKey, Path path, BatchContext batchContext) {
        Batch batch = validateAndGetBatch(batchContext);

        validateArgs(rowKey, path);

        // converting from a string and back normalizes the path, e.g. makes sure ends with the delimiter character
        String start = path.toString();
        String finish = getFinishString(start);

        // would like to just do a delete with a where clause, but unfortunately Cassandra can't do that in CQL (either)
        // with >= and <=

        // Since the path column is in the primary key, we need to just delete whole rows.

        Object[] args = {rowKey,start,finish};
        ResultSet resultSet = session.execute(readForDeleteQuery.bind(args));
        if (resultSet.isExhausted()) {
            // not found
            return;
        }

        Delete deleteStatement = delete().from(tableName);
        deleteStatement
                .using(timestamp(getCurrentMicros()))
                .where(eq(partitionKeyColumnName, rowKey))
                .and(eq(pathColumnName, bindMarker()));

        batch = batchContext == null ? batch() : batch;
        List<Object> bindArguments = batchContext == null ?
                new ArrayList<Object>() :
                ((CqlBatchContext)batchContext).getBindArguments();

        for (Row row : resultSet) {
            String pathToDelete = row.getString(0);
            batch.add(deleteStatement);
            bindArguments.add(pathToDelete);
        }

        if (batchContext == null) {
            BoundStatement query = session.prepare(batch.getQueryString()).bind(bindArguments.toArray());
            query.setConsistencyLevel(defaultConsistencyLevel);
            session.execute(query);
        }
    }

    @Override
    public Path createPath(String... elements) {
        return DefaultPath.fromStrings(elements);
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


    private void validateArgs(K rowKey, Path path) {
        Validate.isTrue(!path.isEmpty(), "Path must not be empty");
        Validate.notNull(rowKey, "Row key must not be empty");
    }

    private Batch validateAndGetBatch(BatchContext batchContext) {
        if (batchContext == null) {
            return null;
        }
        if (!(batchContext instanceof CqlBatchContext)) {
            throw new IllegalArgumentException("batchContext is not a CQL batch context");
        }
        return ((CqlBatchContext)batchContext).getBatch();
    }

    private Map<Path,Object> getPathMap(Path inputPath, Iterable<Row> resultSet) {
        Map<Path,Object> pathMap = new HashMap<Path,Object>();

        for (Row row : resultSet) {
            String valueString = row.getString(valueColumnName);
            Path path = DefaultPath.fromEncodedPathString(row.getString(pathColumnName));

            if (!path.startsWith(inputPath)) {
                throw new IllegalStateException("unexpected path found in database:" + path);
            }
            path = path.tail(inputPath.size());
            Object value = StructureConverter.get().fromString(valueString);
            // this can be a null converted from a JSON null
            pathMap.put(path, value);
        }
        return pathMap;
    }

    /**
     * Get current value of a pseudo-microsecond clock based on
     * System.currentTimeMillis(). The value of resolving conflicts on two threads calling this during
     * the same millisecond is dubious, but could have value on systems where the system clock has limited resolution.
     *
     * @return the difference, measured in microseconds, between the current time and midnight, January 1, 1970 UTC.
     */
    public static long getCurrentMicros() {
        Optional<Long> currentMicros;
        do {
            currentMicros = tryGetCurrentMicros();
        } while (!currentMicros.isPresent());
        return currentMicros.get();
    }

    private static Optional<Long> tryGetCurrentMicros() {
        long nowMicros = TimeUnit.MICROSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        long lastMicros = lastTime.get();
        boolean success = true;
        if (nowMicros > lastMicros) {
            success = lastTime.compareAndSet(lastMicros, nowMicros);
        } else {
            // add a pseudo-microsecond to whatever current lastTime is.
            // Note that if in the unlikely event that we have incremented the counter
            // past the actual system clock, then use an incremented lastTime instead of the system clock.
            // The implication of this is that if we have over a thousand requests on this
            // method within the same millisecond, then the timestamp we use can get out of sync
            // with other client VMs. This is deemed highly unlikely.
            nowMicros = lastTime.incrementAndGet();
        }

        return success ? Optional.of(nowMicros) : Optional.<Long>absent();
    }

    private static class CqlBatchContext implements BatchContext {
        private Batch batch = batch();
        private final List<Object> bindArguments = new LinkedList<Object>();

        private Batch getBatch() {
            return batch;
        }

        private List<Object> getBindArguments() {
            return bindArguments;
        }

        private void reset() {
            batch = batch();
            bindArguments.clear();
        }
    }
}
