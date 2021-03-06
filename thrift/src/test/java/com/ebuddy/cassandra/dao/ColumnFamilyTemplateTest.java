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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.KeyValue;
import org.apache.commons.collections.keyvalue.DefaultKeyValue;
import org.apache.commons.lang3.ObjectUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ebuddy.cassandra.dao.mapper.ColumnMapper;

import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.cassandra.model.ExecutionResult;
import me.prettyprint.cassandra.model.KeyspaceOperationCallback;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.exceptions.HectorTransportException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * Test for ColumnFamilyDaoTemplate.
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class ColumnFamilyTemplateTest {
    private final String columnFamily = "TestColumnFamily";
    private final String rowKey = "testKey";
    private final String columnName = "testColumnName";
    private final List<String> columnNames = Arrays.asList("columnName1", "columnName2");
    private final String columnValue = "testColumnValue";
    private final List<String> columnValues = Arrays.asList("columnValue1", "columnValue2");
    @Mock
    private ExecutionResult executionResult;
    @Mock
    private KeyspaceTemplate.HectorBatchContext txnContext;
    @Mock
    private Mutator<String> mutator;

    @Captor
    private ArgumentCaptor<ColumnMapper<String,String,String>> mapperCaptor;

    private ColumnFamilyOperations<String,String,String> columnFamilyTestDao;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        ExecutingKeyspace keyspace = mock(ExecutingKeyspace.class);
        when(keyspace.doExecute(any(KeyspaceOperationCallback.class))).thenReturn(executionResult);
        columnFamilyTestDao = new ColumnFamilyTemplate<String,String,String>(keyspace,
                                                                             columnFamily,
                                                                             StringSerializer.get(),
                                                                             StringSerializer.get(),
                                                                             StringSerializer.get());
        when(txnContext.getMutator()).thenReturn(mutator);
    }

    @Test(groups = {"unit"})
    public void testReadColumnValue() throws Exception {
        HColumn<String,String> column = mock(HColumn.class);
        String columnValue = "testColumnValue";
        when(column.getValue()).thenReturn(columnValue);
        when(executionResult.get()).thenReturn(column);

        //=========================
        String value = columnFamilyTestDao.readColumnValue(rowKey, columnName);
        //=========================
        assertEquals(value, columnValue);
    }

    @Test(groups = {"unit"}, expectedExceptions = HectorTransportException.class)
    public void testReadColumnValueTranslateHectorException() throws Exception {
        when(executionResult.get()).thenThrow(new HectorTransportException("test hector exception"));
        columnFamilyTestDao.readColumnValue(rowKey, columnName);
    }

    @Test(groups = {"unit"})
    public void testReadColumnsAsMap() throws Exception {
        Map<String, String> testResultMap = new HashMap<String,String>();
        testResultMap.put(columnNames.get(0), columnValues.get(0));
        testResultMap.put(columnNames.get(1), columnValues.get(1));

        ColumnSlice columnSlice = mock(ColumnSlice.class);
        HColumn column1 = mock(HColumn.class);
        HColumn column2 = mock(HColumn.class);

        when(column1.getName()).thenReturn(columnNames.get(0));
        when(column1.getValue()).thenReturn(columnValues.get(0));
        when(column2.getName()).thenReturn(columnNames.get(1));
        when(column2.getValue()).thenReturn(columnValues.get(1));

        when(columnSlice.getColumns()).thenReturn(Arrays.asList(column1, column2));
        when(executionResult.get()).thenReturn(columnSlice);

        //=========================
        Map actualResult = columnFamilyTestDao.readColumnsAsMap(rowKey);
        //=========================
        assertEquals(actualResult, testResultMap);
    }

    @Test(groups = {"unit"})
    public void testReadColumns() throws Exception {
        Map<String, String> testResultMap = new HashMap<String,String>();
        testResultMap.put(columnNames.get(0), columnValues.get(0));
        testResultMap.put(columnNames.get(1), columnValues.get(1));

        ColumnSlice columnSlice = mock(ColumnSlice.class);
        HColumn column1 = mock(HColumn.class);
        HColumn column2 = mock(HColumn.class);

        when(column1.getName()).thenReturn(columnNames.get(0));
        when(column1.getValue()).thenReturn(columnValues.get(0));
        when(column2.getName()).thenReturn(columnNames.get(1));
        when(column2.getValue()).thenReturn(columnValues.get(1));

        when(columnSlice.getColumns()).thenReturn(Arrays.asList(column1, column2));
        when(executionResult.get()).thenReturn(columnSlice);

        //=========================
        List<KeyValue> actualResult = columnFamilyTestDao.readColumns(rowKey,
                                                                      new ColumnMapper<KeyValue,String,String>() {
            @Override
            public KeyValue mapColumn(String columnName, String columnValue) {
                return new DefaultKeyValue(columnName, columnValue);
            }
        });
        //=========================

        Map<String,String> resultMap = new HashMap<String,String>();
        for (KeyValue kv : actualResult) {
            resultMap.put((String)kv.getKey(), (String)kv.getValue());
        }

        assertEquals(resultMap, testResultMap);
    }

    @Test(groups = {"unit"})
    public void testBasicMultiGetAsMap() throws Exception {

        Rows<String,String,String> resultRows = mock(Rows.class);
        Row<String,String,String> row1 = mock(Row.class);
        Row<String,String,String> row2 = mock(Row.class);
        when(row1.getKey()).thenReturn("row1Key");
        when(row2.getKey()).thenReturn("row2Key");

        ColumnSlice columnSlice1 = mock(ColumnSlice.class);
        HColumn column1 = mock(HColumn.class);
        HColumn column2 = mock(HColumn.class);

        when(column1.getName()).thenReturn(columnNames.get(0));
        when(column1.getValue()).thenReturn(columnValues.get(0));
        when(column2.getName()).thenReturn(columnNames.get(1));
        when(column2.getValue()).thenReturn(columnValues.get(1));
        when(columnSlice1.getColumns()).thenReturn(Arrays.asList(column1, column2));

        when(row1.getColumnSlice()).thenReturn(columnSlice1);
        when(row2.getColumnSlice()).thenReturn(columnSlice1);
        Iterator<Row<String,String,String>> iterator = Arrays.asList(row1,row2).iterator();
        when(resultRows.iterator()).thenReturn(iterator);
        when(executionResult.get()).thenReturn(resultRows);

        Iterable<String> rowKeys = Arrays.asList("key1", "key2");

        Map<String,String> oneRowMap = new HashMap<String,String>();
        oneRowMap.put("columnName1", "columnValue1");
        oneRowMap.put("columnName2", "columnValue2");
        Map<String,Map<String,String>> expectedResultMap = new HashMap<String,Map<String,String>>();
        expectedResultMap.put("row1Key", oneRowMap);
        expectedResultMap.put("row2Key", oneRowMap);

        //=========================
        Map<String,Map<String,String>> resultMap = columnFamilyTestDao.multiGetAsMap(rowKeys);
        //=========================

        assertNotNull(resultMap);
        assertEquals(resultMap, expectedResultMap);
    }

    @Test(groups = {"unit"}, expectedExceptions = HectorTransportException.class)
    public void testReadColumnsAsMapTranslateHectorException() throws Exception {
        when(executionResult.get()).thenThrow(new HectorTransportException("test hector exception"));
        columnFamilyTestDao.readColumnsAsMap(rowKey);
    }

    @Test(groups = {"unit"})
    public void testWriteColumn() throws Exception {
        String propertyValue = columnValue;

        //=========================
        columnFamilyTestDao.writeColumn(rowKey, columnName, propertyValue, txnContext);
        //=========================

        HColumn<String,String> column = HFactory.createColumn(columnName,
                                                                        columnValue,
                                                                        StringSerializer.get(),
                                                                        StringSerializer.get());

        ArgumentCaptor<HColumn> columnCaptor = ArgumentCaptor.forClass(HColumn.class);
        verify(mutator).addInsertion(eq(rowKey), eq(columnFamily), columnCaptor.capture());
        HColumn actualColumn = columnCaptor.getValue();
        assertTrue(areColumnsEqual(actualColumn, column));
    }

    @Test(groups = {"unit"}, expectedExceptions = HectorTransportException.class)
    public void testWriteColumnTranslateHectorException() throws Exception {
        when(mutator.addInsertion(eq(rowKey), eq(columnFamily), any(HColumn.class))).
                thenThrow(new HectorTransportException("test hector exception"));

        String propertyValue = columnValue;

        //=========================
        columnFamilyTestDao.writeColumn(rowKey, columnName, propertyValue, txnContext);
        //=========================
    }

    @Test(groups = {"unit"})
    public void testWriteColumns() throws Exception {
        Map<String,String> properties = new HashMap<String,String>();
        Iterator<String> itr = columnValues.iterator();
        for (String columnName : columnNames) {
            properties.put(columnName, itr.next());
        }

        //=========================
        columnFamilyTestDao.writeColumns(rowKey, properties, txnContext);
        //=========================

        HColumn column1 = HFactory.createColumn(columnNames.get(0),
                                              columnValues.get(0),
                                              StringSerializer.get(),
                                              StringSerializer.get());
        HColumn column2 = HFactory.createColumn(columnNames.get(1),
                                                columnValues.get(1),
                                                StringSerializer.get(),
                                                StringSerializer.get());
        ArgumentCaptor<HColumn> columnCaptor =  ArgumentCaptor.forClass(HColumn.class);
        verify(mutator, times(2)).addInsertion(eq(rowKey), eq(columnFamily), columnCaptor.capture());
        List<HColumn> actualColumns = columnCaptor.getAllValues();
        assertTrue(areColumnsEqual(actualColumns.get(0), column1));
        assertTrue(areColumnsEqual(actualColumns.get(1), column2));
    }

    @Test(groups = {"unit"}, expectedExceptions = HectorTransportException.class)
    public void testWriteColumnsTranslateHectorException() throws Exception {
        when(mutator.addInsertion(eq(rowKey),
                                  eq(columnFamily),
                                  any(HColumn.class))).thenThrow(new HectorTransportException(
                "test hector exception"));

        Map<String,String> properties = new HashMap<String,String>();
        Iterator<String> itr = columnValues.iterator();
        for (String columnName : columnNames) {
            properties.put(columnName, itr.next());
        }

        //=========================
        columnFamilyTestDao.writeColumns(rowKey, properties, txnContext);
        //=========================
    }

    @Test(groups = {"unit"})
    public void shouldDeleteColumnSlice() throws Exception {
        ColumnFamilyOperations<String,String,String> spy = spy(columnFamilyTestDao);
        doReturn(Arrays.asList("a", "b", "c")).when(spy).readColumns(eq(rowKey), eq("start"), eq(
                "finish"), eq(Integer.MAX_VALUE), eq(false), mapperCaptor.capture());

        //=========================
        spy.deleteColumns(rowKey, "start", "finish", txnContext);
        //=========================

        verify(mutator).addDeletion(rowKey, columnFamily, "a", StringSerializer.get());
        verify(mutator).addDeletion(rowKey, columnFamily, "a", StringSerializer.get());
        verify(mutator).addDeletion(rowKey, columnFamily, "a", StringSerializer.get());

        verify(mutator, never()).execute();
    }

    private boolean areColumnsEqual(HColumn column1, HColumn column2) {
        return column1 == column2 ||
                column2 != null &&
                        column1 != null &&
                        column1.getClass() == column2.getClass() &&
                        ObjectUtils.equals(column1.getName(), column2.getName()) &&
                        ObjectUtils.equals(column1.getValue(), column2.getValue());
    }
}
