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

import static com.ebuddy.cassandra.dao.AbstractColumnFamilyTemplate.ALL;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ObjectUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ebuddy.cassandra.dao.visitor.ColumnVisitor;

import me.prettyprint.cassandra.model.ExecutingKeyspace;
import me.prettyprint.cassandra.model.ExecutionResult;
import me.prettyprint.cassandra.model.KeyspaceOperationCallback;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.exceptions.HectorTransportException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * Test for SuperColumnFamilyTemplate.
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class SuperColumnFamilyTemplateTest {
    private final String columnFamily = "TestColumnFamily";
    private final String rowKey = "testKey";
    private final String superColumnName = "testSuperColumnName";
    private final String columnName = "testColumnName";
    private final List<String> columnNames = Arrays.asList("columnName1", "columnName2");
    private final String columnValue = "testColumnValue";
    private final List<String> columnValues = Arrays.asList("columnValue1", "columnValue2");
    private final List<String> superColumnNames = Arrays.asList("superColumnName1", "superColumnName2");
    private final List<String> rowKeys = Arrays.asList("rowKey1","rowKey2");
    @Mock
    private ExecutionResult executionResult;
    @Mock
    private KeyspaceTemplate.HectorBatchContext txnContext;
    @Mock
    private Mutator<String> mutator;
    private SuperColumnFamilyOperations<String,String,String,String> superColumnFamilyTestDao;
    
    @Mock
    ColumnVisitor<String, String> columnVisitor;

    @BeforeMethod(alwaysRun = true)
    private void setUp() {
        MockitoAnnotations.initMocks(this);
        ExecutingKeyspace keyspace = mock(ExecutingKeyspace.class);
        when(keyspace.doExecute(any(KeyspaceOperationCallback.class))).thenReturn(executionResult);
        superColumnFamilyTestDao = new SuperColumnFamilyTemplate<String,String,String,String>(keyspace,
                                                                                                           columnFamily,
                                                                                                           StringSerializer.get(),
                                                                                                           StringSerializer.get(),
                                                                                                           StringSerializer.get(),
                                                                                                           StringSerializer.get());
        when(txnContext.getMutator()).thenReturn(mutator);
    }
    
    @Test(groups = {"unit"})
    public void testReadColumnValue() {
        ColumnSlice columnSlice = mock(ColumnSlice.class);
        HColumn column = mock(HColumn.class);
        String columnValue = "testColumnValue";
        when(column.getValue()).thenReturn(columnValue);
        when(columnSlice.getColumns()).thenReturn(Collections.singletonList(column));
        when(executionResult.get()).thenReturn(columnSlice);

        //=========================
        String value = superColumnFamilyTestDao.readColumnValue(rowKey, superColumnName, columnName);
        //=========================
        assertEquals(value, columnValue);
    }

    @Test(groups={"unit"}, expectedExceptions = HectorTransportException.class)
    public void testReadColumnValueAndTranslateHectorException() {
        when(executionResult.get()).thenThrow(new HectorTransportException("test hector exception"));
        superColumnFamilyTestDao.readColumnValue(rowKey, superColumnName, columnName);
    }

    @Test(groups = {"unit"})
    public void testReadColumnsAsMapSpecifyingNoColumnName() {
        Map<String, String> testResultMap = new HashMap<String,String>();
        testResultMap.put("testPropKey1", "testPropValue1");
        testResultMap.put("testPropKey2", "testPropValue2");

        ColumnSlice columnSlice = mock(ColumnSlice.class);
        HColumn column1 = mock(HColumn.class);
        HColumn column2 = mock(HColumn.class);

        setupHColumn(column1, "testPropKey1", "testPropValue1");
        setupHColumn(column2, "testPropKey2" , "testPropValue2");

        when(columnSlice.getColumns()).thenReturn(Arrays.asList(column1, column2));
        when(executionResult.get()).thenReturn(columnSlice);

        //=========================
        Map actualResult = superColumnFamilyTestDao.readColumnsAsMap(rowKey, superColumnName);
        //=========================

        assertEquals(actualResult, testResultMap);
    }

    @Test(groups={"unit"}, expectedExceptions = HectorTransportException.class)
    public void testReadColumnsAsMapAndTranslateHectorException() {
        when(executionResult.get()).thenThrow(new HectorTransportException("test hector exception"));
        superColumnFamilyTestDao.readColumnsAsMap(rowKey, superColumnName);
    }

    @Test(groups={"unit"})
    public void testMultiGetColumnsAsMapSpecifyingNoColumnName() {
        Map<String,Map<String,String>> expectedResult =  new HashMap<String,Map<String,String>>();

        Map<String,String> properties = new HashMap<String, String>();
        Iterator<String> itr = columnValues.iterator();
        for (String columnName : columnNames) {
            properties.put(columnName, itr.next());
        }

        for(String key : rowKeys) {
            expectedResult.put(key, properties);
        }

        Rows<String,String,String> rows  = mock(Rows.class);
        when(executionResult.get()).thenReturn(rows);

        ColumnSlice<String,String> columnSlice = mock(ColumnSlice.class);
        Row<String,String,String> mockRow = mock(Row.class);
        // hard codes number of row keys to be 2
        when(mockRow.getKey()).thenReturn(rowKeys.get(0)).thenReturn(rowKeys.get(1));
        when(mockRow.getColumnSlice()).thenReturn(columnSlice);
        HColumn<String,String> mockColumn = mock(HColumn.class);

        when(mockColumn.getName()).thenReturn(columnNames.get(0)).thenReturn(columnNames.get(1)).
                thenReturn(columnNames.get(0)).thenReturn(columnNames.get(1));
        when(mockColumn.getValue()).
                thenReturn(columnValues.get(0)).
                thenReturn(columnValues.get(1)).
                thenReturn(columnValues.get(0)).
                thenReturn(columnValues.get(1));

        List<HColumn<String,String>> columnList = Arrays.asList(mockColumn,mockColumn);
        when(columnSlice.getColumns()).thenReturn(columnList);

        Iterator<Row<String,String,String>> mockRowIterator = mock(Iterator.class);
        when(rows.iterator()).thenReturn(mockRowIterator);
        when(mockRowIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(mockRowIterator.next()).thenReturn(mockRow).thenReturn(mockRow);

        //=========================
        Map<String,Map<String,String>> result =
                superColumnFamilyTestDao.multiGetColumnsAsMap(rowKeys, superColumnName);
        //=========================

        assertEquals(result, expectedResult);
    }

    @Test(groups={"unit"}, expectedExceptions = HectorTransportException.class)
    public void testMultiGetColumnsAsMapTranslateHectorException() {
        when(executionResult.get()).thenThrow(new HectorTransportException("test hector exception"));
        superColumnFamilyTestDao.multiGetColumnsAsMap(rowKeys, superColumnName);
    }

    @Test(groups={"unit"})
    public void testReadRowAsMap() {
        Map<String,Map<String,String>> expectedResult = new HashMap<String,Map<String,String>>();
        for (String superColumnName : superColumnNames) {
            Map<String,String> properties = new HashMap<String,String>();
            Iterator<String> itr = columnValues.iterator();
            for (String columnName : columnNames) {
                properties.put(columnName, itr.next());
            }
            expectedResult.put(superColumnName, properties);
        }

        SuperSlice superSlice = mock(SuperSlice.class);
        when(executionResult.get()).thenReturn(superSlice);

        HSuperColumn superColumn = mock(HSuperColumn.class);
        when(superSlice.getSuperColumns()).thenReturn(Arrays.asList(superColumn, superColumn));
        when(superColumn.getName()).thenReturn(superColumnNames.get(0)).thenReturn(superColumnNames.get(1));
        HColumn column = mock(HColumn.class);
        when(superColumn.getColumns()).thenReturn(Arrays.asList(column,column));
        when(column.getName()).thenReturn(columnNames.get(0)).
                thenReturn(columnNames.get(1)).
                thenReturn(columnNames.get(0)).
                thenReturn(columnNames.get(1));
        when(column.getValue()).
                thenReturn(columnValues.get(0)).
                thenReturn(columnValues.get(1)).
                thenReturn(columnValues.get(0)).
                thenReturn(columnValues.get(1));

        //=========================
        Map<String,Map<String,String>> result = superColumnFamilyTestDao.readRowAsMap(rowKey);
        //=========================

        assertEquals(result, expectedResult);
    }

    @Test(groups={"unit"}, expectedExceptions = HectorTransportException.class)
    public void testReadRowAsMapTranslateHectorException() {
        when(executionResult.get()).thenThrow(new HectorTransportException("test hector exception"));
        superColumnFamilyTestDao.readRowAsMap(rowKey);
    }

    @Test(groups={"unit"})
    public void testWriteColumns() {
        Map<String,String> properties = new HashMap<String,String>();
        Iterator<String> itr = columnValues.iterator();
        for (String columnName : columnNames) {
            properties.put(columnName, itr.next());
        }

        //=========================
        superColumnFamilyTestDao.writeColumns(rowKey, superColumnName, properties, txnContext);
        //=========================

        List<HColumn<String,String>> columns = new ArrayList<HColumn<String,String>>();
        Iterator<String> itr2 = columnValues.iterator();
        for (String columnName : columnNames) {
            columns.add(HFactory.createColumn(columnName,
                                              itr2.next(),
                                              StringSerializer.get(),
                                              StringSerializer.get()));
        }
        HSuperColumn<String,String,String> superColumn = HFactory.createSuperColumn(superColumnName,
                                                                                              columns,
                                                                                              StringSerializer.get(),
                                                                                              StringSerializer.get(),
                                                                                              StringSerializer.get());
        ArgumentCaptor<HSuperColumn> superColumnCaptor =  ArgumentCaptor.forClass(HSuperColumn.class);
        verify(mutator).addInsertion(eq(rowKey), eq(columnFamily), superColumnCaptor.capture());
        HSuperColumn actualSuperColumn = superColumnCaptor.getValue();
        assertTrue(areSuperColumnsEqual(actualSuperColumn, superColumn));
    }

    @Test(groups={"unit"}, expectedExceptions = HectorTransportException.class)
    public void testWriteColumnsTranslateHectorException() {
        when(mutator.addInsertion(eq(rowKey),
                                  eq(columnFamily),
                                  any(HSuperColumn.class))).thenThrow(new HectorTransportException(
                "test hector exception"));

        Map<String,String> properties = new HashMap<String,String>();
        Iterator<String> itr = columnValues.iterator();
        for (String columnName : columnNames) {
            properties.put(columnName, itr.next());
        }

        //=========================
        superColumnFamilyTestDao.writeColumns(rowKey, superColumnName, properties, txnContext);
        //=========================
    }


    @Test(groups={"unit"})
    public void testWriteColumn() {
        String propertyValue = columnValue;

        //=========================
        superColumnFamilyTestDao.writeColumn(rowKey, superColumnName, columnName, propertyValue, txnContext);
        //=========================

        HColumn<String,String> column = HFactory.createColumn(columnName,
                                                              columnValue,
                                                              StringSerializer.get(),
                                                              StringSerializer.get());

        HSuperColumn<String,String,String> superColumn = HFactory.createSuperColumn(superColumnName,
                                                                                              Arrays.asList(column),
                                                                                              StringSerializer.get(),
                                                                                              StringSerializer.get(),
                                                                                              StringSerializer.get());
        ArgumentCaptor<HSuperColumn> superColumnCaptor = ArgumentCaptor.forClass(HSuperColumn.class);
        verify(mutator).addInsertion(eq(rowKey), eq(columnFamily), superColumnCaptor.capture());
        HSuperColumn actualSuperColumn = superColumnCaptor.getValue();
        assertTrue(areSuperColumnsEqual(actualSuperColumn, superColumn));
    }


    @Test(groups={"unit"}, expectedExceptions = HectorTransportException.class)
    public void testWriteColumnTranslateHectorException() {

        when(mutator.addInsertion(eq(rowKey),
                                  eq(columnFamily),
                                  any(HSuperColumn.class))).
                thenThrow(new HectorTransportException("test hector exception"));

        String propertyValue = columnValue;

        //=========================
        superColumnFamilyTestDao.writeColumn(rowKey, superColumnName, columnName, propertyValue, txnContext);
        //=========================
    }

    @Test(groups={"unit"})
    public void testDeleteColumns() {
        //=========================
        superColumnFamilyTestDao.deleteColumns(rowKey, superColumnName, columnNames, txnContext);
        //=========================


        for (String colName : columnNames) {
            verify(mutator).addSubDelete(rowKey,
                                         columnFamily,
                                         superColumnName,
                                         colName,
                                         StringSerializer.get(),
                                         StringSerializer.get());
        }
    }

    @Test(groups={"unit"}, expectedExceptions = HectorTransportException.class)
    public void testDeleteColumnsTranslateHectorException() {
        when(mutator.addSubDelete(anyString(),
                                  anyString(),
                                  anyString(),
                                  anyString(),
                                  any(Serializer.class),
                                  any(Serializer.class))).
                thenThrow(new HectorTransportException("test hector exception"));

        //=========================
        superColumnFamilyTestDao.deleteColumns(rowKey, superColumnName, columnNames, txnContext);
        //=========================
    }
    
    
    @Test(groups = {"unit"})
    public void testVisitColumn() {
        Map<String, String> testResultMap = new HashMap<String,String>();
        testResultMap.put("testPropKey1", "testPropValue1");
        testResultMap.put("testPropKey2", "testPropValue2");

        ColumnSlice columnSlice = mock(ColumnSlice.class);
        HColumn column1 = mock(HColumn.class);
        HColumn column2 = mock(HColumn.class);

        String propertyValue1 = setupHColumn(column1, "testPropKey1", "testPropValue1");
        String propertyValue2 = setupHColumn(column2, "testPropKey2", "testPropValue1");
        
        when(columnSlice.getColumns()).thenReturn(Arrays.asList(column1, column2));
        when(executionResult.get()).thenReturn(columnSlice);
        
        //=========================
        //Map actualResult = superColumnFamilyTestDao.readColumnsAsMap(rowKey, superColumnName);
        superColumnFamilyTestDao.visitColumns(rowKey, superColumnName, null, null, ALL, false, columnVisitor);
        
        //=========================

        verify(columnVisitor).visit(eq("testPropKey1"), eq(propertyValue1), any(Long.class), any(Integer.class));
        verify(columnVisitor).visit(eq("testPropKey2"), eq(propertyValue2), any(Long.class), any(Integer.class));
    }

    private String setupHColumn(HColumn column1, String columnKey, String columndValue) {
        when(column1.getName()).thenReturn(columnKey);
        String propertyValue = columndValue;
        when(column1.getValue()).thenReturn(propertyValue);
        return propertyValue;
    }

    @SuppressWarnings({"ControlFlowStatementWithoutBraces"})
    private boolean areSuperColumnsEqual(HSuperColumn superColumn1, HSuperColumn superColumn2) {
        if (superColumn1 == superColumn2) return true;
        if (superColumn2 == null) return false;
        if (superColumn1 == null) return false;
        if (superColumn1.getClass() != superColumn2.getClass()) return false;
        if (!ObjectUtils.equals(superColumn1.getName(), superColumn2.getName())) return false;
        if (superColumn1.getColumns().size() != superColumn2.getColumns().size()) return false;
        Iterator<HColumn> itr1 = superColumn1.getColumns().iterator();
        Iterator<HColumn> itr2 = superColumn2.getColumns().iterator();
        while(itr1.hasNext()) {
            if (!areColumnsEqual(itr1.next(), itr2.next())) return false;
        }
        return true;
    }

    @SuppressWarnings({"ControlFlowStatementWithoutBraces", "SimplifiableIfStatement"})
    private boolean areColumnsEqual(HColumn column1, HColumn column2) {
        if (column1 == column2) return true;
        if (column2 == null) return false;
        if (column1 == null) return false;
        if (column1.getClass() != column2.getClass()) return false;
        if (!ObjectUtils.equals(column1.getName(), column2.getName())) return false;
        return ObjectUtils.equals(column1.getValue(), column2.getValue());
    }
}
