package com.ebuddy.cassandra.dao;

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

import org.apache.commons.lang.ObjectUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.dao.DataAccessResourceFailureException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ebuddy.cassandra.property.PropertyValue;
import com.ebuddy.cassandra.property.PropertyValueFactory;
import com.ebuddy.cassandra.property.PropertyValueSerializer;

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
    private final PropertyValueFactory valueFactory = PropertyValueFactory.get();
    @Mock
    private ExecutionResult executionResult;
    @Mock
    private AbstractColumnFamilyTemplate<String,String,String>.HectorTransactionContext txnContext;
    @Mock
    private Mutator<String> mutator;
    private SuperColumnFamilyOperations<String,String,String,PropertyValue<?>> superColumnFamilyTestDao;

    @BeforeMethod(alwaysRun = true)
    private void setUp() {
        MockitoAnnotations.initMocks(this);
        ExecutingKeyspace keyspace = mock(ExecutingKeyspace.class);
        when(keyspace.doExecute(any(KeyspaceOperationCallback.class))).thenReturn(executionResult);
        superColumnFamilyTestDao = new SuperColumnFamilyTemplate<String,String,String,PropertyValue<?>>(keyspace,
                                                                                                           columnFamily,
                                                                                                           StringSerializer.get(),
                                                                                                           StringSerializer.get(),
                                                                                                           StringSerializer.get(),
                                                                                                           PropertyValueSerializer.get());
        when(txnContext.getMutator()).thenReturn(mutator);
    }
    
    @Test(groups = {"unit"})
    public void testReadColumnValue() {
        ColumnSlice columnSlice = mock(ColumnSlice.class);
        HColumn column = mock(HColumn.class);
        String columnValue = "testColumnValue";
        when(column.getValue()).thenReturn(PropertyValueFactory.get().createPropertyValue(columnValue));
        when(columnSlice.getColumns()).thenReturn(Collections.singletonList(column));
        when(executionResult.get()).thenReturn(columnSlice);

        //=========================
        PropertyValue<?> value = superColumnFamilyTestDao.readColumnValue(rowKey, superColumnName, columnName);
        //=========================
        assertEquals(value.getValue(), columnValue);
    }

    @Test(groups={"unit"}, expectedExceptions = DataAccessResourceFailureException.class)
    public void testReadColumnValueAndTranslateHectorException() {
        when(executionResult.get()).thenThrow(new HectorTransportException("test hector exception"));
        superColumnFamilyTestDao.readColumnValue(rowKey, superColumnName, columnName);
    }

    @Test(groups = {"unit"})
    public void testReadColumnsAsMapSpecifyingNoColumnName() {
        Map<String, PropertyValue<?>> testResultMap = new HashMap<String,PropertyValue<?>>();
        PropertyValueFactory valueFactory = PropertyValueFactory.get();
        testResultMap.put("testPropKey1", valueFactory.createPropertyValue("testPropValue1"));
        testResultMap.put("testPropKey2", valueFactory.createPropertyValue("testPropValue2"));

        ColumnSlice columnSlice = mock(ColumnSlice.class);
        HColumn column1 = mock(HColumn.class);
        HColumn column2 = mock(HColumn.class);

        when(column1.getName()).thenReturn("testPropKey1");
        when(column1.getValue()).thenReturn(PropertyValueFactory.get().createPropertyValue("testPropValue1"));
        
        when(column2.getName()).thenReturn("testPropKey2");
        when(column2.getValue()).thenReturn(PropertyValueFactory.get().createPropertyValue("testPropValue2"));
        
        when(columnSlice.getColumns()).thenReturn(Arrays.asList(column1, column2));
        when(executionResult.get()).thenReturn(columnSlice);

        //=========================
        Map actualResult = superColumnFamilyTestDao.readColumnsAsMap(rowKey, superColumnName);
        //=========================

        assertEquals(actualResult, testResultMap);
    }

    @Test(groups={"unit"}, expectedExceptions = DataAccessResourceFailureException.class)
    public void testReadColumnsAsMapAndTranslateHectorException() {
        when(executionResult.get()).thenThrow(new HectorTransportException("test hector exception"));
        superColumnFamilyTestDao.readColumnsAsMap(rowKey, superColumnName);
    }

    @Test(groups={"unit"})
    public void testMultiGetColumnsAsMapSpecifyingNoColumnName() {
        Map<String,Map<String,PropertyValue<?>>> expectedResult =  new HashMap<String,Map<String,PropertyValue<?>>>();

        Map<String,PropertyValue<?>> properties = new HashMap<String, PropertyValue<?>>();
        Iterator<String> itr = columnValues.iterator();
        for (String columnName : columnNames) {
            properties.put(columnName, valueFactory.createPropertyValue(itr.next()));
        }

        for(String key : rowKeys) {
            expectedResult.put(key, properties);
        }

        Rows<String,String,PropertyValue<String>> rows  = mock(Rows.class);
        when(executionResult.get()).thenReturn(rows);

        ColumnSlice<String,PropertyValue<String>> columnSlice = mock(ColumnSlice.class);
        Row<String,String,PropertyValue<String>> mockRow = mock(Row.class);
        // hard codes number of row keys to be 2
        when(mockRow.getKey()).thenReturn(rowKeys.get(0)).thenReturn(rowKeys.get(1));
        when(mockRow.getColumnSlice()).thenReturn(columnSlice);
        HColumn<String,PropertyValue<String>> mockColumn = mock(HColumn.class);

        when(mockColumn.getName()).thenReturn(columnNames.get(0)).thenReturn(columnNames.get(1)).
                thenReturn(columnNames.get(0)).thenReturn(columnNames.get(1));
        when(mockColumn.getValue()).
                thenReturn(valueFactory.createPropertyValue(columnValues.get(0))).
                thenReturn(valueFactory.createPropertyValue(columnValues.get(1))).
                thenReturn(valueFactory.createPropertyValue(columnValues.get(0))).
                thenReturn(valueFactory.createPropertyValue(columnValues.get(1)));

        List<HColumn<String,PropertyValue<String>>> columnList = Arrays.asList(mockColumn,mockColumn);
        when(columnSlice.getColumns()).thenReturn(columnList);

        Iterator<Row<String,String,PropertyValue<String>>> mockRowIterator = mock(Iterator.class);
        when(rows.iterator()).thenReturn(mockRowIterator);
        when(mockRowIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(mockRowIterator.next()).thenReturn(mockRow).thenReturn(mockRow);

        //=========================
        Map<String,Map<String,PropertyValue<?>>> result =
                superColumnFamilyTestDao.multiGetColumnsAsMap(rowKeys, superColumnName);
        //=========================

        assertEquals(result, expectedResult);
    }

    @Test(groups={"unit"}, expectedExceptions = DataAccessResourceFailureException.class)
    public void testMultiGetColumnsAsMapTranslateHectorException() {
        when(executionResult.get()).thenThrow(new HectorTransportException("test hector exception"));
        superColumnFamilyTestDao.multiGetColumnsAsMap(rowKeys, superColumnName);
    }

    @Test(groups={"unit"})
    public void testReadRowAsMap() {
        Map<String,Map<String,PropertyValue<?>>> expectedResult = new HashMap<String,Map<String,PropertyValue<?>>>();
        for (String superColumnName : superColumnNames) {
            Map<String,PropertyValue<?>> properties = new HashMap<String,PropertyValue<?>>();
            Iterator<String> itr = columnValues.iterator();
            for (String columnName : columnNames) {
                properties.put(columnName, PropertyValueFactory.get().createPropertyValue(itr.next()));
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
                thenReturn(PropertyValueFactory.get().createPropertyValue(columnValues.get(0))).
                thenReturn(PropertyValueFactory.get().createPropertyValue(columnValues.get(1))).
                thenReturn(PropertyValueFactory.get().createPropertyValue(columnValues.get(0))).
                thenReturn(PropertyValueFactory.get().createPropertyValue(columnValues.get(1)));

        //=========================
        Map<String,Map<String,PropertyValue<?>>> result = superColumnFamilyTestDao.readRowAsMap(rowKey);
        //=========================

        assertEquals(result, expectedResult);
    }

    @Test(groups={"unit"}, expectedExceptions = DataAccessResourceFailureException.class)
    public void testReadRowAsMapTranslateHectorException() {
        when(executionResult.get()).thenThrow(new HectorTransportException("test hector exception"));
        superColumnFamilyTestDao.readRowAsMap(rowKey);
    }

    @Test(groups={"unit"})
    public void testWriteColumns() {
        Map<String,PropertyValue<?>> properties = new HashMap<String,PropertyValue<?>>();
        Iterator<String> itr = columnValues.iterator();
        for (String columnName : columnNames) {
            properties.put(columnName, PropertyValueFactory.get().createPropertyValue(itr.next()));
        }

        //=========================
        superColumnFamilyTestDao.writeColumns(rowKey, superColumnName, properties, txnContext);
        //=========================

        List<HColumn<String,PropertyValue<?>>> columns = new ArrayList<HColumn<String,PropertyValue<?>>>();
        Iterator<String> itr2 = columnValues.iterator();
        for (String columnName : columnNames) {
            columns.add(HFactory.createColumn(columnName,
                                              PropertyValueFactory.get().createPropertyValue(itr2.next()),
                                              StringSerializer.get(),
                                              PropertyValueSerializer.get()));
        }
        HSuperColumn<String,String,PropertyValue<?>> superColumn = HFactory.createSuperColumn(superColumnName,
                                                                                              columns,
                                                                                              StringSerializer.get(),
                                                                                              StringSerializer.get(),
                                                                                              PropertyValueSerializer.get());
        ArgumentCaptor<HSuperColumn> superColumnCaptor =  ArgumentCaptor.forClass(HSuperColumn.class);
        verify(mutator).addInsertion(eq(rowKey), eq(columnFamily), superColumnCaptor.capture());
        HSuperColumn actualSuperColumn = superColumnCaptor.getValue();
        assertTrue(areSuperColumnsEqual(actualSuperColumn, superColumn));
    }

    @Test(groups={"unit"}, expectedExceptions = DataAccessResourceFailureException.class)
    public void testWriteColumnsTranslateHectorException() {
        when(mutator.addInsertion(eq(rowKey),
                                  eq(columnFamily),
                                  any(HSuperColumn.class))).thenThrow(new HectorTransportException(
                "test hector exception"));

        Map<String,PropertyValue<?>> properties = new HashMap<String,PropertyValue<?>>();
        Iterator<String> itr = columnValues.iterator();
        for (String columnName : columnNames) {
            properties.put(columnName, PropertyValueFactory.get().createPropertyValue(itr.next()));
        }

        //=========================
        superColumnFamilyTestDao.writeColumns(rowKey, superColumnName, properties, txnContext);
        //=========================
    }


    @Test(groups={"unit"})
    public void testWriteColumn() {
        PropertyValue<?> propertyValue = PropertyValueFactory.get().createPropertyValue(columnValue);

        //=========================
        superColumnFamilyTestDao.writeColumn(rowKey, superColumnName, columnName, propertyValue, txnContext);
        //=========================

        HColumn<String,PropertyValue<?>> column = HFactory.createColumn(columnName,
                                                                        PropertyValueFactory.get().createPropertyValue(
                                                                                columnValue),
                                                                        StringSerializer.get(),
                                                                        PropertyValueSerializer.get());

        HSuperColumn<String,String,PropertyValue<?>> superColumn = HFactory.createSuperColumn(superColumnName,
                                                                                              Arrays.asList(column),
                                                                                              StringSerializer.get(),
                                                                                              StringSerializer.get(),
                                                                                              PropertyValueSerializer.get());
        ArgumentCaptor<HSuperColumn> superColumnCaptor = ArgumentCaptor.forClass(HSuperColumn.class);
        verify(mutator).addInsertion(eq(rowKey), eq(columnFamily), superColumnCaptor.capture());
        HSuperColumn actualSuperColumn = superColumnCaptor.getValue();
        assertTrue(areSuperColumnsEqual(actualSuperColumn, superColumn));
    }


    @Test(groups={"unit"}, expectedExceptions = DataAccessResourceFailureException.class)
    public void testWriteColumnTranslateHectorException() {

        when(mutator.addInsertion(eq(rowKey),
                                  eq(columnFamily),
                                  any(HSuperColumn.class))).
                thenThrow(new HectorTransportException("test hector exception"));

        PropertyValue<?> propertyValue = PropertyValueFactory.get().createPropertyValue(columnValue);

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

    @Test(groups={"unit"}, expectedExceptions = DataAccessResourceFailureException.class)
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
