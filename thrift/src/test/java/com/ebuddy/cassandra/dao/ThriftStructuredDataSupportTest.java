package com.ebuddy.cassandra.dao;

import static org.apache.commons.lang3.ObjectUtils.NULL;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ebuddy.cassandra.TypeReference;

/**
 * Unit tests for ThriftStructuredDataSupport.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class ThriftStructuredDataSupportTest {

    @Mock
    private ColumnFamilyOperations<String,String,Object> operations;

    private ThriftStructuredDataSupport<String> dao ;
    private final String columnFamily = "columnfamily";
    private final String rowKey = "rowKey";
    private final String pathString = "a/b/c";
    private final TypeReference<TestPojo> typeReference = new TypeReference<TestPojo>() { };

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        dao = new ThriftStructuredDataSupport<String>(operations);
    }

    @Test(groups = {"unit"})
    public void shouldReadFromPath() throws Exception {

        Map<String,Object> stringObjectMap = getExpectedMap(false);

        when(operations.readColumnsAsMap(columnFamily, rowKey,
                                         "a/b/c/",
                                         "a/b/c/" + Character.MAX_VALUE,
                                         Integer.MAX_VALUE,
                                         false)).thenReturn(stringObjectMap);

        //////////////////////
        TestPojo result = dao.readFromPath(columnFamily, rowKey, pathString, typeReference);
        //////////////////////

        TestPojo expectedResult = new TestPojo("v1", 42L, true, Arrays.asList("e1", "e2"));
        assertEquals(result, expectedResult);
    }

    @Test(groups = {"unit"})
    public void shouldReadFromPathNotFound() throws Exception {

        when(operations.readColumnsAsMap(columnFamily, rowKey,
                                         "a/b/c/",
                                         "a/b/c/" + Character.MAX_VALUE,
                                         Integer.MAX_VALUE,
                                         false)).thenReturn(Collections.<String,Object>emptyMap());

        //////////////////////
        TestPojo result = dao.readFromPath(columnFamily, rowKey, pathString, typeReference);
        //////////////////////

        assertNull(result);
    }


    @Test(groups = {"unit"})
    public void shouldWriteToPath() throws Exception {
        TestPojo testObject = new TestPojo("v1", 42L, true, Arrays.asList("e1", "e2"));

        //////////////////////
        dao.writeToPath(columnFamily, rowKey, pathString, testObject);
        //////////////////////

        Map<String,Object> stringObjectMap = getExpectedMap(true);

        verify(operations).writeColumns(columnFamily, rowKey, stringObjectMap);
    }

    @Test(groups = {"unit"})
    public void shouldDeletePath() throws Exception {

        //////////////////////
        dao.deletePath(columnFamily, rowKey, pathString);
        //////////////////////

        verify(operations).deleteColumns(columnFamily, rowKey, pathString + "/", pathString +"/" +Character.MAX_VALUE);
    }

    private Map<String,Object> getExpectedMap(boolean useNullToken) {
        Map<String,Object> stringObjectMap = new HashMap<String,Object>();
        stringObjectMap.put("a/b/c/s/", "v1");
        stringObjectMap.put("a/b/c/n/", 42L);
        stringObjectMap.put("a/b/c/b/", true);
        stringObjectMap.put("a/b/c/list/@0/", "e1");
        stringObjectMap.put("a/b/c/list/@1/", "e2");
        stringObjectMap.put("a/b/c/nullTest/", useNullToken ? NULL : null);
        return stringObjectMap;
    }
}
