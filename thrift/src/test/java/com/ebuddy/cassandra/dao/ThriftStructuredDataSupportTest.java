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

import com.ebuddy.cassandra.Path;
import com.ebuddy.cassandra.TypeReference;

/**
 * Unit tests for ThriftStructuredDataSupport.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class ThriftStructuredDataSupportTest {
    private static final int MAX_CODE_POINT = 0x10FFFF;

    @Mock
    private ColumnFamilyOperations<String,String,Object> operations;

    private ThriftStructuredDataSupport<String> dao ;
    private final String rowKey = "rowKey";
    private final TypeReference<TestPojo> typeReference = new TypeReference<TestPojo>() { };
    private Path path;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        dao = new ThriftStructuredDataSupport<String>(operations);
        path = dao.createPath("a","b","c");
    }

    @Test(groups = {"unit"})
    public void shouldReadFromPath() throws Exception {

        Map<String,Object> stringObjectMap = getExpectedMap(false);

        when(operations.readColumnsAsMap(rowKey,
                                         "a/b/c/",
                                         getFinishString("a/b/c/"),
                                         Integer.MAX_VALUE,
                                         false)).thenReturn(stringObjectMap);

        //////////////////////
        TestPojo result = dao.readFromPath(rowKey, path, typeReference);
        //////////////////////

        TestPojo expectedResult = new TestPojo("v1", 42L, true, Arrays.asList("e1", "e2"));
        assertEquals(result, expectedResult);
    }

    @Test(groups = {"unit"})
    public void shouldReadFromPathNotFound() throws Exception {

        when(operations.readColumnsAsMap(rowKey,
                                         "a/b/c/",
                                         getFinishString("a/b/c/"),
                                         Integer.MAX_VALUE,
                                         false)).thenReturn(Collections.<String,Object>emptyMap());

        //////////////////////
        TestPojo result = dao.readFromPath(rowKey, path, typeReference);
        //////////////////////

        assertNull(result);
    }


    @Test(groups = {"unit"})
    public void shouldWriteToPath() throws Exception {
        TestPojo testObject = new TestPojo("v1", 42L, true, Arrays.asList("e1", "e2"));

        //////////////////////
        dao.writeToPath(rowKey, path, testObject);
        //////////////////////

        Map<String,Object> stringObjectMap = getExpectedMap(true);

        verify(operations).writeColumns(rowKey, stringObjectMap);
    }

    @Test(groups = {"unit"})
    public void shouldDeletePath() throws Exception {

        //////////////////////
        dao.deletePath(rowKey, path);
        //////////////////////

        verify(operations).deleteColumns(rowKey, "a/b/c/", getFinishString("a/b/c/"));
    }

    private Map<String,Object> getExpectedMap(boolean useNullToken) {
        Map<String,Object> stringObjectMap = new HashMap<String,Object>();
        stringObjectMap.put("a/b/c/s/", "v1");
        stringObjectMap.put("a/b/c/n/", 42L);
        stringObjectMap.put("a/b/c/b/", true);
        stringObjectMap.put("a/b/c/list/@0/", "e1");
        stringObjectMap.put("a/b/c/list/@1/", "e2");
        stringObjectMap.put("a/b/c/list/@2/", "\uFFFF\uFFFF");
        stringObjectMap.put("a/b/c/nullTest/", useNullToken ? NULL : null);
        return stringObjectMap;
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
}
