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

package com.ebuddy.cassandra.structure.jackson;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ebuddy.cassandra.databind.CustomTypeResolverBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Test for preserving sets in object conversion.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
@SuppressWarnings({
                          "CloneableClassWithoutClone",
                          "OverlyStrongTypeCast",
                          "unchecked",
                          "CollectionDeclaredAsConcreteClass"
                  })
public class CustomTypeResolverBuilderTest {

    private ObjectMapper mapper;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        mapper.setDefaultTyping(new CustomTypeResolverBuilder());
    }

    @Test(groups = "unit")
    public void convertValueShouldTypeListsWithSubclassedSet() throws Exception {
        TestPojoWithSet testObject = getTestPojoWithSubclassedSets();
        Object convertedObject = mapper.convertValue(testObject, Object.class);
        Map<String,?> map = (Map<String,?>)convertedObject;

        List<?> list = (List<Object>)map.get("list");
        assertEquals(list.size(), 2);
        assertEquals(list.get(0), "java.util.ArrayList");
        assertEquals((List<Object>)list.get(1), Arrays.asList("l2", "l1", "l3"));

        list = (List<?>)map.get("objectSet");
        assertEquals(list.size(), 2);
        assertEquals(list.get(0), "com.ebuddy.cassandra.structure.jackson.CustomTypeResolverBuilderTest$1<java.lang.Object>");
        list = (List<?>)list.get(1);
        assertEquals(list.size(), 2);
        assertTrue(list.contains(1));
        assertTrue(list.contains("X"));

        list = (List<Object>)map.get("hashSet");
        assertEquals(list.size(), 2);
        assertEquals(list.get(0), "com.ebuddy.cassandra.structure.jackson.CustomTypeResolverBuilderTest$2<java.lang.String>");
        list = (List<?>)list.get(1);
        assertEquals(list.size(), 2);
        assertTrue(list.contains("a"));
        assertTrue(list.contains("b"));

        list = (List<Object>)map.get("set");
        assertEquals(list.size(), 2);
        assertEquals(list.get(0), "com.ebuddy.cassandra.structure.jackson.CustomTypeResolverBuilderTest$3<java.lang.String>");
        list = (List<?>)list.get(1);
        assertEquals(list.size(), 2);
        assertTrue(list.contains("x"));
        assertTrue(list.contains("y"));
    }

    @Test(groups = "unit")
    public void convertValueShouldTypeListsWithoutSubclassedSet() throws Exception {
        TestPojoWithSet testObject = getTestPojoWithoutSubclassedSets();
        Object convertedObject = mapper.convertValue(testObject, Object.class);
        Map<String,?> map = (Map<String,?>)convertedObject;

        List<?> list = (List<Object>)map.get("list");
        assertEquals(list.size(), 2);
        assertEquals(list.get(0), "java.util.ArrayList");
        assertEquals((List<Object>)list.get(1), Arrays.asList("l2", "l1", "l3"));

        list = (List<?>)map.get("objectSet");
        assertEquals(list.size(), 2);
        assertEquals(list.get(0), "java.util.HashSet");
        list = (List<?>)list.get(1);
        assertEquals(list.size(), 2);
        assertTrue(list.contains(1));
        assertTrue(list.contains("X"));

        list = (List<Object>)map.get("hashSet");
        assertEquals(list.size(), 2);
        assertEquals(list.get(0), "java.util.HashSet");
        list = (List<?>)list.get(1);
        assertEquals(list.size(), 2);
        assertTrue(list.contains("a"));
        assertTrue(list.contains("b"));

        list = (List<Object>)map.get("set");
        assertEquals(list.size(), 2);
        assertEquals(list.get(0), "java.util.TreeSet");
        list = (List<?>)list.get(1);
        assertEquals(list.size(), 2);
        assertTrue(list.contains("x"));
        assertTrue(list.contains("y"));
    }

    @Test(groups = {"unit"})
    public void shouldConvertNormallyNoCollectionsWithAnnotations() throws Exception {
        TestPojoWithAnnotations testPojo = new TestPojoWithAnnotations("abc", "xyz");
        Map<?,?> simplifiedStructure = (Map<?,?>)mapper.convertValue(testPojo, Object.class);
        assertFalse(simplifiedStructure.containsKey("value"));
    }

    private TestPojoWithSet getTestPojoWithoutSubclassedSets() {
        HashSet<Object> objectSet = new HashSet<Object>();
        objectSet.add(1);
        objectSet.add("X");

        HashSet<String> hashSet = new HashSet<String>();
        hashSet.add("a");
        hashSet.add("b");

        Set<String> set = new TreeSet<String>();
        set.add("x");
        set.add("y");
        return new TestPojoWithSet("string",
                                   42L,
                                   true,
                                   Arrays.asList("l2", "l1", "l3"),
                                   objectSet,
                                   hashSet,
                                   set);
    }

    private TestPojoWithSet getTestPojoWithSubclassedSets() {
        return new TestPojoWithSet("string",
                                   42L,
                                   true,
                                   Arrays.asList("l2", "l1", "l3"),
                                   new HashSet<Object>() {{
                                       add(1);
                                       add("X");
                                   }},
                                   new HashSet<String>() {{
                                       add("a");
                                       add("b");
                                   }},
                                   new TreeSet<String>() {{
                                       add("x");
                                       add("y");
                                   }});
    }
}
