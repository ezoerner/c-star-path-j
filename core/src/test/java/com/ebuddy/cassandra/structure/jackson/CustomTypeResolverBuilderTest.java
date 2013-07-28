package com.ebuddy.cassandra.structure.jackson;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
@SuppressWarnings({"CloneableClassWithoutClone", "OverlyStrongTypeCast", "unchecked"})
public class CustomTypeResolverBuilderTest {

    private ObjectMapper mapper;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        mapper.setDefaultTyping(new CustomTypeResolverBuilder());
    }

    @Test(groups = "unit")
    public void convertValueShouldTypeLists() throws Exception {
        TestPojoWithSet testObject = getTestPojoWithSets();
        Object convertedObject = mapper.convertValue(testObject, Object.class);
        Map<String,?> map = (Map<String,?>)convertedObject;

        List<?> list = (List<Object>)map.get("list");
        assertEquals(list.size(), 2);
        assertEquals(list.get(0), "java.util.ArrayList");
        assertEquals((List<Object>)list.get(1), Arrays.asList("l2", "l1", "l3"));

        list = (List<?>)map.get("objectSet");
        assertEquals(list.size(), 2);
        assertEquals(list.get(0), "com.ebuddy.cassandra.structure.jackson.CustomTypeResolverBuilderTest$1");
        list = (List<?>)list.get(1);
        assertEquals(list.size(), 2);
        assertTrue(list.contains(1));
        assertTrue(list.contains("X"));

        list = (List<Object>)map.get("hashSet");
        assertEquals(list.size(), 2);
        assertEquals(list.get(0), "com.ebuddy.cassandra.structure.jackson.CustomTypeResolverBuilderTest$2");
        list = (List<?>)list.get(1);
        assertEquals(list.size(), 2);
        assertTrue(list.contains("a"));
        assertTrue(list.contains("b"));

        list = (List<Object>)map.get("set");
        assertEquals(list.size(), 2);
        assertEquals(list.get(0), "com.ebuddy.cassandra.structure.jackson.CustomTypeResolverBuilderTest$3");
        list = (List<?>)list.get(1);
        assertEquals(list.size(), 2);
        assertTrue(list.contains("x"));
        assertTrue(list.contains("y"));
    }

    @Test(groups = "unit", enabled=false)
    public void convertingToPojoShouldPreserveSets() throws Exception {
        TestPojoWithSet testObject = getTestPojoWithSets();
        Object convertedObject = mapper.convertValue(testObject, Object.class);
    }

    private TestPojoWithSet getTestPojoWithSets() {
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
                                   }}
        );
    }
}
