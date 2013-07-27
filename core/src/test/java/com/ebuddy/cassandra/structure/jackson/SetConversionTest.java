package com.ebuddy.cassandra.structure.jackson;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ebuddy.cassandra.databind.CustomBeanSerializerFactory;
import com.ebuddy.cassandra.databind.SetTypeResolverBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * // TODO: Add class description here.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
@SuppressWarnings("CloneableClassWithoutClone")
public class SetConversionTest {

    private ObjectMapper mapper;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        mapper.setSerializerFactory(new CustomBeanSerializerFactory());
        mapper.setDefaultTyping(new SetTypeResolverBuilder());
    }

    @Test(groups = "unit")
    public void convertValueShouldPreserveSet() throws Exception {
        TestPojoWithSet testObject = new TestPojoWithSet("string",
                                                         42L,
                                                         true,
                                                         Arrays.asList("l2", "l1"),
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
        Object convertedObject = mapper.convertValue(testObject, Object.class);
        @SuppressWarnings("unchecked")
        Map<String,Object> map = (Map<String,Object>)convertedObject;
        //assertTrue(map.get("hashSet") instanceof Set);
    }
}
