package com.ebuddy.cassandra.cql;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 */
public class StructuredDaoTest {

    @Test
    public void testJsonNumbers() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Object obj = mapper.readValue("42", Object.class);
        assertEquals(obj.getClass().getSimpleName(), "Integer");
    }

    @Test
    public void testJsonNumbersInMap() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Map<Integer,String> map = new HashMap<Integer, String>() {{
            put(1,"3");
            put(2,"4");
        }};
        String jsonString = mapper.writeValueAsString(map);

        Map<?,?> map2 = mapper.readValue(jsonString, Map.class);
        assertTrue(map2.containsKey("1"));

        Map<Integer,String> map3 = mapper.readValue(jsonString, new TypeReference<Map<Integer,String>>() { });
        assertTrue(map3.containsKey(1));
    }
}
