package com.ebuddy.cassandra.cql;

import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 */
public class StructuredDaoTest {
    @Test
    public void testJsonNumbers() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Object obj = mapper.readValue("42", Object.class);
        assertEquals(obj.getClass().getSimpleName(), "Number");
    }
}
