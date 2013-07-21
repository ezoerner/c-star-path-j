package com.ebuddy.cassandra.property;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.testng.annotations.Test;

/**
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
@SuppressWarnings({"JavaDoc"})
public class PropertyValueTest {

    @Test(groups = {"unit"})
    public void testJsonStringValue() throws Exception {
        PropertyValueFactory valueFactory = PropertyValueFactory.get();
        ObjectMapper mapper = new ObjectMapper();
        PropertyValue<String> propertyValue = valueFactory.createPropertyValue("fubar");
        assertSame(propertyValue.getClass(), StringValue.class);
        String jsonString = mapper.writeValueAsString(propertyValue);
        PropertyValue<String> newValue = mapper.readValue(jsonString, new TypeReference<PropertyValue<String>>() { });
        assertSame(newValue.getClass(), StringValue.class);
        assertEquals(newValue, propertyValue);
    }

    @Test(groups = {"unit"})
    public void testStoreStringAsBytes() throws Exception {
        PropertyValueFactory valueFactory = PropertyValueFactory.get();
        String string = "value";
        PropertyValue<String> propertyValue = valueFactory.createPropertyValue(string);
        assertSame(propertyValue.getClass(), StringValue.class);
        ByteBuffer storeBytes = propertyValue.toBytes();

        PropertyValue<?> newValue = valueFactory.createPropertyValue(storeBytes);

        assertEquals(newValue, propertyValue);
        assertSame(newValue.getClass(), StringValue.class);
        String actualString = (String)newValue.getValue();
        String expectedString = propertyValue.getValue();
        assertEquals(actualString, expectedString);
    }


    @Test(groups = {"unit"})
    public void testJsonStringValueInOtherObject() throws Exception {
        PropertyValueFactory valueFactory = PropertyValueFactory.get();
        ObjectMapper mapper = new ObjectMapper();
        PropertyValue<String> propertyValue = valueFactory.createPropertyValue("fubar");

        List<PropertyValue<String>> list = new ArrayList<PropertyValue<String>>();
        list.add(propertyValue);

        String jsonString = mapper.writeValueAsString(list);
        List<PropertyValue<String>> newValue = mapper.readValue(jsonString, new TypeReference<List<PropertyValue<String>>>() { });
        assertEquals(newValue, list);
    }

    @Test(groups = {"unit"})
    public void testJsonNestedProperties() throws Exception {
        PropertyValueFactory valueFactory = PropertyValueFactory.get();
        ObjectMapper mapper = new ObjectMapper();
        Map<String,PropertyValue<?>> map = new HashMap<String, PropertyValue<?>>();
        map.put("prop1", valueFactory.createPropertyValue("value1"));
        map.put("prop2", valueFactory.createPropertyValue("value2"));
        PropertyValue<Map<String,PropertyValue<?>>> propertyValue = valueFactory.createPropertyValue(map);

        assertSame(propertyValue.getClass(), NestedProperties.class);

        String jsonString = mapper.writeValueAsString(propertyValue);
        PropertyValue<Map<String,PropertyValue<?>>> newValue
                = mapper.readValue(jsonString, new TypeReference<PropertyValue<Map<String,PropertyValue<?>>>>() { });

        assertSame(newValue.getClass(), NestedProperties.class);

        assertEquals(newValue, propertyValue);
    }

    @Test(groups = {"unit"})
    public void testStoreMapAsBytes() throws Exception {
        PropertyValueFactory valueFactory = PropertyValueFactory.get();
        Map<String, PropertyValue<?>> map = new HashMap<String, PropertyValue<?>>();
        map.put("key1", valueFactory.createPropertyValue("val1"));
        map.put("key2", valueFactory.createPropertyValue("val2"));
        PropertyValue<Map<String, PropertyValue<?>>> propertyValue = valueFactory.createPropertyValue(map);
        assertSame(propertyValue.getClass(), NestedProperties.class);
        ByteBuffer storeBytes = propertyValue.toBytes();

        @SuppressWarnings({"unchecked"})
        PropertyValue<Map<String, PropertyValue<?>>> newValue =
                (PropertyValue<Map<String, PropertyValue<?>>>)valueFactory.createPropertyValue(storeBytes);

        assertEquals(newValue, propertyValue);
        assertSame(newValue.getClass(), NestedProperties.class);
        Map<String, PropertyValue<?>> actualMap = newValue.getValue();
        Map<String, PropertyValue<?>> expectedMap = propertyValue.getValue();
        assertEquals(actualMap, expectedMap);
    }


    @Test(groups = {"unit"})
    public void testJsonListOfStrings() throws Exception {
        PropertyValueFactory valueFactory = PropertyValueFactory.get();
        ObjectMapper mapper = new ObjectMapper();
        List<String> list = new ArrayList<String>();
        list.add("val1");
        list.add("val2");
        PropertyValue<List<String>> propertyValue = valueFactory.createPropertyValue(list);
        String jsonString = mapper.writeValueAsString(propertyValue);
        PropertyValue<List<String>> newValue
                = mapper.readValue(jsonString, new TypeReference<PropertyValue<List<String>>>() { });
        assertEquals(newValue, propertyValue);
        List<String> actualList = newValue.getValue();
        List<String> expectedList = propertyValue.getValue();
        assertEquals(actualList, expectedList);
    }

    @Test(groups = {"unit"})
    public void testStoreListAsBytes() throws Exception {
        PropertyValueFactory valueFactory = PropertyValueFactory.get();
        List<String> list = new ArrayList<String>();
        list.add("val1");
        list.add("val2");
        PropertyValue<List<String>> propertyValue = valueFactory.createPropertyValue(list);
        assertTrue(propertyValue.getClass() == ListValue.class);
        ByteBuffer storeBytes = propertyValue.toBytes();
        
        @SuppressWarnings({"unchecked"})
        PropertyValue<List<String>> newValue =
                (PropertyValue<List<String>>)valueFactory.createPropertyValue(storeBytes);

        assertEquals(newValue, propertyValue);
        assertTrue(newValue.getClass() == ListValue.class);
        List<String> actualList = newValue.getValue();
        List<String> expectedList = propertyValue.getValue();
        assertEquals(actualList, expectedList);
    }

    @Test(groups = {"unit"})
    public void testJsonNestedPropertiesMultipleLevels() throws Exception {
        PropertyValueFactory valueFactory = PropertyValueFactory.get();
        ObjectMapper mapper = new ObjectMapper();
        Map<String,PropertyValue<?>> map = new HashMap<String, PropertyValue<?>>();
        Map<String,PropertyValue<?>> mapLevel = new HashMap<String, PropertyValue<?>>();
        mapLevel.put("prop1", valueFactory.createPropertyValue("valueLevel1"));
        mapLevel.put("prop2", valueFactory.createPropertyValue("valueLevel2"));
        
        map.put("prop1", valueFactory.createPropertyValue(mapLevel));
        map.put("prop2", valueFactory.createPropertyValue("value2"));
        
        PropertyValue<Map<String,PropertyValue<?>>> preJsonHighLevelPropertyValue = valueFactory.createPropertyValue(map);
        String jsonString = mapper.writeValueAsString(preJsonHighLevelPropertyValue);
        //@ Note that with multi levels it's getting tricky with generics
        //this is how the data is read from one level to another
        //using the  isNestedProperties will allow us to know
        //if there is a lower level for the current  PropertyValue
        
        //get 1st level
        PropertyValue<Map<String,PropertyValue<?>>> postJSONLevelOneNewValue
                = mapper.readValue(jsonString, new TypeReference<PropertyValue<?>>() { });
        assertEquals(postJSONLevelOneNewValue, preJsonHighLevelPropertyValue);
        
        //get Level two
        PropertyValue<?> postJSONLevelTwoNewValue = postJSONLevelOneNewValue.getValue().get("prop1");        
        assertTrue(postJSONLevelTwoNewValue.isNestedProperties());
               
        
        //Get level 3
        @SuppressWarnings({"unchecked"})
        PropertyValue<?> postJSONLevelThreeNewValue =
                ((Map<String,PropertyValue<?>>)postJSONLevelTwoNewValue.getValue()).get("prop1");

        assertFalse(postJSONLevelThreeNewValue.isNestedProperties());                
    }
    
    @Test(groups = {"unit"})
    public void testJsonNestedPropertiesInOtherObject() throws Exception {
        PropertyValueFactory valueFactory = PropertyValueFactory.get();
        ObjectMapper mapper = new ObjectMapper();
        Map<String,PropertyValue<?>> map = new HashMap<String, PropertyValue<?>>();
        map.put("prop1", valueFactory.createPropertyValue("value1"));
        map.put("prop2", valueFactory.createPropertyValue("value2"));
        PropertyValue<Map<String,PropertyValue<?>>> propertyValue = valueFactory.createPropertyValue(map);

        List<PropertyValue<Map<String,PropertyValue<?>>>> list = new ArrayList<PropertyValue<Map<String,PropertyValue<?>>>>();
        list.add(propertyValue);

        String jsonString = mapper.writeValueAsString(list);
        List<PropertyValue<Map<String,PropertyValue<?>>>> newValue
                = mapper.readValue(jsonString, new TypeReference<List<PropertyValue<?>>>() { });
        assertEquals(newValue, list);
    }

    @Test(groups={"unit"})
    /**
     * This tests what happens when converting a "structured" string back to a PropertyValue.
     * This situation can occur when encountering data written by writing a PropertyValue containing
     * an enum or a UUID, which was written by a previous version of the code that had a bug in it
     * that allowed arbitrary objects to be created into a PropertyValue as a structured object, then
     * written out as a String. This created an entry in the database with the header bytes for a
     * structured object followed by the toString result of the object surrounded by double quotes.
     * The correct behavior is to read the object as a PropertyValue<String> (StringValue)
     */
    public void testStructuredString() throws Exception {
        byte[] bytes = ArrayUtils.addAll(ListValue.UTF8_HEADER_BYTES, "\"test\"".getBytes("utf8"));
        PropertyValue<?> stringValue = PropertyValueFactory.get().createPropertyValue(bytes);
        assertEquals(stringValue, PropertyValueFactory.get().createPropertyValue("test"));
    }
}
