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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.TreeSet;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ebuddy.cassandra.Path;
import com.ebuddy.cassandra.StructuredDataSupport;
import com.ebuddy.cassandra.TypeReference;
import com.ebuddy.cassandra.test.AbstractCassandraThriftSystemTest;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * System tests for ThriftStructuredDataSupport.
 * Requires a Cassandra 1.2.x+ instance
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class ThriftSuperStructuredDataSupportSystemTest extends AbstractCassandraThriftSystemTest {

    private final String columnFamily = "testpojo";
    private StructuredDataSupport<String> dao;

    @Override
    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        super.setUp();
        Serializer<String> keySerializer = StringSerializer.get();
        Serializer<String> supercolumnSerializer = StringSerializer.get();
        Serializer<String> columnNameSerializer = StringSerializer.get();
        Serializer<Object> valueSerializer = StructureSerializer.get();
        SuperColumnFamilyOperations<String,String,String,Object> operations = new SuperColumnFamilyTemplate<String,String,String,Object>(
                keyspace,
                columnFamily,
                keySerializer,
                supercolumnSerializer,
                columnNameSerializer,
                valueSerializer);

        dao = new ThriftSuperStructuredDataSupport<String>(operations);
    }

    @Test(groups = {"system"})
    public void shouldWriteReadDeleteTestPojo() throws Exception {
        TestPojo testObject = new TestPojo("v1", 42L, true, Arrays.asList("e1", "e2"));
        String rowKey = "pojo0";
        Path path = dao.createPath("a","b","c");
        TypeReference<TestPojo> typeReference = new TypeReference<TestPojo>() { };

        dao.writeToPath(rowKey, path, testObject);
        TestPojo result = dao.readFromPath(rowKey, path, typeReference);
        assertNotSame(result, testObject);
        assertEquals(result, testObject);

        dao.deletePath(rowKey, path);
        TestPojo result2 = dao.readFromPath(rowKey, path, typeReference);
        assertNull(result2);
    }

    @Test(groups = {"system"})
    public void shouldWriteReadDeleteTestPojoWithSet() throws Exception {
        TestPojoWithSet testObject = getTestPojoWithSubclassedSets();
        String rowKey = "pojo1";
        Path path = dao.createPath("a","b","c");
        TypeReference<TestPojoWithSet> typeReference = new TypeReference<TestPojoWithSet>() { };

        dao.writeToPath(rowKey, path, testObject);
        TestPojoWithSet result = dao.readFromPath(rowKey, path, typeReference);
        assertNotSame(result, testObject);
        assertEquals(result, testObject);

        dao.deletePath(rowKey, path);
        TestPojoWithSet result2 = dao.readFromPath(rowKey, path, typeReference);
        assertNull(result2);
    }



    @Override
    protected void dropAndCreateSchema() throws InterruptedException {
        super.dropAndCreateSchema();
        ColumnFamilyDefinition columnFamilyDefinition = HFactory.createColumnFamilyDefinition(keyspace.getKeyspaceName(),
                                                                                              columnFamily,
                                                                                              ComparatorType.UTF8TYPE);
        columnFamilyDefinition.setColumnType(ColumnType.SUPER);
        columnFamilyDefinition.setDefaultValidationClass("UTF8Type");
        columnFamilyDefinition.setSubComparatorType(ComparatorType.UTF8TYPE);
        cluster.addColumnFamily(columnFamilyDefinition);
    }

    @SuppressWarnings("CloneableClassWithoutClone")
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
