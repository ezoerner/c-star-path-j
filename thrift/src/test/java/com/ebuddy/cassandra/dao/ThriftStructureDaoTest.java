package com.ebuddy.cassandra.dao;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Tests for ThriftStructureDao.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class ThriftStructureDaoTest {

    @Mock
    private ColumnFamilyOperations<String,String,Object> operations;

    private ThriftStructureDao<String> dao ;
    private final String columnFamily = "columnfamily";
    private final String rowKey = "rowKey";
    private final String pathString = "a/b/c";
    private final TypeReference<TestPojo> typeReference = new TypeReference<TestPojo>() { };

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        dao = new ThriftStructureDao<String>(operations);
    }

    @Test(groups = {"unit"})
    public void shouldReadFromPath() throws Exception {

        Map<String,Object> columnsMap = new HashMap<String,Object>();
        columnsMap.put("a/b/c/s/", "v1");
        columnsMap.put("a/b/c/n/", 42L);
        columnsMap.put("a/b/c/alwaysNull/", null);
        columnsMap.put("a/b/c/b/", true);

        when(operations.readColumnsAsMap(columnFamily, rowKey,
                                         "a/b/c/",
                                         "a/b/c/" + Character.MAX_VALUE,
                                         Integer.MAX_VALUE,
                                         false)).thenReturn(columnsMap);

        //////////////////////
        TestPojo result = dao.readFromPath(columnFamily, rowKey, pathString, typeReference);
        //////////////////////


        TestPojo expectedResult = new TestPojo("v1", 42L, true);
        assertEquals(result, expectedResult);
    }

    @Test(groups = {"unit"})
    public void shouldWriteToPath() throws Exception {
        TestPojo testObject = new TestPojo("v1", 42L, true);

        //////////////////////
        dao.writeToPath(columnFamily, rowKey, pathString, testObject);
        //////////////////////

        Map<String,Object> stringObjectMap = new HashMap<String,Object>();
        stringObjectMap.put("a/b/c/s/", "v1");
        stringObjectMap.put("a/b/c/n/", 42L);
        stringObjectMap.put("a/b/c/alwaysNull/", null);
        stringObjectMap.put("a/b/c/b/", true);

        verify(operations).writeColumns(columnFamily, rowKey, stringObjectMap);
    }

    @SuppressWarnings({"PublicField", "InstanceVariableNamingConvention", "UnusedDeclaration"})
    public static class TestPojo {
        public String s;
        public long n;
        public final String alwaysNull = null;
        public boolean b;

        public TestPojo() {
        }

        public TestPojo(String s, long n, boolean b) {
            this.s = s;
            this.n = n;
            this.b = b;
        }

        public void setN(long n) {
            this.n = n;
        }

        public void setB(boolean b) {
            this.b = b;
        }

        public void setS(String s) {
            this.s = s;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestPojo testPojo = (TestPojo)o;

            if (b != testPojo.b) {
                return false;
            }
            if (Float.compare(testPojo.n, n) != 0) {
                return false;
            }
            if (alwaysNull != null ? !alwaysNull.equals(testPojo.alwaysNull) : testPojo.alwaysNull != null) {
                return false;
            }
            return s.equals(testPojo.s);
        }

        @Override
        public int hashCode() {
            int result = s.hashCode();
            result = 31 * result + (n == +0.0f ? 0 : Float.floatToIntBits(n));
            result = 31 * result + (alwaysNull != null ? alwaysNull.hashCode() : 0);
            result = 31 * result + (b ? 1 : 0);
            return result;
        }
    }
}
