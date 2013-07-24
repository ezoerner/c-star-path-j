package com.ebuddy.cassandra.dao;

import java.util.List;

/**
 * Test class used for both unit and system tests in this package.
 * It contains the basic types supported by JSON.
 * TODO: Support null values and keys since JSON supports null. Currently a null value or key will throw an exception
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
@SuppressWarnings({"PublicField", "InstanceVariableNamingConvention", "UnusedDeclaration"})
public class TestPojo {
    public String s;
    public long n;
    public boolean b;
    public List<String> list;

    public TestPojo() {
    }

    public TestPojo(String s, long n, boolean b, List<String> list) {
        this.s = s;
        this.n = n;
        this.b = b;
        this.list = list;
    }

    public void setList(List<String> list) {
        this.list = list;
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
        if (n != testPojo.n) {
            return false;
        }
        if (!list.equals(testPojo.list)) {
            return false;
        }
        return s.equals(testPojo.s);
    }

    @Override
    public int hashCode() {
        int result = s.hashCode();
        result = 31 * result + (int)(n ^ (n >>> 32));
        result = 31 * result + (b ? 1 : 0);
        result = 31 * result + list.hashCode();
        return result;
    }
}
