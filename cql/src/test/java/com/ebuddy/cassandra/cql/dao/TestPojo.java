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

package com.ebuddy.cassandra.cql.dao;

import java.util.List;

/**
 * Test class used for both unit and system tests in this package.
 * It contains the basic types supported by JSON.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
@SuppressWarnings({"PublicField", "InstanceVariableNamingConvention", "UnusedDeclaration"})
public class TestPojo {
    public String s;
    public long n;
    public String nullTest;
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

    public void setNullTest(String nullTest) {
        this.nullTest = nullTest;
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
        if (nullTest != null ? !nullTest.equals(testPojo.nullTest) : testPojo.nullTest != null) {
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
        result = 31 * result + (nullTest != null ? nullTest.hashCode() : 0);
        result = 31 * result + (b ? 1 : 0);
        result = 31 * result + list.hashCode();
        return result;
    }
}
