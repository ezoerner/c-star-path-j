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

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TestPojoWithAnnotations {

    private final String string1;
    private final String string2;

    public TestPojoWithAnnotations(String string2, String string1) {
        this.string2 = string2;
        this.string1 = string1;
    }

    public String getString2() {
        return string2;
    }

    public String getString1() {
        return string1;
    }

    @JsonIgnore
    public String getValue() {
        String delimiter = ":";
        return string2 + delimiter + string1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TestPojoWithAnnotations that = (TestPojoWithAnnotations)o;

        if (string2 != null ? !string2.equals(that.string2) : that.string2 != null) {
            return false;
        }
        return !(string1 != null ? !string1.equals(that.string1) : that.string1 != null);

    }

    @Override
    public int hashCode() {
        int result = string1 != null ? string1.hashCode() : 0;
        result = 31 * result + (string2 != null ? string2.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("smsIntegrator", getString2())
                .append("virtualNumber", getString1()).toString();
    }
}

