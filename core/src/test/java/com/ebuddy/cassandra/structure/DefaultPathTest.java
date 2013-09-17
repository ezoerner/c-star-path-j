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

package com.ebuddy.cassandra.structure;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.ebuddy.cassandra.Path;

/**
 * Tests for DefaultPath.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class DefaultPathTest {

    @Test(groups = "unit")
    public void convertFromPathStringWithTrailingDelimiter() throws Exception {
        Path path = DefaultPath.fromEncodedPathString("x/y/");
        assertEquals(path.size(), 2);
        assertEquals(path.toString(), "x/y/");
    }

    @Test(groups = "unit")
    public void convertFromPathStringWithoutTrailingDelimiter() throws Exception {
        Path path = DefaultPath.fromEncodedPathString("x/y");
        assertEquals(path.size(), 2);
        assertEquals(path.toString(), "x/y/");
    }

    @Test(groups = "unit")
    public void convertFromPathStringOneElementTrailingDelimiter() throws Exception {
        Path path = DefaultPath.fromEncodedPathString("x/");
        assertEquals(path.size(), 1);
        assertEquals(path.toString(), "x/");
    }

    @Test(groups = "unit")
    public void convertFromPathStringOneElement() throws Exception {
        Path path = DefaultPath.fromStrings("x");
        assertEquals(path.size(), 1);
        assertEquals(path.toString(), "x/");
    }

    @Test(groups = "unit")
    public void convertFromEmptyPath() throws Exception {
        Path path = DefaultPath.fromEncodedPathString("");
        assertEquals(path.size(), 0);
        assertEquals(path.toString(), "");
        assertTrue(path.isEmpty());
    }

    @Test(groups = "unit")
    public void convertFromPathWithOnlyDelimiter() throws Exception {
        Path path = DefaultPath.fromEncodedPathString("/");
        assertEquals(path.size(), 0);
        assertEquals(path.toString(), "");
        assertTrue(path.isEmpty());
    }
}
