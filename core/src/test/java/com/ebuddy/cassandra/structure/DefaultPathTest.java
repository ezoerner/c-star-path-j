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
