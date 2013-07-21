package com.ebuddy.cassandra.structure;

import static org.testng.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for Composer.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class ComposerTest {
    private Composer composer;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        composer = new Composer();
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void composeNull() throws Exception {
        composer.compose(null);
    }

    @Test(groups = "unit")
    public void composeEmpty() throws Exception {
        Object result = composer.compose(new HashMap<Path,Object>());
        assertEquals(result, Collections.emptyMap());
    }
}
