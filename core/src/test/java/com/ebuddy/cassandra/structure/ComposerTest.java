package com.ebuddy.cassandra.structure;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for Composer.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
@SuppressWarnings({"MagicNumber", "CloneableClassWithoutClone"})
public class ComposerTest {
    private Composer composer;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        composer = Composer.get();
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

    @Test(groups = "unit")
    public void composeSimpleObjectsWithSimplePaths() throws Exception {
        Map<Path,Object> simpleObjects = new HashMap<Path,Object>();
        simpleObjects.put(Path.fromString("x"), "");
        simpleObjects.put(Path.fromString("y"), 42);
        simpleObjects.put(Path.fromString("z"), true);
        simpleObjects.put(Path.fromString("N"), null);

        /////////////
        Object result = composer.compose(simpleObjects);
        /////////////

        // output of only simple objects is equal to the input, but has strings as keys instead of Paths
        Map<String,Object> expectedResult = new HashMap<String,Object>();
        for (Map.Entry<Path,Object> entry : simpleObjects.entrySet()) {
            expectedResult.put(entry.getKey().head(), entry.getValue());
        }

        assertEquals(result, expectedResult);
    }

    @Test(groups = "unit")
    public void composeSimpleObjectsWithLongerPaths() throws Exception {
        Map<Path,Object> simpleObjects = new HashMap<Path,Object>();
        simpleObjects.put(Path.fromString("a/b/c"), "");
        simpleObjects.put(Path.fromString("a/b/d"), 42);
        simpleObjects.put(Path.fromString("d/e/f"), true);
        simpleObjects.put(Path.fromString("d/f/g"), false);
        simpleObjects.put(Path.fromString("j/k/l"), null);

        /////////////
        Object result = composer.compose(simpleObjects);
        /////////////

        Map<String,Object> expectedResult = getExpectedMapForComposeSimpleObjectWithLongerPaths();
        assertEquals(result, expectedResult);
    }

    @Test(groups = {"unit"})
    public void composeList() throws Exception {
        Map<Path,Object> simpleObjects = new HashMap<Path,Object>();
        simpleObjects.put(Path.fromString("a/@0"), "");
        simpleObjects.put(Path.fromString("a/@1"), 42);

        /////////////
        Object result = composer.compose(simpleObjects);
        /////////////

        Map<String,Object> expectedResult = new HashMap<String,Object>();
        expectedResult.put("a", Arrays.asList("", 42));

        assertEquals(result, expectedResult);
    }

    @Test(groups = {"unit"})
    public void composeListWithDeletedIndex() throws Exception {
        Map<Path,Object> simpleObjects = new HashMap<Path,Object>();
        simpleObjects.put(Path.fromString("a/@0"), "");
        simpleObjects.put(Path.fromString("a/@2"), 42);

        /////////////
        Object result = composer.compose(simpleObjects);
        /////////////

        Map<String,Object> expectedResult = new HashMap<String,Object>();
        expectedResult.put("a", Arrays.asList("", 42));

        assertEquals(result, expectedResult);
    }

    @Test(groups = {"unit"})
    public void composeListOfMaps() throws Exception {
        Map<Path,Object> simpleObjects = new HashMap<Path,Object>();
        simpleObjects.put(Path.fromString("a/@0/b"), "");
        simpleObjects.put(Path.fromString("a/@0/c"), 42);
        simpleObjects.put(Path.fromString("a/@1/b"), "");
        simpleObjects.put(Path.fromString("a/@1/c"), 42);

        /////////////
        Object result = composer.compose(simpleObjects);
        /////////////

        Map<String,Object> expectedResult = new HashMap<String,Object>();
        Map<String,Object> innerMap = new HashMap<String,Object>() {{
            put("b", "");
            put("c", 42);
        }};
        expectedResult.put("a", Arrays.asList(innerMap, innerMap));

        assertEquals(result, expectedResult);
    }

    @Test(groups = {"unit"})
    public void composeMapOfLists() throws Exception {
        Map<Path,Object> simpleObjects = new HashMap<Path,Object>();
        simpleObjects.put(Path.fromString("a/@0"), "");
        simpleObjects.put(Path.fromString("a/@1"), 42);
        simpleObjects.put(Path.fromString("b/@0"), "");
        simpleObjects.put(Path.fromString("b/@1"), 42);

        /////////////
        Object result = composer.compose(simpleObjects);
        /////////////

        Map<String,Object> expectedResult = new HashMap<String,Object>() {{
            put("a", Arrays.asList("",42));
            put("b", Arrays.asList("",42));
        }};

        assertEquals(result, expectedResult);
    }


    private Map<String,Object> getExpectedMapForComposeSimpleObjectWithLongerPaths() {
        Map<String,Object> result = new HashMap<String,Object>();
        final Map<String,Object> innerMap1 = new HashMap<String,Object>() {{
            put("c", "");
            put("d", 42);
        }};
        Map<String,Object> innerMap2 = new HashMap<String,Object>() {{
            put("b", innerMap1);
        }};
        result.put("a", innerMap2);

        final Map<String,Object> innerMapF = new HashMap<String,Object>() {{
            put("f", true);
        }};
        final Map<String,Object> innerMapG = new HashMap<String,Object>() {{
            put("g", false);
        }};
        Map<String,Object> innerMapEF = new HashMap<String,Object>() {{
            put("e", innerMapF);
            put("f", innerMapG);
        }};
        result.put("d", innerMapEF);

        final Map<String,Object> innerMapL = new HashMap<String,Object>() {{
            put("l", null);
        }};
        Map<String,Object> innerMapK = new HashMap<String,Object>() {{
            put("k", innerMapL);
        }};
        result.put("j", innerMapK);
        return result;
    }

}
