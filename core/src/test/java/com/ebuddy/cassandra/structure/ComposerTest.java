package com.ebuddy.cassandra.structure;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ebuddy.cassandra.Path;
import com.google.common.collect.ImmutableMap;

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
        simpleObjects.put(DefaultPath.fromString("x"), "");
        simpleObjects.put(DefaultPath.fromString("y"), 42);
        simpleObjects.put(DefaultPath.fromString("z"), true);
        simpleObjects.put(DefaultPath.fromString("N"), null);

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
        simpleObjects.put(DefaultPath.fromString("a%2F%40%23/b/c"), "");
        simpleObjects.put(DefaultPath.fromString("a%2F%40%23/b/d"), 42);
        simpleObjects.put(DefaultPath.fromString("d/e/f"), true);
        simpleObjects.put(DefaultPath.fromString("d/f/g"), false);
        simpleObjects.put(DefaultPath.fromString("j/k/l"), null);

        /////////////
        Object result = composer.compose(simpleObjects);
        /////////////

        Map<String,Object> expectedResult = getExpectedMapForComposeSimpleObjectWithLongerPaths();
        assertEquals(result, expectedResult);
    }

    @Test(groups = {"unit"})
    public void composeList() throws Exception {
        Map<Path,Object> simpleObjects = new HashMap<Path,Object>();
        simpleObjects.put(DefaultPath.fromString("a/@0"), "");
        simpleObjects.put(DefaultPath.fromString("a/@1"), 42);

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
        simpleObjects.put(DefaultPath.fromString("a/@0"), "");
        simpleObjects.put(DefaultPath.fromString("a/@2"), 42);

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
        simpleObjects.put(DefaultPath.fromString("a/@0/b"), "");
        simpleObjects.put(DefaultPath.fromString("a/@0/c"), 42);
        simpleObjects.put(DefaultPath.fromString("a/@1/b"), "");
        simpleObjects.put(DefaultPath.fromString("a/@1/c"), 42);

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
        simpleObjects.put(DefaultPath.fromString("a/@0"), "");
        simpleObjects.put(DefaultPath.fromString("a/@1"), 42);
        simpleObjects.put(DefaultPath.fromString("b/@0"), "");
        simpleObjects.put(DefaultPath.fromString("b/@1"), 42);

        /////////////
        Object result = composer.compose(simpleObjects);
        /////////////

        Map<String,Object> expectedResult = new HashMap<String,Object>() {{
            put("a", Arrays.asList("",42));
            put("b", Arrays.asList("",42));
        }};

        assertEquals(result, expectedResult);
    }

    @Test(groups = {"unit"},
          description = "tests the inconsistent root when the structure comes first in iteration order")
    public void shouldComposeInconsistentRoot() throws Exception {
        Map<Path,Object> map = ImmutableMap.<Path,Object>of(DefaultPath.fromString("a/b/"), "c",
                                                            DefaultPath.fromString("a/"), "d");
        // inconsistent paths, cannot have both a map (with key b) at "a/" and also a simple object at "a/"

        /////////////
        Object result = composer.compose(map);
        /////////////

        // expected result has no loss of information with special key used for the inconsistent root
        Map<String,Object> expectedResult = ImmutableMap.<String,Object>of("a", ImmutableMap.of("b", "c",
                                                                                                "@ROOT", "d"));
        assertEquals(result, expectedResult);
    }

    @Test(groups = {"unit"}, description = "tests the inconsistent root when the simple value comes first in iteration order")
    public void shouldComposeInconsistentRootOtherOrder() throws Exception {
        // guava ImmutableMaps have user-defined iteration order :-)
        Map<Path,Object> map = ImmutableMap.<Path,Object>of(DefaultPath.fromString("a/"), "d",
                                                            DefaultPath.fromString("a/b/"), "c");
        // inconsistent paths, cannot have both a map (with key b) at "a/" and also a simple object at "a/"

        /////////////
        Object result = composer.compose(map);
        /////////////

        // expected result has no loss of information with special key used for the inconsistent root
        Map<String,Object> expectedResult = ImmutableMap.<String,Object>of("a", ImmutableMap.of("b", "c",
                                                                                                "@ROOT", "d"));
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
        result.put("a/@#", innerMap2);

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
