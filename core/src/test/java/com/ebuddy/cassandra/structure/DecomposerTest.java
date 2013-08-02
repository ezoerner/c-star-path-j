package com.ebuddy.cassandra.structure;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ObjectUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for Decomposer.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class DecomposerTest {
    private Decomposer decomposer;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        decomposer = Decomposer.get();
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void decomposeNull() throws Exception {
        decomposer.decompose(null);
    }

    @Test(groups = "unit")
    public void decomposeEmpty() throws Exception {
        Map<Path,Object> result = decomposer.decompose(new HashMap<Path,Object>());
        assertTrue(result.isEmpty());
    }

    @Test(groups = "unit")
    public void decomposeSimpleObjectsWithSimplePaths() throws Exception {
        Map<Path,Object> structures = new HashMap<Path,Object>();
        structures.put(Path.fromString("x"), "");
        structures.put(Path.fromString("y"), 42);
        structures.put(Path.fromString("z"), true);
        structures.put(Path.fromString("N"), null);

        Map<Path,Object> result = decomposer.decompose(structures);

        assertNotSame(result, structures);

        // output of only simple objects is still equal to the input
        // with one exception, nulls are replaced by the NULL token
        structures.put(Path.fromString("N"), ObjectUtils.NULL);
        assertEquals(result, structures);
    }

    @Test(groups = "unit")
    public void decomposeSimpleObjectsWithLongerPaths() throws Exception {
        Map<Path,Object> structures = new HashMap<Path,Object>();
        structures.put(Path.fromString("a/b@/c"), "");
        structures.put(Path.fromString("d/e#/f"), 42);
        structures.put(Path.fromString("g/h/i"), true);
        structures.put(Path.fromString("j/k/l"), null);

        Map<Path,Object> result = decomposer.decompose(structures);

        assertNotSame(result, structures);

        // output of only simple objects is equal to the input
        //  except nulls are replaced by the NULL token.
        //  Note that the special characters are not URL-encoded in input paths  
        structures.put(Path.fromString("j/k/l"), ObjectUtils.NULL);
        assertEquals(result, structures);
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void decomposeUnsupportedType() throws Exception {
        Map<Path,Object> structures = new HashMap<Path,Object>();
        structures.put(Path.fromString("z"), new Object());

        decomposer.decompose(structures);
    }

    @Test(groups = "unit")
    public void decomposeNestedStructure() throws Exception {
        Map<Path,Object> structures = new HashMap<Path,Object>();
        Map<String,Object> nestedMap = new HashMap<String,Object>();
        nestedMap.put("y", "test");
        structures.put(Path.fromString("x"), nestedMap);

        Map<Path,Object> expected  = new HashMap<Path,Object>();
        expected.put(Path.fromString("x/y"), "test");

        Map<Path,Object> result = decomposer.decompose(structures);
        assertEquals(result, expected);
    }

    @Test(groups = "unit")
    public void decomposeNestedStructureWithLongerPath() throws Exception {
        Map<Path,Object> structures = new HashMap<Path,Object>();
        Map<String,Object> nestedMap = new HashMap<String,Object>();
        nestedMap.put("y", "test");
        nestedMap.put("@##//", "special@#");
        structures.put(Path.fromString("a/b/c"), nestedMap);

        // map keys are URL-encoded, values are not
        Map<Path,Object> expected  = new HashMap<Path,Object>();
        expected.put(Path.fromString("a/b/c/y"), "test");
        expected.put(Path.fromString("a/b/c/%40%23%23%2F%2F"), "special@#");

        Map<Path,Object> result = decomposer.decompose(structures);
        assertTrue(result.equals(expected));
    }

    @Test(groups = "unit")
    public void decomposeList() throws Exception {
        Map<Path,Object> structures = new HashMap<Path,Object>();
        structures.put(Path.fromString("list"), Arrays.asList("java.util.ArrayList", Arrays.asList("x", "y", "z")));

        Map<Path,Object> expected  = new HashMap<Path,Object>();
        expected.put(Path.fromString("list/@0"), "x");
        expected.put(Path.fromString("list/@1"), "y");
        expected.put(Path.fromString("list/@2"), "z");

        Map<Path,Object> result = decomposer.decompose(structures);
        assertEquals(result, expected);
    }

    @Test(groups = "unit")
    public void decomposeListOfMaps() throws Exception {
        Map<Path,Object> structures = new HashMap<Path,Object>();

        Map<String,Object> map1 = new HashMap<String,Object>();
        map1.put("k1", "v1");
        Map<String,Object> map2 = new HashMap<String,Object>();
        map2.put("k2", "v2");
        Map<String,Object> map3 = new HashMap<String,Object>();
        map3.put("k3", "v3");

        structures.put(Path.fromString("list"), Arrays.asList("java.util.ArrayList", Arrays.asList(map1, map2, map3)));

        Map<Path,Object> expected  = new HashMap<Path,Object>();
        expected.put(Path.fromString("list/@0/k1"), "v1");
        expected.put(Path.fromString("list/@1/k2"), "v2");
        expected.put(Path.fromString("list/@2/k3"), "v3");

        Map<Path,Object> result = decomposer.decompose(structures);
        assertEquals(result, expected);
    }

    @Test(groups = "unit")
    public void decomposeMapOfLists() throws Exception {
        Map<Path,Object> structures = new HashMap<Path,Object>();

        List<?> list1 = Arrays.asList("java.util.ArrayList", Arrays.asList("a", "b", "c"));
        List<?> list2 = Arrays.asList("java.util.ArrayList", Arrays.asList("d", "e", "f"));
        List<?> list3 = Arrays.asList("java.util.ArrayList", Arrays.asList("g", "h", "i"));

        structures.put(Path.fromString("1"), list1);
        structures.put(Path.fromString("2"), list2);
        structures.put(Path.fromString("3"), list3);

        Map<Path,Object> expected  = new HashMap<Path,Object>();

        expected.put(Path.fromString("1/@0"), "a");
        expected.put(Path.fromString("1/@1"), "b");
        expected.put(Path.fromString("1/@2"), "c");
        expected.put(Path.fromString("2/@0"), "d");
        expected.put(Path.fromString("2/@1"), "e");
        expected.put(Path.fromString("2/@2"), "f");
        expected.put(Path.fromString("3/@0"), "g");
        expected.put(Path.fromString("3/@1"), "h");
        expected.put(Path.fromString("3/@2"), "i");

        Map<Path,Object> result = decomposer.decompose(structures);
        assertEquals(result, expected);
    }

}
