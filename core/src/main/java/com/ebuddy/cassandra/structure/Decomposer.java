package com.ebuddy.cassandra.structure;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Support for decomposing complex objects into paths to simple objects and vice-versa, i.e. composing
 * paths to objects back into complex objects.
 * Only the basic JSON structures are supported, i.e. Maps, Lists, Strings, Numbers, Booleans, and null.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class Decomposer {
    private static final Decomposer INSTANCE = new Decomposer();

    private Decomposer() { }

    public static Decomposer get() {
        return INSTANCE;
    }

    /**
     * Decompose a map of arbitrarily complex structured objects into a map of
     * simple objects keyed by paths.
     *
     * @param structures the input map of paths to objects
     * @return a map of simple object keyed by paths
     * @throws IllegalArgumentException if there is an object of unsupported type in the structures
     * or if structures is null
     */
    public Map<Path,Object> decompose(Map<Path,Object> structures) {
        if (structures == null) {
            throw new IllegalArgumentException("structures is null");
        }
        Map<Path,Object> decomposed = new HashMap<Path,Object>(structures.size());

        for (Map.Entry<Path,Object> entry : structures.entrySet()) {

            Object structure = entry.getValue();
            Path path = entry.getKey();

            if (Types.isSimple(structure)) {
                decomposed.put(path, structure);
                continue;
            }

            Map<Path,Object> decomposedMap = decomposeStructure(structure);

            for (Map.Entry<Path,Object> decomposedEntry : decomposedMap.entrySet()) {
                decomposed.put(path.concatenate(decomposedEntry.getKey()), decomposedEntry.getValue());
            }
        }
        return decomposed;
    }

    //////// Private Methods //////////

    @SuppressWarnings("ChainOfInstanceofChecks")
    private Map<Path,Object> decomposeStructure(Object structure) {
        Map<Path,Object> decomposedMap;
        if (structure instanceof Map) {
            decomposedMap = normalizeMap((Map<?,?>)structure);
        } else if (structure instanceof List) {
            decomposedMap = normalizeList((List<?>)structure);
        } else {
            throw new IllegalArgumentException("Unsupported data type: " + structure.getClass().getSimpleName());
        }
        return decompose(decomposedMap);
    }

    private Map<Path,Object> normalizeMap(Map<?,?> map) {
        Map<Path,Object> normalized = new HashMap<Path,Object>(map.size());
        for (Map.Entry<?,?> entry : map.entrySet()) {

            Object key = entry.getKey();
            Path keyPath = key instanceof Path ? (Path)key : Path.fromString(key.toString());

            Object value = entry.getValue();
            normalized.put(keyPath, value);
        }
        return normalized;
    }

    private Map<Path,Object> normalizeList(List<?> list) {
        Map<Path,Object> normalized = new HashMap<Path,Object>(list.size());
        for (int i = 0; i < list.size(); i++) {
            normalized.put(Path.fromIndex(i), list.get(i));
        }
        return normalized;
    }
}
