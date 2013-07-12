package com.ebuddy.cassandra;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Support for decomposing complex objects into paths to simple objects and vice-versa, i.e. composing
 * paths to objects back into complex objects.
 *
 * Only the basic JSON structures are supported, i.e. Maps, Lists, Strings, Numbers, Booleans, and null.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class Composer {
    private static final Class<?>[] simpleTypes = new Class<?>[] {
        String.class, Number.class, Boolean.class
    };

    /**
     * Decompose a map arbitrarily complex structured objects into a map of
     * simple objects.
     */
    public Map<Path,Object> decompose(Map<Path,Object> structures) {
        Map<Path,Object> decomposed = new HashMap<Path,Object>(structures.size());

        for (Map.Entry<Path,Object> entry : structures.entrySet()) {

            Object structure = entry.getValue();
            Path path = entry.getKey();

            if (isSimple(structure)) {
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

    public Map<Path,Object> compose(Map<Path,Object> map) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    //////// Private Methods //////////

    private Map<Path,Object> decomposeStructure(Object structure) {
        Map<Path,Object> decomposedMap;
        if (structure instanceof Map) {
            decomposedMap = normalizeMap((Map<?,?>)structure);
        } else if (structure instanceof List) {
            decomposedMap = normalizeList((List<?>)structure);
        } else {
            throw new IllegalArgumentException("Unsupported data type: " +
                                                       structure.getClass().getSimpleName());
        }
        return decompose(decomposedMap);
    }

    private Map<Path,Object> normalizeMap(Map<?,?> map) {
        Map<Path,Object> normalized = new HashMap<Path,Object>(map.size());
        for (Map.Entry<?,?> entry : map.entrySet()) {
            Path key = Path.fromObject(entry.getKey());
            Object value = entry.getValue();
            normalized.put(key, value);
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

    private boolean isSimple(Object object) {
        if (object == null) {
            return true;
        }
        for (Class<?> type : simpleTypes) {
            if (type.isInstance(object)) {
                return true;
            }
        }
        return false;
    }

}
