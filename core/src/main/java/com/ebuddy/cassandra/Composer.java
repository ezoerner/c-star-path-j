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
        Map<Path,Object> result = new HashMap<Path,Object>();
        for (Map.Entry<Path,Object> entry : structures.entrySet()) {
            Object structure = entry.getValue();
            Path path = entry.getKey();
            if (isSimple(structure)) {
                result.put(path, structure);
                continue;
            }
            if (structure instanceof Map) {
                decomposeMap((Map<?,?>)structure, path, result);
                continue;
            }
            if (structure instanceof List) {
                decomposeList((List<?>)structure, path, result);
            }
            throw new IllegalArgumentException("Unsupported data type: " + structure.getClass().getSimpleName());
        }
        return result;
    }

    public Map<Path,Object> compose(Map<Path,Object> map) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private void decomposeList(List<?> structure, Path path, Map<Path,Object> result) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private void decomposeMap(Map<?,?> structure, Path path, Map<Path,Object> result) {
        throw new UnsupportedOperationException("Not yet implemented");
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
