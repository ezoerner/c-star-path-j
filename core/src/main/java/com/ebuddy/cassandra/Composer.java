package com.ebuddy.cassandra;

import java.util.HashMap;
import java.util.Iterator;
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
        decomposeInPlace(decomposed, structures);
        return decomposed;
    }

    private void decomposeInPlace(Map<Path,Object> decomposed, Map<Path,Object> toDo) {
        while (!toDo.isEmpty()) {
            for (Iterator<Map.Entry<Path,Object>> iterator = toDo.entrySet().iterator(); iterator.hasNext(); ) {
                // get next to do entry and remove it from the to do map
                // TODO: this won't work because we can't modify the map we are iterating over
                Map.Entry<Path,Object> toDoEntry = iterator.next();
                iterator.remove();

                Object structure = toDoEntry.getValue();
                Path path = toDoEntry.getKey();

                if (isSimple(structure)) {
                    decomposed.put(path, structure);
                    continue;
                }

                Map<String,Object> simplerMap;
                if (structure instanceof Map) {
                    simplerMap = simplifyMap((Map<?,?>)structure);
                } else if (structure instanceof List) {
                    simplerMap = simplifyList((List<?>)structure);
                } else {
                    throw new IllegalArgumentException("Unsupported data type: " +
                                                               structure.getClass().getSimpleName());
                }

                for (Map.Entry<String,Object> simplerEntry : simplerMap.entrySet()) {
                    toDo.put(path.with(simplerEntry.getKey()), simplerEntry.getValue());
                }
            }
            decomposeInPlace(decomposed, toDo);
        }
    }

    public Map<Path,Object> compose(Map<Path,Object> map) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private Map<String,Object> simplifyMap(Map<?,?> map) {
        Map<String,Object> decomposed = new HashMap<String,Object>(map.size());
        for (Map.Entry<?,?> entry : map.entrySet()) {
            decomposed.put(toJsonObjectKey(entry.getKey()), entry.getValue());
        }
        return decomposed;
    }

    private String toJsonObjectKey(Object object) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private Map<String,Object> simplifyList(List<?> list) {
        Map<String,Object> decomposed = new HashMap<String,Object>(list.size());
        for (int i = 0; i < list.size(); i++) {
            decomposed.put(Path.listIndex(i), list.get(i));
        }
        return decomposed;
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
