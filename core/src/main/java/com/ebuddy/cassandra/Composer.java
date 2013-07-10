package com.ebuddy.cassandra;

import java.util.HashMap;
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
            
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private boolean isSimple(Object structure) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public Map<Path,Object> compose(Map<Path,Object> map) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

}
