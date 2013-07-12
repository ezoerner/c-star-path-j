package com.ebuddy.cassandra;

import java.util.Iterator;
import java.util.Map;


/**
 * Support for composing paths to objects back  complex objects.
 * Only the basic JSON structures are supported, i.e. Maps, Lists, Strings, Numbers, Booleans, and null.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class Composer {

    /**
     * Compose a map of simple objects keyed by paths into a single complex object.
     *
     * @param decomposedObjects input map of decomposed objects, paths mapped to simple values
     * @return a complex object such as a map or list decoded from the paths in decomposedObjects,
     *         or null if decomposedObjects is empty.
     * @throws IllegalArgumentException if there are unsupported objects types in decomposedObjects.
     */
    public Object compose(Map<Path,Object> decomposedObjects) {
        // assume that the first key determines the type and that the data will be self-consistent.
        // If we encounter an inconsistency then we will backtrack.
        Iterator<Map.Entry<Path,Object>> iterator = decomposedObjects.entrySet().iterator();
        if (!iterator.hasNext()) {
            return null; // no data
        }
        Map.Entry<Path,Object> firstEntry = iterator.next();
        Path firstKey = firstEntry.getKey();
        if (firstKey.isListIndex()) {
            return composeList(decomposedObjects);
        }
        return composeMap(decomposedObjects);
    }

    //////// Private Methods //////////


    private Object composeList(Map<Path,Object> decomposedObjects) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private Object composeMap(Map<Path,Object> decomposedObjects) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

}
