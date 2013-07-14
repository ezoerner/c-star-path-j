package com.ebuddy.cassandra;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;


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
     * @param decomposedObjects input map of decomposed objects, paths mapped to simple values.
     *                          Sorted by the path keys to facilitate list reconstruction.
     * @return a complex object such as a map or list decoded from the paths in decomposedObjects,
     *         or null if decomposedObjects is empty.
     * @throws IllegalArgumentException if there are unsupported objects types in decomposedObjects.
     */
    public Object compose(SortedMap<Path,Object> decomposedObjects) {
        // assume that the first key determines the type and that the data will be self-consistent.
        // If we encounter an inconsistency then we will backtrack.
        Iterator<Map.Entry<Path,Object>> iterator = decomposedObjects.entrySet().iterator();
        if (!iterator.hasNext()) {
            return null; // no data
        }
        Map.Entry<Path,Object> firstEntry = iterator.next();
        Path firstKey = firstEntry.getKey();
        if (firstKey.isListIndex()) {
            List<Object> list = composeList(decomposedObjects);
            if (list != null) {
                return list;
            }
        }
        return composeMap(decomposedObjects);
    }

    //////// Private Methods //////////


    /**
     * @return null if cannot make a list from the data
     */
    private List<Object> composeList(SortedMap<Path,Object> decomposedObjects) {
        List<Object> list = new ArrayList<Object>(decomposedObjects.size());
        for (Map.Entry<Path,Object> entry : decomposedObjects.entrySet()) {
            // regardless of the actual indexes found in the paths, assign to list based on
            // sorted paths since some indexes may have been deleted. Sparse lists not supported.
            Path path = entry.getKey();
            if (!path.isListIndex()) {
                // could not get a valid index for this path (so we will make a map at this level instead)
                return null;
            }
            list.add(entry.getValue());
        }
        return list;
        // TODO: we need to advance and propagate the paths somehow...
    }

    private Object composeMap(Map<Path,Object> decomposedObjects) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

}
