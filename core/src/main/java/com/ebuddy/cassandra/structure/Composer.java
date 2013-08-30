package com.ebuddy.cassandra.structure;

import static java.util.AbstractMap.SimpleEntry;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;


/**
 * Support for composing paths back to complex objects.
 * Only the basic JSON structures are supported, i.e. Maps, Lists, Strings, Numbers, Booleans, and null.
 *
 * It is possible to write data that will cause inconsistencies in an object structure
 * when it is reconstructed. This implementation will resolve inconsistencies as follows:
 *
 * If data objects are found at a particular path as well as longer paths, the data object
 * is returned in a map structure with the special key "@ROOT". This may cause an error
 * if the data is later attempted to be deserialized into a POJO.
 *
 * If list elements are found at the same level as longer paths or a data object, then
 * the list elements are returned in a map with the index as keys in the map, e.g. "@0", "@1",
 * etc.
 *
 * If inconsistencies such as these are preventing data from being deserialized into a
 * particular POJO, the data can always be retrieved using an instance of (a sublcass of) TypeReference<Object>,
 * which will return the basic JSON to Java mappings, i.e. Maps, Lists and Strings, etc.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class Composer {
    private static final String INCONSISTENT_ROOT = "@ROOT";
    private static final Composer INSTANCE = new Composer();

    private Composer() { }

    public static Composer get() {
        return INSTANCE;
    }

    /**
     * Compose a map of simple objects keyed by paths into a single complex object, e.g. a map or list
     *
     * @param simpleObjects input map of decomposed objects, paths mapped to simple values (i.e. strings, numbers, or booleans)
     * @return a complex object such as a map or list decoded from the paths in decomposedObjects,
     *         or null if decomposedObjects is empty.
     * @throws IllegalArgumentException if there are unsupported objects types in decomposedObjects, or
     * if there is a key that is an empty path
     */
    public Object compose(Map<Path,Object> simpleObjects) {
        if (simpleObjects == null) {
            throw new IllegalArgumentException("simpleObject is null");
        }

        if (simpleObjects.isEmpty()) {
            return Collections.emptyMap();
        }

        // decompose into nested maps by merging the partial map from each path.
        // After composing into nested maps, go through the tree structure and transform SortedMaps into Lists.
        // The reason for a two-pass approach is that the lists may be "sparse" due to deleted indexes, and
        // this is difficult to handle in one pass.
        return transformLists(composeMap(simpleObjects));
    }

    private Map<String,Object> composeMap(Map<Path,Object> simpleObjects) {
        Map<String,Object> composition = new LinkedHashMap<String,Object>(simpleObjects.size());
        for (Map.Entry<Path,Object> entry : simpleObjects.entrySet()) {
            merge(entry, composition);
        }
        return composition;
    }

    @SuppressWarnings("unchecked")
    private void merge(Map.Entry<Path,Object> simpleEntry, Map<String,Object> compositionMap) {
        Path path = simpleEntry.getKey();
        String head = path.first();
        assert head != null;
        Object nextLevelComposition = compositionMap.get(head);
        Path rest = path.rest();
        Object simpleValue = simpleEntry.getValue();
        if (nextLevelComposition == null) {
            mergeEntryIntoEmptySlot(compositionMap, head, rest, simpleValue);
        } else if (Types.isSimple(nextLevelComposition)) {
            mergeEntryWithSimple(compositionMap, nextLevelComposition, head, rest, simpleValue);
        } else {
            mergeEntryWithStructure(compositionMap, (Map<String,Object>)nextLevelComposition, rest, simpleValue);
        }
    }

    private void mergeEntryWithStructure(Map<String,Object> composition,
                                         Map<String,Object> nextLevelComposition,
                                         Path rest,
                                         Object simpleValue) {
        if (rest.isEmpty()) {
            // INCONSISTENCY!! there is a simple value at the same level as a complex object
            // Resolve this by putting this value at the special key "@ROOT".
            composition.put(INCONSISTENT_ROOT, simpleValue);
        } else {
            // simply advance to next level since the first matches a key already there
            merge(new SimpleEntry<Path, Object>(rest, simpleValue), nextLevelComposition);
        }
    }

    private void mergeEntryIntoEmptySlot(Map<String,Object> composition, String head, Path rest, Object simpleValue) {
        if (rest.isEmpty()) {
            composition.put(head, simpleValue);
        } else {
            composition.put(head, composeMap(Collections.singletonMap(rest, simpleValue)));
        }
    }

    private void mergeEntryWithSimple(Map<String,Object> composition,
                                      Object nextLevelComposition,
                                      String head,
                                      Path rest,
                                      Object simpleValue) {
        if (rest.isEmpty()) {
            // merging two simple values at same level, this cannot happen because map keys are unique
            throw new IllegalStateException("two simple values at same level");
        }

        // merging longer path with simple value
        composition.put(INCONSISTENT_ROOT, nextLevelComposition);
        composition.put(head, composeMap(Collections.singletonMap(rest, simpleValue)));
    }

    @SuppressWarnings("unchecked")
    private Object transformLists(Map<String,Object> map) {
        // go through nested maps and transform maps into lists where possible
        if (Path.isList(map)) {
            return transformActualList(map);
        }
        // if not a list, then just recursively transform the structure, and also URLDecode the keys
        Map<String,Object> newMap = new HashMap<String,Object>(map.size());
        for (Map.Entry<String,Object> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map) {
                Object transformedValue = transformLists((Map<String,Object>)value);
                newMap.put(urlDecode(entry.getKey()), transformedValue);
            } else if (Types.isSimple(value)) {
                newMap.put(urlDecode(entry.getKey()), entry.getValue());
            } else {
                throw new IllegalStateException("found strange object in structure: " + value);
            }
        }
        return newMap;
    }

    @SuppressWarnings("unchecked")
    private Object transformActualList(Map<String,Object> map) {
        SortedMap<Integer,Object> sortedMap = new TreeMap<Integer,Object>();
        List<Object> list = new ArrayList<Object>(map.size());

        int listSize = -1;

        // convert keys into integer indexes and sort
        for (Map.Entry<String,Object> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (Types.isListTerminator(value)) {
                listSize = Path.getListIndex(entry.getKey());
                continue;
            }

            Object transformedValue;
            if (Types.isSimple(value)) {
                transformedValue = value;
            } else if (value instanceof Map) {
                transformedValue = transformLists((Map<String,Object>)value);
            } else {
                throw new IllegalStateException("found strange object in structure: " + value);
            }
            sortedMap.put(Path.getListIndex(entry.getKey()), transformedValue);
        }

        // if no listSize was found then something went wrong, but just warn and use whole list found
        if (listSize == -1) {
            // TODO: what log framework to use?
            // log.warn("no list terminator found, using all list elements");
        }

        // copy values into list in sorted order
        int index = 0;
        for (Object value : sortedMap.values()) {
            if (index == listSize) {
                break;
            }
            list.add(value);
            index++;
        }
        return list;
    }

    private String urlDecode(String head) {
        try {
            head = URLDecoder.decode(head, "UTF-8");
        } catch (UnsupportedEncodingException ignored) {
            throw new AssertionError("UTF-8 is unknown");
        }
        return head;
    }
}
