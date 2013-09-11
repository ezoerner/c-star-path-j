package com.ebuddy.cassandra.structure;

import static org.apache.commons.lang3.ObjectUtils.NULL;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ebuddy.cassandra.Path;

/**
 * Support for decomposing complex objects into paths to simple objects.
 * Only the basic JSON structures are currently supported, i.e. Maps, Lists, Strings, Numbers, Booleans, and null.
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

            // handle null specially by replacing with a Null token
            if (structure == null) {
                decomposed.put(path, NULL);
                continue;
            }

            if (Types.isSimple(structure)) {
                decomposed.put(path, structure);
                continue;
            }

            Map<Path,Object> decomposedMap = decomposeStructure(structure);

            for (Map.Entry<Path,Object> decomposedEntry : decomposedMap.entrySet()) {
                decomposed.put(path.concat(decomposedEntry.getKey()), decomposedEntry.getValue());
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
            if (!Types.isSimple(key)) {
                throw new IllegalArgumentException(String.format("map key of type %s not supported",
                                                                 key.getClass().getSimpleName()));
            }
            Path keyPath = key instanceof Path ? (Path)key : DefaultPath.fromString(urlEncode(key));

            Object value = entry.getValue();
            normalized.put(keyPath, value);
        }
        return normalized;
    }

    private String urlEncode(Object key) {
        String keyString;
        try {
             keyString = URLEncoder.encode(key.toString(), "UTF-8");
        } catch (UnsupportedEncodingException ignored) {
            throw new AssertionError("UTF-8 is unknown");
        }
        return keyString;
    }

    private Map<Path,Object> normalizeList(List<?> list) {
        // get type info for list
        // TODO: if this is a set of simple types, then encode the set into the keys using #
        String type = (String)list.get(0);
        /// get list itself
        List<?> listItself = (List<?>)list.get(1);

        Map<Path,Object> normalized = new HashMap<Path,Object>(listItself.size());
        for (int i = 0; i < listItself.size(); i++) {
            normalized.put(DefaultPath.fromIndex(i), listItself.get(i));
        }
        // add terminator column, issue #20
        normalized.put(DefaultPath.fromIndex(listItself.size()), Types.LIST_TERMINATOR_VALUE);

        return normalized;
    }
}
