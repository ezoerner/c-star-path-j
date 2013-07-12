package com.ebuddy.cassandra;

import java.util.Arrays;
import java.util.Iterator;

/**
 * // TODO: Add class description here.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class Path {
    private static final char PATH_DELIMITER_CHAR = '|';
    private static final String LIST_INDEX_PREFIX = "@";

    private final String[] pathElements;

    public Path(String string) {
        this(new String[]{string});
    }

    private Path(String[] pathElements) {
        this.pathElements = pathElements;
    }

    public Path concatenate(Path other) {
        String[] newPathElements = new String[pathElements.length + other.pathElements.length];
        System.arraycopy(pathElements, 0, newPathElements, 0, pathElements.length);
        System.arraycopy(other.pathElements, 0, newPathElements, pathElements.length, other.pathElements.length);
        return new Path(newPathElements);
    }

    /**
     * Return true if the first element in this path is a list index.
     */
    public boolean isListIndex() {
        return pathElements[0].startsWith(LIST_INDEX_PREFIX);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        Iterator<String> itr = Arrays.asList(pathElements).iterator();
        while (itr.hasNext()) {
            String pathElement = itr.next();
            builder.append(pathElement);
            if (itr.hasNext()) {
                builder.append(PATH_DELIMITER_CHAR);
            }
        }
        return builder.toString();
    }

    public static Path fromIndex(int i) {
        return new Path(LIST_INDEX_PREFIX + i);
    }

    public static Path fromObject(Object o) {
        if (o instanceof Path) {
            return (Path)o;
        }
        return new Path(o.toString());
    }
}
