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
    private static final char LIST_INDEX_CHAR = '@';

    private final String[] pathElements;

    public Path(Path parent, String child) {
        int parentLength = parent.pathElements.length;
        pathElements = new String[parentLength + 1];
        System.arraycopy(parent.pathElements, 0, pathElements, 0, parentLength);
        pathElements[parentLength] = child;
    }

    public Path with(String child) {
        return new Path(this, child);
    }

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

    public static String listIndex(int i) {
        return String.valueOf(LIST_INDEX_CHAR) + i;
    }
}
