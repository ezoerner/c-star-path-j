package com.ebuddy.cassandra;

import java.util.Arrays;

/**
 * // TODO: Add class description here.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class Path implements Comparable<Path> {
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
     * Return true if the head element in this path is a list index.
     */
    public boolean isListIndex() {
        String head = pathElements[0];
        if (!head.startsWith(LIST_INDEX_PREFIX)) {
            return false;
        }
        String rest = head.substring(LIST_INDEX_PREFIX.length());
        int index;
        try {
            index = Integer.parseInt(rest);
        } catch (NumberFormatException ignored) {
            return false;
        }
        return index >= 0;
    }

    /**
     * Paths always end with the delimiter character in order to facilitate
     * start/finish slice queries in Cassandra.
     * @return the String representation of a path.
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (String pathElement : Arrays.asList(pathElements)) {
            builder.append(pathElement);
            builder.append(PATH_DELIMITER_CHAR);
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

    @Override
    public int compareTo(Path o) {
        return toString().compareTo(o.toString());
    }
}
