package com.ebuddy.cassandra.structure;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * A path used as column names in Cassandra for encoding structures and for querying elements of a structured object.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class Path implements Comparable<Path> {
    private static final char PATH_DELIMITER_CHAR = '/';
    private static final String LIST_INDEX_PREFIX = "@";

    private final List<String> pathElements;

    /** initialize a single path element */
    // private so clients don't accidently pass in a path string here
    // client should use the static factory methods to construct a path
    private Path(String string) {
        this(Arrays.asList(string));
    }

    private Path(List<String> pathElements) {
        this.pathElements = pathElements;
    }

    public Path concatenate(Path other) {
        List<String> newPathElements = new LinkedList<String>();
        newPathElements.addAll(pathElements);
        newPathElements.addAll(other.pathElements);
        return new Path(newPathElements);
    }

    /**
     * Paths always end with the delimiter character in order to facilitate
     * start/finish slice queries in Cassandra.
     * @return the String representation of a path.
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (String pathElement : pathElements) {
            builder.append(pathElement);
            builder.append(PATH_DELIMITER_CHAR);
        }
        return builder.toString();
    }

    public static Path fromIndex(int i) {
        return new Path(LIST_INDEX_PREFIX + i);
    }


    /** Create a Path object given a path string. */
    public static Path fromString(String pathString) {
        return fromString(pathString, PATH_DELIMITER_CHAR);
    }

    // used for backward compatibility to specify the vertical bar as a delimiter
    public static Path fromString(String pathString, char delmiterChar) {
        String[] parts = StringUtils.split(pathString, delmiterChar);
        return new Path(Arrays.asList(parts));
    }

    @Override
    public int compareTo(Path o) {
        return toString().compareTo(o.toString());
    }

    /**
     * Return true if all the keys in decomposedObjects are list indexes.
     * @throws IllegalArgumentException if an empty path is found
     */
    public static boolean isList(Map<String,Object> decomposedObjects) {
        for (String key : decomposedObjects.keySet())  {
            if (!isListIndex(key)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the first element in this path.
     * If this is an empty path, return null, otherwise return the first element in the path.
     */
    public String head() {
        return pathElements.size() == 0 ? null : pathElements.get(0);
    }

    /**
     * Returns the rest of the path after the head.
     * If this is an empty path, return null,
     * if this path has only one element, return an empty path,
     * otherwise return a new path with elements starting after the head.
     */
    public Path rest() {
        return pathElements.size() == 0 ? null : new Path(pathElements.subList(1, pathElements.size()));
    }

    public boolean isEmpty() {
        return pathElements.size() == 0;
    }

    public static int getListIndex(String pathElement) {
        if (pathElement.isEmpty()) {
            throw new IllegalStateException("empty path");
        }

        if (!pathElement.startsWith(LIST_INDEX_PREFIX)) {
            throw new IllegalStateException("not a list index");
        }

        String rest = pathElement.substring(LIST_INDEX_PREFIX.length());
        try {
            return Integer.parseInt(rest);
        } catch (NumberFormatException ignored) {
            throw new IllegalStateException("bad format for list index");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return pathElements.equals(((Path)o).pathElements);

    }

    @Override
    public int hashCode() {
        return pathElements.hashCode();
    }

    public int size() {
        return pathElements.size();
    }

    /** (package protected) Create a single element path from an object. */
    static Path fromObject(Object o) {
        if (o instanceof Path) {
            return (Path)o;
        }
        return new Path(o.toString());
    }

    /**
     * Return true if the head element in this path is a list index.
     */
    private static boolean isListIndex(String pathElement) {
        if (!pathElement.startsWith(LIST_INDEX_PREFIX)) {
            return false;
        }
        String rest = pathElement.substring(LIST_INDEX_PREFIX.length());
        int index;
        try {
            index = Integer.parseInt(rest);
        } catch (NumberFormatException ignored) {
            return false;
        }
        return index >= 0;
    }
}
