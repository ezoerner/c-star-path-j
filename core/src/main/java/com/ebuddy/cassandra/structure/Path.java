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

    /** Create a path from a list of path element strings. */
    private Path(List<String> pathElements) {
        this.pathElements = pathElements;
    }

    public static Path fromElements(String... elements) {
        return new Path(Arrays.asList(elements));
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
        return new Path(Arrays.asList(LIST_INDEX_PREFIX + i));
    }


    /** Create a Path object given a path string. */
    public static Path fromString(String pathString) {
        return fromString(pathString, PATH_DELIMITER_CHAR);
    }

    // used for backward compatibility to specify the vertical bar as a delimiter
    public static Path fromString(String pathString, char delimiterChar) {
        String[] parts = StringUtils.split(pathString, delimiterChar);
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
    public String first() {
        return pathElements.size() == 0 ? null : pathElements.get(0);
    }

    /**
     * Returns the rest of the path after the first element.
     * If this is an empty path, throws IndexOutOfBoundsException,
     * if this path has only one element, return an empty path,
     * otherwise return a new path with elements starting after the first.
     */
    public Path tail() {
        return tail(1);
    }

    /**
     * Return a new Path consisting of the rest of the path elements of this path starting with the specified index.
     * @param startIndex 0-based start index
     * @return new Path
     * @throws IndexOutOfBoundsException if path has insufficient size
     */
    public Path tail(int startIndex) {
        return new Path(pathElements.subList(startIndex, pathElements.size()));
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

    /**
     * Return true if this path starts with the specified path.
     */
    public boolean startsWith(Path path) {
        return pathElements.subList(0, path.size()).equals(path.pathElements);
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

    /**
     * Return true if the first element in this path is a list index.
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
