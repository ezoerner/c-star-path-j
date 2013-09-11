package com.ebuddy.cassandra.structure;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import com.ebuddy.cassandra.Path;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * Implementation of Path used as column names in Cassandra for encoding structures and for querying elements
 * of a structured object.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class DefaultPath implements Path {
    private static final char PATH_DELIMITER_CHAR = '/';
    private static final String LIST_INDEX_PREFIX = "@";

    private static final Function<String,String> urlEncodeFunction = new UrlEncode();

    private final List<String> pathElements;


    /** Create a DefaultPath from a list of encoded path element strings. */
    private DefaultPath(List<String> encodedPathElements) {
        pathElements = encodedPathElements;
    }

    /** Create a DefaultPath from an array of encoded path element strings. */
    private DefaultPath(String... encodedPathElements) {
        this(Arrays.asList(encodedPathElements));
    }

    /** Create a DefaultPath from un-encoded string elements. */
    public static DefaultPath fromStrings(String... elements) {
        return new DefaultPath(Lists.transform(Arrays.asList(elements), urlEncodeFunction));
    }

    @Override
    public Path concat(Path other) {
        List<String> newPathElements = new LinkedList<String>();
        newPathElements.addAll(pathElements);
        newPathElements.addAll(other.getElements());
        return new DefaultPath(newPathElements);
    }

    /**
     * Paths always end with the delimiter character in order to facilitate
     * start/finish slice queries in Cassandra.
     * @return the (encoded) String representation of a path.
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

    public static DefaultPath fromIndex(int i) {
        return new DefaultPath(Arrays.asList(LIST_INDEX_PREFIX + i));
    }

    @Override
    public String head() {
        return pathElements.size() == 0 ? null : pathElements.get(0);
    }

    @Override
    public DefaultPath tail() {
        return tail(1);
    }

    @Override
    public DefaultPath tail(int startIndex) {
        return new DefaultPath(pathElements.subList(startIndex, pathElements.size()));
    }

    @Override
    public boolean isEmpty() {
        return pathElements.size() == 0;
    }

    @Override
    public boolean startsWith(Path path) {
        return pathElements.subList(0, path.size()).equals(path.getElements());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return pathElements.equals(((DefaultPath)o).pathElements);

    }

    @Override
    public int hashCode() {
        return pathElements.hashCode();
    }

    @Override
    public int size() {
        return pathElements.size();
    }

    @Override
    public List<String> getElements() {
        return Collections.unmodifiableList(pathElements);
    }

    @Override
    public Path withIndices(int... indices) {
        List<String> newPathElements = new ArrayList<String>(pathElements.size() + indices.length);
        newPathElements.addAll(pathElements);
        for (int index : indices) {
            newPathElements.add(LIST_INDEX_PREFIX + index);
        }
        return new DefaultPath(newPathElements);
    }

    @Override
    public Path withElements(String... elements) {
        List<String> newPathElements = new ArrayList<String>(pathElements.size() + elements.length);
        newPathElements.addAll(pathElements);
        newPathElements.addAll(Lists.transform(Arrays.asList(elements), urlEncodeFunction));
        return new DefaultPath(newPathElements);
    }

    /** Create a Path object from a String produced by the Path#toString method. */

    public static Path fromEncodedPathString(String pathString) {
        String[] parts = StringUtils.split(pathString, PATH_DELIMITER_CHAR);
        return new DefaultPath(parts);
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
     * Return true if all the given encoded path elements are list indexes.
     * @throws IllegalArgumentException if an empty path is found
     */
    public static boolean isList(Iterable<String> encodedElements) {
        for (String element : encodedElements)  {
            if (!isListIndex(element)) {
                return false;
            }
        }
        return true;
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

    private static class UrlEncode implements Function<String,String> {
        @Nullable
        @Override
        public String apply(@Nullable String s) {
            if (s == null) {
                return null;
            }
            String encodedString;
            try {
                encodedString = URLEncoder.encode(s, "UTF-8");
            } catch (UnsupportedEncodingException ignored) {
                throw new AssertionError("UTF-8 is unknown?");
            }
            return encodedString;
        }
    }
}
