/*
 * Copyright 2013 eBuddy B.V.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.ebuddy.cassandra.structure;

import static com.google.common.collect.Iterables.elementsEqual;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Iterables.skip;
import static com.google.common.collect.Iterables.unmodifiableIterable;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import com.ebuddy.cassandra.Path;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
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

    private final Iterable<String> pathElements;


    /** Create a DefaultPath from an iterable of encoded path element strings. */
    private DefaultPath(Iterable<String> encodedPathElements) {
        pathElements = encodedPathElements;
    }

    /** Create a DefaultPath from un-encoded string elements. */
    public static DefaultPath fromStrings(String... elements) {
        return new DefaultPath(Lists.transform(Arrays.asList(elements), urlEncodeFunction));
    }

    @Override
    public Path concat(Path other) {
        Iterable<String> newPathElements = Iterables.concat(pathElements, other.getElements());
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
        return getFirst(pathElements, null);
    }

    @Override
    public DefaultPath tail() {
        return tail(1);
    }

    @Override
    public DefaultPath tail(int startIndex) {
        return new DefaultPath(skip(pathElements, startIndex));
    }

    @Override
    public boolean isEmpty() {
        return Iterables.isEmpty(pathElements);
    }

    @Override
    public boolean startsWith(Path path) {
        return elementsEqual(limit(pathElements, path.size()),
                             path.getElements());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return elementsEqual(pathElements, ((DefaultPath)o).pathElements);
    }

    @Override
    public int hashCode() {
        return Lists.newArrayList(pathElements).hashCode();
    }

    @Override
    public int size() {
        return Iterables.size(pathElements);
    }

    @Override
    public Iterable<String> getElements() {
        return unmodifiableIterable(pathElements);
    }

    @Override
    public Path withIndices(int... indices) {
        List<String> newPathElements = Lists.newArrayList(pathElements);
        for (int index : indices) {
            newPathElements.add(LIST_INDEX_PREFIX + index);
        }
        return new DefaultPath(newPathElements);
    }

    @Override
    public Path withElements(String... elements) {
        List<String> newPathElements = Lists.newArrayList(pathElements);
        newPathElements.addAll(Lists.transform(Arrays.asList(elements), urlEncodeFunction));
        return new DefaultPath(newPathElements);
    }

    /** Create a Path object from a String produced by the Path#toString method. */

    public static Path fromEncodedPathString(String pathString) {
        Iterable<String> parts = Splitter.on(PATH_DELIMITER_CHAR).omitEmptyStrings().split(pathString);
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
