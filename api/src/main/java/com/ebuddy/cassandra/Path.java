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

package com.ebuddy.cassandra;

import java.util.List;

/**
 * A path used as for accessing and querying elements of a structured object.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public interface Path {

    /** Return a new Path consisting of this Path concatenated with another Path. */
    Path concat(Path other);

    /** Return a new Path consisting of this Path concatenated with the specified list indices as elements. */
    Path withIndices(int... indices);

    /** Return a new Path consisting of this Path concatenated with the specified elements. */
    Path withElements(String... elements);

    /** Get the encoded elements of this Path. */
    List<String> getElements();

    /**
     * Returns the first element in this path, or null if this is an empty path.
     */
    String head();

    /**
     * Returns the rest of the path after the head (first) element.
     * If this is an empty path, throws IndexOutOfBoundsException,
     * if this path has only one element, return an empty path,
     * otherwise return a new path with elements starting after the head.
     */
    Path tail();

    /**
     * Return a new Path consisting of the rest of the path elements of this path starting with the specified index.
     * @param startIndex 0-based start index
     * @return new Path
     * @throws IndexOutOfBoundsException if path has insufficient size
     */
    Path tail(int startIndex);

    /**
     * Return true if this path starts with the specified path.
     */
    boolean startsWith(Path path);

    /** Get the number of elements in this path. */
    int size();

    /** Return true if this Path has zero elements. */
    boolean isEmpty();
}
