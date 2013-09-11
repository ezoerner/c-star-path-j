package com.ebuddy.cassandra;

import java.util.List;

/**
 * A path used as for accessing and querying elements of a structured object.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public interface Path {

    Path concat(Path other);

    List<String> getElements();

    Path withListIndexes(int... listIndex);

    Path withElements(String... elements);

    String head();

    Path tail();

    Path tail(int startIndex);

    boolean startsWith(Path path);

    int size();

    boolean isEmpty();
}
