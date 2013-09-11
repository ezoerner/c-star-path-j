package com.ebuddy.cassandra;

import java.util.List;

/**
 * A path used as for accessing and querying elements of a structured object.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public interface Path {
    boolean isEmpty();

    Path concatenate(Path other);

    String head();

    Path tail();

    Path tail(int startIndex);

    boolean startsWith(Path path);

    int size();

    List<String> getPathElements();
}
