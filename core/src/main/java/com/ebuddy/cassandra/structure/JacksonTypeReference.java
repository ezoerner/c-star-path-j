package com.ebuddy.cassandra.structure;

import java.lang.reflect.Type;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Subclass of jackson TypeReference, used to wrap our own TypeReference and plug it into jackson.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class JacksonTypeReference<T> extends TypeReference<T> {
    private final com.ebuddy.cassandra.TypeReference<T> providedType;

    public JacksonTypeReference(com.ebuddy.cassandra.TypeReference<T> providedType) {
        this.providedType = providedType;
    }

    @Override
    public Type getType() {
        return providedType.getType();
    }
}
