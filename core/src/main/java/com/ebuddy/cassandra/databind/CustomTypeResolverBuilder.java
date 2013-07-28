package com.ebuddy.cassandra.databind;

import java.util.Collection;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder;

/**
 * Custom type resolver builder to put type information in all collections and arrays when serializing,
 * and does nothing special on deserializing.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 * @see ObjectMapper#setDefaultTyping
 */
public class CustomTypeResolverBuilder extends StdTypeResolverBuilder {

    public CustomTypeResolverBuilder() {
        init(JsonTypeInfo.Id.CLASS, null);
        inclusion(JsonTypeInfo.As.WRAPPER_ARRAY);
    }

    @Override
    public TypeDeserializer buildTypeDeserializer(DeserializationConfig config,
                                                  JavaType baseType,
                                                  Collection<NamedType> subtypes) {
        return super.buildTypeDeserializer(config, baseType, subtypes);
    }

    @Override
    public TypeSerializer buildTypeSerializer(SerializationConfig config,
                                              JavaType baseType,
                                              Collection<NamedType> subtypes) {
        return useForType(baseType) ? super.buildTypeSerializer(config, baseType, subtypes) : null;
    }

    private boolean useForType(JavaType t) {
        return t.isArrayType() || Collection.class.isAssignableFrom(t.getRawClass());
    }
}
