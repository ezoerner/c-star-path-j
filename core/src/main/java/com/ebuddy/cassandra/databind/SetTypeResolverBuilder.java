package com.ebuddy.cassandra.databind;

import java.util.Collection;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.SimpleType;

/**
 * // TODO: Add class description here.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 * @see ObjectMapper#setDefaultTyping
 */
public class SetTypeResolverBuilder extends StdTypeResolverBuilder {

    public static final CollectionType SET_TYPE = CollectionType.construct(Set.class,
                                                                           SimpleType.construct(Object.class));

    public SetTypeResolverBuilder() {
        init(JsonTypeInfo.Id.NAME, new CustomTypeIdResolver());
        inclusion(JsonTypeInfo.As.PROPERTY);
        typeProperty("@type");
    }

    @Override
    public TypeDeserializer buildTypeDeserializer(DeserializationConfig config,
                                                  JavaType baseType,
                                                  Collection<NamedType> subtypes) {
        return useForType(baseType) ? super.buildTypeDeserializer(config, baseType, subtypes) : null;
    }

    @Override
    public TypeSerializer buildTypeSerializer(SerializationConfig config,
                                              JavaType baseType,
                                              Collection<NamedType> subtypes) {
        return useForType(baseType) ? super.buildTypeSerializer(config, baseType, subtypes) : null;
    }

    private boolean useForType(JavaType t) {
        return Set.class.isAssignableFrom(t.getRawClass());
    }

    private static class CustomTypeIdResolver implements TypeIdResolver {

        public static final String TYPE_SET = "Set";

        @Override
        public void init(JavaType baseType) {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        @Override
        public String idFromValue(Object value) {
            return TYPE_SET;
        }

        @Override
        public String idFromValueAndType(Object value, Class<?> suggestedType) {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        @Override
        public String idFromBaseType() {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        @Override
        public JavaType typeFromId(String id) {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        @Override
        public JsonTypeInfo.Id getMechanism() {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }
}
