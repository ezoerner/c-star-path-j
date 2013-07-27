package com.ebuddy.cassandra.databind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;
import com.fasterxml.jackson.databind.ser.std.StdDelegatingSerializer;
import com.fasterxml.jackson.databind.type.MapLikeType;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.databind.util.Converter;

/**
 * // TODO: Add class description here.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class CustomBeanSerializerFactory extends BeanSerializerFactory {

    public CustomBeanSerializerFactory() {
        super(null);
    }

    @Override
    protected JsonSerializer<?> buildContainerSerializer(SerializerProvider prov,
                                                         final JavaType type,
                                                         BeanDescription beanDesc,
                                                         boolean staticTyping) throws JsonMappingException {
        SerializationConfig config = prov.getConfig();

        /* [Issue#23], 15-Mar-2013, tatu: must force static handling of root value type,
         *   with just one important exception: if value type is "untyped", let's
         *   leave it as is; no clean way to make it work.
         */
        if (!staticTyping && type.useStaticType()) {
            if (!type.isContainerType() || type.getContentType().getRawClass() != Object.class) {
                staticTyping = true;
            }
        }

        // Let's see what we can learn about element/content/value type, type serializer for it:
        JavaType elementType = type.getContentType();
        TypeSerializer elementTypeSerializer = createTypeSerializer(config,
                                                                    elementType);

        // if elements have type serializer, can not force static typing:
        if (elementTypeSerializer != null) {
            staticTyping = false;
        }
        JsonSerializer<Object> elementValueSerializer = _findContentSerializer(prov,
                                                                               beanDesc.getClassInfo());

        if (Set.class.isAssignableFrom(type.getRawClass())) {
            final MapLikeType mlt = TypeFactory.defaultInstance().constructMapType(HashMap.class, String.class, Object.class);

            /* 29-Sep-2012, tatu: This is actually too early to (try to) find
             *  key serializer from property annotations, and can lead to caching
             *  issues (see [Issue#75]). Instead, must be done from 'createContextual()' call.
             *  But we do need to check class annotations.
             */
            JsonSerializer<Object> keySerializer = _findKeySerializer(prov, beanDesc.getClassInfo());
            JsonSerializer<?> mapSerializer = buildMapSerializer(config, (MapType) mlt, beanDesc, staticTyping,
                                      keySerializer, elementTypeSerializer, elementValueSerializer);
            Converter<Object,Map<String,Object>> setMapConverter = new Converter<Object,Map<String,Object>>() {
                @SuppressWarnings("unchecked")
                @Override
                public Map<String,Object> convert(Object value) {
                    Map<String,Object> map = new HashMap<String,Object>();
                    map.put("@type", "Set");
                    map.put("@value", new ArrayList((Collection<?>)value));
                    return map;
                }

                @Override
                public JavaType getInputType(TypeFactory typeFactory) {
                    return type;
                }

                @Override
                public JavaType getOutputType(TypeFactory typeFactory) {
                    return mlt;
                }
            };
            StdDelegatingSerializer delegatingSerializer = new StdDelegatingSerializer(setMapConverter, type, mapSerializer);
            return delegatingSerializer;
        }
        return super.buildContainerSerializer(prov, type, beanDesc, staticTyping);
    }

}
