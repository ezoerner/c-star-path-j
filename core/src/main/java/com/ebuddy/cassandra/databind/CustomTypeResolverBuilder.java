package com.ebuddy.cassandra.databind;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
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
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.databind.util.ClassUtil;

/**
 * Custom type resolver builder to put type information in all collections and arrays.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 * @see ObjectMapper#setDefaultTyping
 */
public class CustomTypeResolverBuilder extends StdTypeResolverBuilder {

    public CustomTypeResolverBuilder() {
        init(JsonTypeInfo.Id.CLASS, new CustomTypeIdResolver());
        inclusion(JsonTypeInfo.As.WRAPPER_ARRAY);
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
        return t.isArrayType() || Collection.class.isAssignableFrom(t.getRawClass());
    }

    private class CustomTypeIdResolver implements TypeIdResolver {
        private final TypeFactory typeFactory = TypeFactory.defaultInstance();
        private JavaType baseType;

        @Override
        public void init(JavaType bt) {
            baseType = bt;
        }

        @Override
        public String idFromValue(Object value) {
            return idFrom(value, value.getClass());
        }

        @Override
        public String idFromValueAndType(Object value, Class<?> type) {
            return idFrom(value, type);
        }

        @Override
        public JavaType typeFromId(String id)
        {
        /* 30-Jan-2010, tatu: Most ids are basic class names; so let's first
         *    check if any generics info is added; and only then ask factory
         *    to do translation when necessary
         */
            if (id.indexOf('<') > 0) {
                JavaType t = typeFactory.constructFromCanonical(id);
                // note: may want to try combining with specialization (esp for EnumMap)?
                return t;
            }
            try {
                Class<?> cls =  ClassUtil.findClass(id);
                return typeFactory.constructSpecializedType(baseType, cls);
            } catch (ClassNotFoundException ignored) {
                throw new IllegalArgumentException("Invalid type id '"+id+"' (for id type 'Id.class'): no such class found");
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid type id '"+id+"' (for id type 'Id.class'): "+e.getMessage(), e);
            }
        }

        @Override
        public JsonTypeInfo.Id getMechanism() {
            return JsonTypeInfo.Id.CUSTOM;
        }

        @Override
        public String idFromBaseType()
        {
        /* By default we will just defer to regular handling, handing out the
         * base type; and since there is no value, must just pass null here
         * assuming that implementations can deal with it.
         * Alternative would be to pass a bogus Object, but that does not seem right.
         */
            return idFromValueAndType(null, baseType.getRawClass());
        }


        @SuppressWarnings("ChainOfInstanceofChecks")
        protected final String idFrom(Object value, Class<?> cls)
        {
            // [JACKSON-380] Need to ensure that "enum subtypes" work too
            if (Enum.class.isAssignableFrom(cls)) {
                if (!cls.isEnum()) { // means that it's sub-class of base enum, so:
                    cls = cls.getSuperclass();
                }
            }

            // try to get element type if this is a Set (otherwise we don't care)
            Class<?> elementType = null;
            if (value instanceof Set) {
                // if we have baseType, use that
                if (baseType != null) {
                    JavaType contentType = baseType.getContentType();
                    elementType = contentType.getRawClass();
                } else {
                    Class<?> clazz = value.getClass();
                    do {
                        Type superclassType = clazz.getGenericSuperclass();
                        if (superclassType instanceof ParameterizedType) {
                            ParameterizedType parameterizedSuperclass = (ParameterizedType)superclassType;
                            Type[] elementTypes = parameterizedSuperclass.getActualTypeArguments();
                            if (elementTypes.length == 1 && elementTypes[0] instanceof Class) {
                                elementType = (Class<?>)elementTypes[0];
                            }
                        }
                        clazz = clazz.getSuperclass();
                    } while (elementType == null && clazz != null);
                }
            }

            String str = cls.getName();
            if (str.startsWith("java.util")) {
            /* 25-Jan-2009, tatu: There are some internal classes that
             *   we can not access as is. We need better mechanism; for
             *   now this has to do...
             */
            /* Enum sets and maps are problematic since we MUST know
             * type of contained enums, to be able to deserialize.
             * In addition, EnumSet is not a concrete type either
             */
                if (value instanceof EnumSet<?>) { // Regular- and JumboEnumSet...
                    Class<?> enumClass = ClassUtil.findEnumType((EnumSet<?>) value);
                    // not optimal: but EnumSet is not a customizable type so this is sort of ok
                    str = TypeFactory.defaultInstance().constructCollectionType(EnumSet.class, enumClass).toCanonical();
                } else if (value instanceof EnumMap<?,?>) {
                    Class<?> enumClass = ClassUtil.findEnumType((EnumMap<?,?>) value);
                    Class<?> valueClass = Object.class;
                    // not optimal: but EnumMap is not a customizable type so this is sort of ok
                    str = TypeFactory.defaultInstance().constructMapType(EnumMap.class, enumClass, valueClass).toCanonical();
                } else {
                    String end = str.substring(9);
                    if ((end.startsWith(".Arrays$") || end.startsWith(".Collections$"))
                            && str.contains("List")) {
                    /* 17-Feb-2010, tatus: Another such case: result of
                     *    Arrays.asList() is named like so in Sun JDK...
                     *   Let's just plain old ArrayList in its place
                     * NOTE: chances are there are plenty of similar cases
                     * for other wrappers... (immutable, singleton, synced etc)
                     */
                        str = "java.util.ArrayList";
                    }
                }
            } else if (str.indexOf('$') >= 0) {
            /* Other special handling may be needed for inner classes, [JACKSON-584].
             * The best way to handle would be to find 'hidden' constructor; pass parent
             * value etc (which is actually done for non-anonymous static classes!),
             * but that is just not possible due to various things. So, we will instead
             * try to generalize type into something we will be more likely to be able
             * construct.
             */
                Class<?> outer = ClassUtil.getOuterClass(cls);
                if (outer != null) {
                /* one more check: let's actually not worry if the declared
                 * static type is non-static as well; if so, deserializer does
                 * have a chance at figuring it all out.
                 */
                    Class<?> staticType = baseType.getRawClass();
                    if (ClassUtil.getOuterClass(staticType) == null) {
                        // Is this always correct? Seems like it should be...
                        cls = baseType.getRawClass();
                        str = cls.getName();
                    }
                }
            }

            if (elementType != null) {
                return str + "<" + elementType.getName() + ">";
            }
            return str;
        }
    }
}
