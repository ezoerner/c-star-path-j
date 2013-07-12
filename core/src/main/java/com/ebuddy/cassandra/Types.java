package com.ebuddy.cassandra;

/**
 * // TODO: Add class description here.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
class Types {
    private static final Class<?>[] simpleTypes = new Class<?>[]{
            String.class, Number.class, Boolean.class
    };

    private Types() { }


    static boolean isSimple(Object object) {
        if (object == null) {
            return true;
        }
        for (Class<?> type : simpleTypes) {
            if (type.isInstance(object)) {
                return true;
            }
        }
        return false;
    }
}
