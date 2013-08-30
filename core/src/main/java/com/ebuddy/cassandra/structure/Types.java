package com.ebuddy.cassandra.structure;

/**
 * Utility methods for working with types within structured objects.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
class Types {
    // "reserved" unicode characters-- don't use \uFFFF alone since that is used already, see StructureConverter
    static final String LIST_TERMINATOR_VALUE = "\uFFFF\uFFFF";

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

    static boolean isListTerminator(Object value) {
        return value.equals(LIST_TERMINATOR_VALUE);
    }
}
