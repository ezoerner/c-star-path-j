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
