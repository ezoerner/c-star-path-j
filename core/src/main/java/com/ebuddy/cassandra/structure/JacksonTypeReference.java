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
