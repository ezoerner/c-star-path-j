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

package com.ebuddy.cassandra.property;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nonnull;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
@Deprecated
public class ListValue extends StructuredValue<List<String>> {
    private static final Logger LOG = LoggerFactory.getLogger(ListValue.class);

    /** header char, a unicode non-character, used to flag a binary encoded StructuredValue. */
    static final int HEADER_CHAR = '\uFFFE';
    /** utf-8 encoded bytes for HEADER_CHAR. */
    static final byte[] UTF8_HEADER_BYTES = {(byte)0xef, (byte)0xbf, (byte)0xbe};
    
    ListValue(@Nonnull List<String> value) {
        super(value);
    }

    ListValue(String jsonString) {
        super(jsonString);
    }

    @Override
    protected byte[] getUtf8HeaderBytes() {
        return UTF8_HEADER_BYTES;
    }

    @Override
    public boolean isList() {
        return true;
    }

    @Override
    public List<String> getValue() {
        if (value != null) {
            return value;
        }

        TypeReference<List<String>> ref = new TypeReference<List<String>>() { };
        try {
            value = JSON_MAPPER.readValue(jsonString, ref);
            return value;
        } catch (JsonParseException e) {
            LOG.error("Unable to parse '" + jsonString + "', returning null", e);
            return null;
        } catch (IOException ioe) {
            throw new RuntimeException("unexpected on '" + jsonString + "'", ioe);
        }
    }

    @Override
    public boolean isEmpty() {
        return value.isEmpty();
    }
}
