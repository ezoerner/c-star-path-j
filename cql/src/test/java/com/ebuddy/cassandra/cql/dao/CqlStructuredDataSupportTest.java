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

package com.ebuddy.cassandra.cql.dao;

import static org.testng.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicLong;

import org.testng.annotations.Test;

/**
 * Unit tests for CqlStructuredDataSupport.
 * This class is mostly tested by {@link CqlStructuredDataSupportSystemTest} instead
 * of unit tests due to the difficulty of mocking out the CQL driver dependency.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class CqlStructuredDataSupportTest {

    @Test(groups = {"unit"})
    public void shouldGetCurrentMicros() throws Exception {
        AtomicLong numSameMs = new AtomicLong();
        for (int i = 0; i < 2000; i++) {
            long timeOne = CqlStructuredDataSupport.getCurrentMicros();
            long timeTwo = CqlStructuredDataSupport.getCurrentMicros();
            assertTrue(timeOne < timeTwo);
            if ((timeOne / 1000L) == (timeTwo / 1000L)) {
                numSameMs.incrementAndGet();
            }
        }
        System.out.println("number of times ms resolution conflict resolved=" + numSameMs.get());
    }
}
