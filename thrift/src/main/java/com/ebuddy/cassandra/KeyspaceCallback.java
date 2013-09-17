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

package com.ebuddy.cassandra;

import me.prettyprint.cassandra.service.KeyspaceService;

/**
 * A callback used with a CassandraTemplate when a single Keyspace is needed.
 * T is the type of result from the {@link #execute} method.
 *
 * @author Eric Zoerner
 * @see CassandraTemplate
 */
public interface KeyspaceCallback<T> {

    /**
     * Execute an operation using a Keyspace.
     *
     * @param keyspace the Keyspace
     * @return the result
     */
    T execute(KeyspaceService keyspace);
}
