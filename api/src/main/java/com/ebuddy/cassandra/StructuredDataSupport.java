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

/**
 * <p>
 * Provides data access for structured objects in Cassandra.
 * Objects are read using a hierarchical path that must contain at least one path element.
 * When reading values, they will be constructed as the specified type in a TypeReference.
 * Any object that can be converted to JSON can be written to a path. Jackson annotations can be used
 * to customize how the object is converted. See <a href="http://wiki.fasterxml.com/JacksonHome">Jackson JSON Processor</a>.
 * </p>
 *
 * Paths that refer to list elements use a special notation, the '@' symbol followed by
 * the element index within the list (as a string).
 *
 * @param <K> The row key type
 * @see <a href="http://wiki.fasterxml.com/JacksonHome">Jackson JSON Processor</a>
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com>Jackson </a>
 */
public interface StructuredDataSupport<K> {

    /**
     * Begin a batch operation. Returns a BatchContext object which must be passed to all mutating methods that
     * participate in this batch operation, and then is passed to applyBatch to execute the batch.
     * @return the BatchContext
     */
    BatchContext beginBatch();

    /**
     * Execute the batch.
     * @param batchContext The BatchContext for this set of mutating operations to execute
     */
    void applyBatch(BatchContext batchContext);

    /**
     * Read an object from a path in the database.
     * @param rowKey the row key for the object to be read
     * @param path the path to the object to be read
     * @param type a TypeReference for the type of object to be returned
     * @param <T> the type of the object to be returned
     * @return the object of type T, or null if not found
     * @throws IllegalArgumentException if path is empty or contains any of the special characters '@#'
     */
    <T> T readFromPath(K rowKey, Path path, TypeReference<T> type);

    /**
     * Write an object to a path in the database.
     * @param rowKey the row key for the object to be written
     * @param path the path to the object to be written
     * @param value the Object to be written
     */
    void writeToPath(K rowKey, Path path, Object value);

    /**
     * Write an object to a path in the database as part of a batch operation.
     * @param rowKey the row key for the object to be written
     * @param path the path to the object to be written
     * @param value the Object to be written
     * @param batchContext the BatchContext that this write operation should participate in
     */
    void writeToPath(K rowKey, Path path, Object value, BatchContext batchContext);

    /**
     * Delete the values found at the specified path.
     * @param rowKey the row key for the object to be deleted
     * @param path the path to the object to be deleted
     */
    void deletePath(K rowKey, Path path);

    /**
     * Delete the values found at the specified path as part of a batch operation.
     * @param rowKey the row key for the object to be deleted
     * @param path the path to the object to be deleted
     * @param batchContext the BatchContext that this delete operation should participate in
     */
    void deletePath(K rowKey, Path path, BatchContext batchContext);

    /**
     * Utility method for creating a Path from individual string elements.
     */
    Path createPath(String... elements);
}
