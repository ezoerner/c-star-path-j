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

package com.ebuddy.cassandra.dao;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.Validate;

import com.ebuddy.cassandra.BatchContext;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * Template for a keyspace. Implements methods that operate on a Keyspace.
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */

// See comment in AbstractColumnFamilyTemplate.
public class KeyspaceTemplate<K> implements KeyspaceOperations {

    /**
     * The keyspace for operations.
     */
    private final Keyspace keyspace;
    /**
     * The serializer for row keys.
     */
    private final Serializer<K> keySerializer;

    public KeyspaceTemplate(Keyspace keyspace, Serializer<K> keySerializer) {
        if (keySerializer == null) {
            throw new IllegalArgumentException("keySerializer is null");
        }
        if (keyspace == null) {
            throw new IllegalArgumentException("keyspace is null");
        }
        this.keyspace = keyspace;
        this.keySerializer = keySerializer;
    }

    /**
     * Create a BatchContext for use with batch operations.
     */
    @Override
    public final BatchContext begin() {
        return new HectorBatchContext(keyspace, keySerializer);
    }

    /**
     * Execute a batch.
     *
     * @param txnContext the  BatchContext
     */
    @Override
    public final void commit(@Nonnull BatchContext txnContext) {
        Mutator<K> mutator = validateAndGetMutator(txnContext);
        Validate.notNull(mutator);

        // could translate the hector exception here in a try/catch to a library specific exception;
        // we used to translate to spring exceptions, but spring dependency has been removed
        mutator.execute();
    }

    protected final Mutator<K> validateAndGetMutator(BatchContext txnContext) {
        if (txnContext == null) {
            return null;
        }

        //noinspection UnnecessarilyQualifiedInnerClassAccess
        Validate.isTrue(txnContext instanceof KeyspaceTemplate.HectorBatchContext,
                        "BatchContext not valid for this DAO implementation");

        @SuppressWarnings({"unchecked", "ConstantConditions"})
        HectorBatchContext htc = (HectorBatchContext)txnContext;

        htc.validateSameKeyspace(keyspace);
        return htc.getMutator();
    }

    protected final Serializer<K> getKeySerializer() {
        return keySerializer;
    }

    protected final Keyspace getKeyspace() {
        return keyspace;
    }

    class HectorBatchContext implements BatchContext {
        final Mutator<K> mutator;

        private HectorBatchContext(Keyspace keyspace, Serializer<K> keySerializer) {
            mutator = HFactory.createMutator(keyspace, keySerializer);
        }

        Mutator<K> getMutator() {
            return mutator;
        }

        void validateSameKeyspace(Keyspace keyspace) {
            if (KeyspaceTemplate.this.keyspace != keyspace) {
                throw new IllegalArgumentException("This transaction object was created for a different keyspace");
            }
        }
    }
}
