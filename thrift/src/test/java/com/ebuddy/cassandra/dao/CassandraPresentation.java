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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSubSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;

/**
 *
 */
public class CassandraPresentation {
    private Keyspace userDataKeyspace;

    public List<String> readAllUUIDS(List<String> keys) {
        MultigetSubSliceQuery<String,String,String,String> query =
                HFactory.createMultigetSubSliceQuery(userDataKeyspace,
                                                     StringSerializer.get(),
                                                     StringSerializer.get(),
                                                     StringSerializer.get(),
                                                     StringSerializer.get());
        query.setKeys(keys).
                setColumnFamily("SocialGraph").
                setSuperColumn("UUUID").
                setRange(null, null, false, 1);

        QueryResult<Rows<String,String,String>> result = query.execute();
        List<String> uuids = new ArrayList<String>(result.get().getCount());

        for (Row<String,String,String> row : result.get()) {
            String key = row.getKey();
            HColumn<String,String> uuidColumn = row.getColumnSlice().getColumns().get(0);
            uuids.add(uuidColumn.getName());
        }
        return uuids;
    }

    // "xms_0e4e40d6-cd7c-4fe2-80ec-bb9bc2ee1015@xms.ebuddy.com"

    public void testAddToServiceSettings() {
        addToServiceSettings("0e4e40d6-cd7c-4fe2-80ec-bb9bc2ee1015","fb_","xms_0e4e40d6-cd7c-4fe2-80ec-bb9bc2ee1015@xms.ebuddy.com");
    }

    public void addToServiceSettings(String userId,String sourceNetworkId,String targetNetworkId) {
        // first create a column
        List<HColumn<String,String>> columns = Arrays.asList(
                HFactory.createColumn(sourceNetworkId,
                                      null,
                                      StringSerializer.get(),
                                      StringSerializer.get()),
                HFactory.createColumn(targetNetworkId,
                                      null,
                                      StringSerializer.get(),
                                      StringSerializer.get())
                );
        // wrap with a super column
        HSuperColumn<String,String,String> superColumn = HFactory.createSuperColumn("uds",
                                                                                    columns,
                                                                                    StringSerializer.get(),
                                                                                    StringSerializer.get(),
                                                                                    StringSerializer.get());
        Mutator<String> mutator = HFactory.createMutator(userDataKeyspace, StringSerializer.get());

        mutator.insert(userId,"ServiceSettings",superColumn);
    }
}
