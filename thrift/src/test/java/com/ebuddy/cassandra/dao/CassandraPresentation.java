package com.ebuddy.cassandra.dao;

import com.ebuddy.cassandra.property.PropertyValue;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.*;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSubSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
