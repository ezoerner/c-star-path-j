package com.ebuddy.cassandra.cql;

import com.datastax.driver.core.*;

/**
 * @see <a href="https://github.com/datastax/java-driver/blob/master/driver-core/src/test/java/com/datastax/driver/core/CustomTypeTest.java">CustomTypeTest.java</a>
 */
public class StructuredDao {
    public void doSomething() {
        ResultSet resultSet = null;
        Row row = resultSet.one();
        row.getBytesUnsafe(0);
    }
}
