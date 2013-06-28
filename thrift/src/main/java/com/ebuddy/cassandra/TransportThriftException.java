package com.ebuddy.cassandra;

import org.springframework.dao.DataAccessResourceFailureException;

/**
 * Translation of a Hector HectorTransportException to the Spring DataAccessException hierarchy.
 * A HectorTransportException can be caused by a Thrift <code>TTransportException</code> or <code>TException</code>.
 * <p>
 * A transport exception (either in Thrift or avro).</p>
 *
 * @author Eric Zoerner
 */
public class TransportThriftException extends DataAccessResourceFailureException {

    /**
     * Constructor for PoolIllegalStateHectorException.
     *
     * @param msg the detail message
     * @param cause the root cause
     */
    public TransportThriftException(String msg, Throwable cause) {
        super(msg, cause);
    }
}