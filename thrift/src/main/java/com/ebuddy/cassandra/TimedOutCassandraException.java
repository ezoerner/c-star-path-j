package com.ebuddy.cassandra;

import org.springframework.dao.TransientDataAccessResourceException;

/**
 * Translation of a cassandra TimedOutException to the Spring DataAccessException hierarchy.
 * <p>
 * The node responsible for the write or read did not respond during the rpc interval specified in your configuration
 * (default 10s). This can happen if the request is too large, the node is oversaturated with requests, or the node is
 * down but the failure detector has not yet realized it (usually this takes < 30s).</p>
 *
 * @author Eric Zoerner
 */
public class TimedOutCassandraException extends TransientDataAccessResourceException {

    /**
     * Constructor for TimedOutCassandraException.
     * 
     * @param msg the detail message
     * @param cause the root cause
     */
    public TimedOutCassandraException(String msg, Throwable cause) {
        super(msg, cause);
    }
}