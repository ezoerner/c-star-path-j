package com.ebuddy.cassandra;

import org.springframework.dao.DataAccessResourceFailureException;

/**
 * Translation of a Hector HectorPoolException to the Spring DataAccessException hierarchy.
 * <p>
 * Error while borrowing or returning object to the pool.</p>
 *
 * @author Eric Zoerner
 */
public class PoolHectorException extends DataAccessResourceFailureException {

    /**
     * Constructor for UnavailableCassandraException.
     *
     * @param msg the detail message
     * @param cause the root cause
     */
    public PoolHectorException(String msg, Throwable cause) {
        super(msg, cause);
    }
}