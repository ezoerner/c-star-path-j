package com.ebuddy.cassandra;

import org.springframework.dao.DataAccessResourceFailureException;

/**
 * Translation of a cassandra UnavailableException to the Spring DataAccessException hierarchy.
 * <p>
 * Not all the replicas required could be created and/or read.</p>
 *
 * @author Eric Zoerner
 */
public class UnavailableCassandraException extends DataAccessResourceFailureException {

    /**
     * Constructor for UnavailableCassandraException.
     *
     * @param msg the detail message
     * @param cause the root cause
     */
    public UnavailableCassandraException(String msg, Throwable cause) {
        super(msg, cause);
    }
}