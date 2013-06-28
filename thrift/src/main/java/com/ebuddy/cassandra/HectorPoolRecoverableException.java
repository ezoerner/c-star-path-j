package com.ebuddy.cassandra;

import org.springframework.dao.DataAccessResourceFailureException;

/**
 * Translation of a Hector HPoolRecoverableException to the Spring DataAccessException hierarchy.
 * <p>
 * Indicates a recoverable pool exception.</p>
 *
 * @author Eric Zoerner
 */
public class HectorPoolRecoverableException extends DataAccessResourceFailureException {

    /**
     * Constructor for UnavailableCassandraException.
     *
     * @param msg the detail message
     * @param cause the root cause
     */
    public HectorPoolRecoverableException(String msg, Throwable cause) {
        super(msg, cause);
    }
}