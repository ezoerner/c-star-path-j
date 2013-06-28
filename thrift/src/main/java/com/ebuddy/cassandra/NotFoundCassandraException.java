package com.ebuddy.cassandra;

import org.springframework.dao.DataRetrievalFailureException;

/**
 * Translation of a cassandra NotFoundCassandraException to the Spring DataAccessException hierarchy.
 * <p>
 * A specific column was requested that does not exist.</p>
 *
 * @author Eric Zoerner
 */
public class NotFoundCassandraException extends DataRetrievalFailureException {

    /**
     * Constructor for NotFoundCassandraException.
     *
     * @param msg the detail message
     * @param cause the root cause
     */
    public NotFoundCassandraException(String msg, Throwable cause) {
        super(msg, cause);
    }
}