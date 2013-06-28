package com.ebuddy.cassandra;

import org.springframework.dao.InvalidDataAccessApiUsageException;

/**
 * Translation of a Hector PoolIllegalStateException to the Spring DataAccessException hierarchy.
 * <p>
 * Happens when the pool has been closed, but a new borrow request comes to it.</p>
 *
 * @author Eric Zoerner
 */
public class PoolIllegalStateHectorException extends InvalidDataAccessApiUsageException {

    /**
     * Constructor for PoolIllegalStateHectorException.
     *
     * @param msg the detail message
     * @param cause the root cause
     */
    public PoolIllegalStateHectorException(String msg, Throwable cause) {
        super(msg, cause);
    }
}