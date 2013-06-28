package com.ebuddy.cassandra;

import org.springframework.dao.InvalidDataAccessResourceUsageException;

/**
 * Translation of a cassandra InvalidRequestException to the Spring DataAccessException hierarchy.
 * <p>
 * Invalid request could mean keyspace or column family does not exist, required parameters are missing, or a parameter
 * is malformed. <code>why </code> contains an associated error message.</p>
 *
 * @author Eric Zoerner
 */
public class InvalidRequestCassandraException extends InvalidDataAccessResourceUsageException {

    private String why;

    /**
     * Constructor for InvalidRequestCassandraException.
     *
     * @param msg the detail message
     * @param cause the root cause
     */
    public InvalidRequestCassandraException(String msg, Throwable cause) {
        super(msg, cause);
        setWhy(msg);
    }

    /**
     * Set additional information about the reason for this exception.
     *
     * @param w a String description.
     */
    public void setWhy(String w) {
      why = w;
    }

    /**
     * Get additional information about the reason for this exception.
     *
     * @return a String description.
     */
    @SuppressWarnings({"UnusedDeclaration"})
    public String getWhy() {
      return why;
    }
}