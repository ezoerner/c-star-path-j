package com.ebuddy.cassandra;

import org.springframework.dao.UncategorizedDataAccessException;

/**
 * Translation of an exception from Hector or Cassandra to the Spring DataAccessException hierarchy,
 * used for an exception that has not otherwise been categorized as another DataAccessException.
 * 
 * @author Joost van de Wijgerd <jvandewijgerd@ebuddy.com>
 */
public class UncategorizedCassandraException extends UncategorizedDataAccessException {

    /**
     * Construct an UncategorizedCassandraException.
     *
     * @param msg detail error message
     * @param cause an exception that caused this exception
     */
	public UncategorizedCassandraException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
