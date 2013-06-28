package com.ebuddy.cassandra;

import org.springframework.dao.DataAccessException;

/**
 * Translates exceptions from type T to the Spring DataAccessException hierarchy,
 * optionally in the context of executing SQL and a named task.
 *
 * @author Joost van de Wijgerd <jvandewijgerd@ebuddy.com>
 */
public interface NoSQLExceptionTranslator<T extends Exception> {
    
	/**
	 * Translate the given {@link Exception} to a generic {@link org.springframework.dao.DataAccessException}.
	 * <p>The returned DataAccessException is supposed to contain the original
	 * <code>SQLException</code> as root cause. However, client code may not generally
	 * rely on this due to DataAccessExceptions possibly being caused by other resource
	 * APIs as well. That said, a <code>getRootCause() instanceof SQLException</code>
	 * check (and subsequent cast) is considered reliable when expecting JDBC-based
	 * access to have happened.
     * 
	 *
     * @param ex the offending <code>SQLException</code>
     * @return the DataAccessException, wrapping the <code>SQLException</code>
	 * @see org.springframework.dao.DataAccessException#getRootCause()
	 */
// previously there were two extra parameters here, but we removed them because they were never used
/*
* @param task readable text describing the task being attempted (may be <code>null</code>)
* @param sql SQL query or update that caused the problem (may be <code>null</code>)
*/
	DataAccessException translate(T ex);
}




