package com.ebuddy.cassandra.cql;

import org.springframework.dao.DataAccessException;

import com.datastax.driver.core.Session;

/**
 * // TODO: Add class description here.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public interface SessionCallback<T> {
    T doInSession(Session session) throws DataAccessException;
}
