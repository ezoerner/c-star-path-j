package com.ebuddy.cassandra.dao;

/**
 * Marker interface for an internal object that tracks a transaction (a single unit of work, not necessarily
 * with ACID properties).
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public interface TransactionContext { }
