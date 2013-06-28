package com.ebuddy.cassandra;

import me.prettyprint.hector.api.exceptions.*;
import org.springframework.dao.DataAccessException;

/**
 * Translates HectorExceptions to Spring DataAccessExceptions.
 *
 * @author Joost van de Wijgerd <jvandewijgerd@ebuddy.com>
 * @author Eric Zoerner
 */
public final class HectorExceptionTranslator implements NoSQLExceptionTranslator<HectorException> {

	@SuppressWarnings("ChainOfInstanceofChecks")
    @Override
	public DataAccessException translate(HectorException hex) {
        if (hex instanceof HTimedOutException) {
            return new TimedOutCassandraException(hex.getMessage(), hex);
        } else if (hex instanceof PoolIllegalStateException) {
            return new PoolIllegalStateHectorException(hex.getMessage(), hex);
        } else if (hex instanceof HUnavailableException) {
            return new UnavailableCassandraException(hex.getMessage(), hex);
        } else if (hex instanceof HNotFoundException) {
            return new NotFoundCassandraException(hex.getMessage(), hex);
        } else if (hex instanceof HectorTransportException) {
            return new TransportThriftException(hex.getMessage(), hex);
        } else if (hex instanceof HectorPoolException) {
            // this is not expected because
            // HectorPoolException is never thrown in the current Hector library
            return new PoolHectorException(hex.getMessage(), hex);
        } else if (hex instanceof HPoolRecoverableException) {
            return new HectorPoolRecoverableException(hex.getMessage(), hex);
        } else if (hex instanceof HInvalidRequestException) {
            InvalidRequestCassandraException e = new InvalidRequestCassandraException(hex.getMessage(), hex);
            e.setWhy(((HInvalidRequestException)hex).getWhy());
            return e;
        }
		return new UncategorizedCassandraException(hex.getMessage(), hex);
	}
}
