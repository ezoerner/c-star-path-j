package com.ebuddy.cassandra.structure;

/**
 * Error thrown when the data format is incorrect; either data cannot be parsed as JSON or an object could
 * not be encoded as JSON.
 *
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
public class DataFormatException extends RuntimeException {

    public DataFormatException(String message) {
        super(message);
    }

    public DataFormatException(String s, Exception cause) {
        super(s, cause);
    }
}
