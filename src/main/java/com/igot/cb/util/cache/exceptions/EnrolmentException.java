package com.igot.cb.util.cache.exceptions;

/**
 * Dedicated exception for failures while performing an atomic Redis cache operation.
 * Extends RuntimeException to indicate unchecked exceptions.
 */
public class EnrolmentException extends RuntimeException {

    public EnrolmentException(String message, Throwable cause) {
        super(message, cause);
    }

}
