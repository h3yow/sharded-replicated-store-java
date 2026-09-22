package com.github.h3yow.shardedreplicatedstorejava.exception;

/** 已向足够多副本发起写入，但确认数未达多数派。 */
public class InsufficientAcksException extends StoreException {
    public InsufficientAcksException(String reason, String message) {
        super(reason, message);
    }
}
