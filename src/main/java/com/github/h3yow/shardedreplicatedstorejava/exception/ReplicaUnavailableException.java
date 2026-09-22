package com.github.h3yow.shardedreplicatedstorejava.exception;

/** 可用副本数在发起写入前就不足多数派。 */
public class ReplicaUnavailableException extends StoreException {
    public ReplicaUnavailableException(String reason, String message) {
        super(reason, message);
    }
}
