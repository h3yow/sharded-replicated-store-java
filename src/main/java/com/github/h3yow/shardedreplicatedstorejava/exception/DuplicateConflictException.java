package com.github.h3yow.shardedreplicatedstorejava.exception;

/** 同一 requestId 携带了不同的写内容：疑似非幂等重试，必须拒绝。 */
public class DuplicateConflictException extends StoreException {
    public DuplicateConflictException(String reason, String message) {
        super(reason, message);
    }
}
