package com.github.h3yow.shardedreplicatedstorejava.exception;

/**
 * 全部业务异常的基类，携带可区分的机器可读原因码 reason。
 */
public class StoreException extends RuntimeException {
    private final String reason;

    public StoreException(String reason, String message) {
        super(message);
        this.reason = reason;
    }

    public String getReason() {
        return reason;
    }
}
