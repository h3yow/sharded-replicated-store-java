package com.github.h3yow.shardedreplicatedstorejava.exception;

/** 日志损坏（截断 / 校验和不符 / 缺号 / 乱序），恢复停在最后一致位点。 */
public class LogCorruptedException extends StoreException {
    public LogCorruptedException(String reason, String message) {
        super(reason, message);
    }
}
