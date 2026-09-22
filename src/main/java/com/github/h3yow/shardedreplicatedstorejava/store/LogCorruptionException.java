package com.github.h3yow.shardedreplicatedstorejava.store;

/** 扫描日志时发现损坏：携带原因、行号与最后一个一致位点。 */
public class LogCorruptionException extends RuntimeException {

    private final LogCorruptionReason reason;
    private final long lineNumber;
    private final long lastConsistentIndex;

    public LogCorruptionException(LogCorruptionReason reason, long lineNumber,
                                  long lastConsistentIndex, String message) {
        super(message);
        this.reason = reason;
        this.lineNumber = lineNumber;
        this.lastConsistentIndex = lastConsistentIndex;
    }

    public LogCorruptionReason reason() {
        return reason;
    }

    public long lineNumber() {
        return lineNumber;
    }

    public long lastConsistentIndex() {
        return lastConsistentIndex;
    }
}
