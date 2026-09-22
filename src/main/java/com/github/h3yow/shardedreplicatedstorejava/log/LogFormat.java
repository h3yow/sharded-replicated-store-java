package com.github.h3yow.shardedreplicatedstorejava.log;

import com.github.h3yow.shardedreplicatedstorejava.exception.LogCorruptedException;
import com.github.h3yow.shardedreplicatedstorejava.model.LogEntry;
import com.github.h3yow.shardedreplicatedstorejava.util.Checksums;

/**
 * 日志行的序列化与解析。
 *
 * <p>行格式（制表符分隔，value/base64 已做转义，不含制表与换行）：
 * <pre>v1 \t index \t term \t op \t requestId \t key \t valueB64 \t checksum</pre>
 * valueB64 为 "-" 表示 null（DELETE）。
 *
 * <p>解析失败统一抛 {@link LogCorruptedException}，reason 取值：
 * {@code LOG_TRUNCATED}（行被截断、字段不全）、{@code LOG_MALFORMED}（数字/枚举无法解析）、
 * {@code LOG_CHECKSUM_MISMATCH}（校验和不符）。
 */
public final class LogFormat {

    public static final int VERSION = 1;
    public static final String TRUNCATED = "LOG_TRUNCATED";
    public static final String MALFORMED = "LOG_MALFORMED";
    public static final String CHECKSUM_MISMATCH = "LOG_CHECKSUM_MISMATCH";

    private static final int FIELD_COUNT = 8;

    private LogFormat() {
    }

    public static String serialize(LogEntry e) {
        String valueB64 = e.value() == null ? "-"
                : java.util.Base64.getEncoder().encodeToString(
                        e.value().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return String.join("\t",
                "v" + VERSION,
                Long.toString(e.index()),
                Long.toString(e.term()),
                e.op().name(),
                e.requestId(),
                e.key(),
                valueB64,
                Long.toString(e.checksum()));
    }

    public static LogEntry parse(String line) {
        String[] parts = line.split("\t", -1);
        if (parts.length != FIELD_COUNT) {
            throw new LogCorruptedException(TRUNCATED,
                    "truncated log line: expected " + FIELD_COUNT
                            + " fields but got " + parts.length);
        }
        if (!("v" + VERSION).equals(parts[0])) {
            throw new LogCorruptedException(MALFORMED, "unknown format tag: " + parts[0]);
        }
        long index;
        long term;
        long checksum;
        try {
            index = Long.parseLong(parts[1]);
            term = Long.parseLong(parts[2]);
            checksum = Long.parseLong(parts[7]);
        } catch (NumberFormatException ex) {
            throw new LogCorruptedException(MALFORMED, "non-numeric numeric field: " + ex.getMessage());
        }
        LogEntry.Operation op;
        try {
            op = LogEntry.Operation.valueOf(parts[3]);
        } catch (IllegalArgumentException ex) {
            throw new LogCorruptedException(MALFORMED, "unknown operation: " + parts[3]);
        }
        String requestId = parts[4];
        String key = parts[5];
        if (requestId.isEmpty() || key.isEmpty()) {
            throw new LogCorruptedException(MALFORMED, "blank requestId or key");
        }
        String value;
        if ("-".equals(parts[6])) {
            value = null;
        } else {
            try {
                value = new String(java.util.Base64.getDecoder().decode(parts[6]),
                        java.nio.charset.StandardCharsets.UTF_8);
            } catch (IllegalArgumentException ex) {
                throw new LogCorruptedException(MALFORMED, "bad base64 value: " + ex.getMessage());
            }
        }
        long expected = Checksums.of(index, term, requestId, key, value, op.name());
        if (expected != checksum) {
            throw new LogCorruptedException(CHECKSUM_MISMATCH,
                    "checksum mismatch at index " + index + ": stored=" + checksum
                            + " computed=" + expected);
        }
        return new LogEntry(index, term, requestId, key, value, op, checksum);
    }
}
