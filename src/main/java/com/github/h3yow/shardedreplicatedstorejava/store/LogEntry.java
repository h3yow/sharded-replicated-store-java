package com.github.h3yow.shardedreplicatedstorejava.store;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.CRC32;

/**
 * 一条追加到分片日志中的条目。
 *
 * <ul>
 *   <li>{@code index}：分片日志内的位置，必须从 1 开始严格连续；</li>
 *   <li>{@code term}：协调者世代（拓扑 epoch）；</li>
 *   <li>{@code requestId}：客户端提交 ID，用于重复提交识别；</li>
 *   <li>{@code version}：键的新版本号，状态机仅接受更大的版本。</li>
 * </ul>
 *
 * <p>单行文本格式（字段以制表符分隔，键/值做 Base64 编码，行首为载荷 CRC32）：</p>
 * <pre>crc=CHECKSUM  idx=..  term=..  req=..  type=PUT  ver=..  key=..  val=..</pre>
 */
public record LogEntry(long index, long term, String requestId, EntryType type,
                       String key, String value, long version) {

    public static final long NO_VERSION = -1L;

    public LogEntry {
        if (index <= 0) {
            throw new IllegalArgumentException("log index must be positive: " + index);
        }
        if (requestId == null || type == null || key == null) {
            throw new IllegalArgumentException("requestId/type/key must not be null");
        }
    }

    public static LogEntry put(long index, long term, String requestId,
                               String key, String value, long version) {
        return new LogEntry(index, term, requestId, EntryType.PUT, key, value, version);
    }

    public static LogEntry migrate(long index, long term, String requestId,
                                   String key, String value, long version) {
        return new LogEntry(index, term, requestId, EntryType.MIGRATE, key, value, version);
    }

    private static String b64(String raw) {
        return Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    private static String unb64(String encoded) {
        return new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
    }

    private static long checksum(String payload) {
        CRC32 crc = new CRC32();
        crc.update(payload.getBytes(StandardCharsets.UTF_8));
        return crc.getValue();
    }

    /** 序列化为单行文本（不含行终止符）。 */
    public String serialize() {
        String payload = String.join("\t",
                "idx=" + index,
                "term=" + term,
                "req=" + b64(requestId),
                "type=" + type.name(),
                "ver=" + version,
                "key=" + b64(key),
                "val=" + b64(value == null ? "" : value));
        return "crc=" + checksum(payload) + "\t" + payload;
    }

    /**
     * 从单行解析。格式错误抛 {@link LogCorruptionException}（MALFORMED_LINE），
     * 校验和不匹配抛 CHECKSUM_MISMATCH；index 连续性由日志扫描层负责。
     */
    public static LogEntry deserialize(String line) {
        String[] parts = line.split("\t", -1);
        if (parts.length != 8 || !parts[0].startsWith("crc=")) {
            throw new LogCorruptionException(LogCorruptionReason.MALFORMED_LINE,
                    -1, -1, "日志行列结构不合法: " + abbreviate(line));
        }
        long expectedCrc;
        try {
            expectedCrc = Long.parseLong(parts[0].substring(4));
        } catch (NumberFormatException e) {
            throw new LogCorruptionException(LogCorruptionReason.MALFORMED_LINE,
                    -1, -1, "校验和字段不是数字: " + abbreviate(line));
        }
        String payload = line.substring(parts[0].length() + 1);
        if (checksum(payload) != expectedCrc) {
            throw new LogCorruptionException(LogCorruptionReason.CHECKSUM_MISMATCH,
                    -1, -1, "校验和不匹配（疑似位翻转/篡改）: " + abbreviate(line));
        }
        try {
            long index = Long.parseLong(field(parts[1], "idx"));
            long term = Long.parseLong(field(parts[2], "term"));
            String requestId = unb64(field(parts[3], "req"));
            EntryType type = EntryType.valueOf(field(parts[4], "type"));
            long version = Long.parseLong(field(parts[5], "ver"));
            String key = unb64(field(parts[6], "key"));
            String rawValue = unb64(field(parts[7], "val"));
            String value = rawValue.isEmpty() ? null : rawValue;
            return new LogEntry(index, term, requestId, type, key, value, version);
        } catch (RuntimeException e) {
            throw new LogCorruptionException(LogCorruptionReason.MALFORMED_LINE,
                    -1, -1, "日志行字段无法解析: " + abbreviate(line) + " (" + e + ")");
        }
    }

    private static String field(String token, String name) {
        String prefix = name + "=";
        if (!token.startsWith(prefix)) {
            throw new IllegalArgumentException("期望字段 " + name + "，实际 " + token);
        }
        return token.substring(prefix.length());
    }

    private static String abbreviate(String line) {
        return line.length() <= 80 ? line : line.substring(0, 80) + "...";
    }
}
