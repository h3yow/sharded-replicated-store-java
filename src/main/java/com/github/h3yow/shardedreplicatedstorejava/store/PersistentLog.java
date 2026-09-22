package com.github.h3yow.shardedreplicatedstorejava.store;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于本地文件的追加式分片日志。
 *
 * <p>每条 {@link LogEntry} 序列化为一行。{@link #scanWithReport()} 是恢复/追赶时
 * 唯一可信的读取入口，逐条验证：行可解析、校验和一致、index 从 1 开始严格连续。
 * 一旦发现：</p>
 * <ul>
 *   <li>文件中途结束但最后一行没有换行 —— {@code TRUNCATED_ENTRY}；</li>
 *   <li>index 跳跃 —— {@code MISSING_INDEX}；</li>
 *   <li>index 倒退/重复 —— {@code INDEX_OUT_OF_ORDER}；</li>
 *   <li>校验和错误 —— {@code CHECKSUM_MISMATCH}；</li>
 * </ul>
 * <p>{@link #scanWithReport()} 不抛异常，而是返回损坏点之前的一致前缀与停止原因，
 * 恢复据此停在最后一个一致位点，绝不静默跳过坏行继续应用；{@link LogEntry#deserialize}
 * 在单行无法解析/校验和不符时抛出携带原因的 {@link LogCorruptionException}。</p>
 */
public final class PersistentLog {

    private final Path file;

    public PersistentLog(Path file) {
        this.file = file;
    }

    public Path file() {
        return file;
    }

    /** 追加一条日志并落盘（每次追加都 fsync 语义的 close，保证重启可见）。 */
    public synchronized void append(LogEntry entry) throws IOException {
        Files.createDirectories(file.getParent());
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            w.write(entry.serialize());
            w.newLine();
        }
    }

    /** 容错扫描结果：损坏点之前的一致前缀 + 停止原因（clean=true 时原因为 null）。 */
    public record ScanResult(List<LogEntry> prefix, boolean clean,
                             LogCorruptionReason reason, long lineNumber,
                             long lastConsistentIndex) {
    }

    /**
     * 容错扫描：不抛异常，返回损坏点之前的一致前缀与停止原因。
     * 供副本重启恢复使用 —— 恢复的职责就是“停在最后一个一致位点”。
     */
    public synchronized ScanResult scanWithReport() throws IOException {
        if (!Files.exists(file)) {
            return new ScanResult(List.of(), true, null, 0, 0);
        }
        byte[] bytes = Files.readAllBytes(file);
        String content = new String(bytes, StandardCharsets.UTF_8);
        if (content.isEmpty()) {
            return new ScanResult(List.of(), true, null, 0, 0);
        }

        String[] lines = content.split("\n", -1);
        int lineCount = lines.length;
        boolean danglingLast = !lines[lines.length - 1].isEmpty();
        if (!danglingLast) {
            lineCount = lines.length - 1;
        }

        List<LogEntry> prefix = new ArrayList<>();
        long expectedIndex = 1;
        for (int i = 0; i < lineCount; i++) {
            long lineNumber = i + 1;
            String line = lines[i];
            long lastConsistent = expectedIndex - 1;

            if (i == lineCount - 1 && danglingLast) {
                return new ScanResult(prefix, false,
                        LogCorruptionReason.TRUNCATED_ENTRY, lineNumber, lastConsistent);
            }
            if (line.isEmpty()) {
                return new ScanResult(prefix, false,
                        LogCorruptionReason.MALFORMED_LINE, lineNumber, lastConsistent);
            }

            LogEntry entry;
            try {
                entry = LogEntry.deserialize(line);
            } catch (LogCorruptionException e) {
                return new ScanResult(prefix, false, e.reason(), lineNumber, lastConsistent);
            }

            if (entry.index() > expectedIndex) {
                return new ScanResult(prefix, false,
                        LogCorruptionReason.MISSING_INDEX, lineNumber, lastConsistent);
            }
            if (entry.index() < expectedIndex) {
                return new ScanResult(prefix, false,
                        LogCorruptionReason.INDEX_OUT_OF_ORDER, lineNumber, lastConsistent);
            }
            prefix.add(entry);
            expectedIndex++;
        }
        return new ScanResult(prefix, true, null, 0, expectedIndex - 1);
    }

    /**
     * 保留前 keepInclusive 行（即 index=1..keepInclusive 的前缀），删除其余全部行。
     * 按原始文本行操作而不解析载荷，因此即使待丢弃部分包含损坏行也能完成截断。
     */
    public synchronized void truncateSuffix(long keepInclusive) throws IOException {
        List<String> kept = new ArrayList<>();
        if (Files.exists(file) && keepInclusive > 0) {
            List<String> rawLines = Files.readAllLines(file, StandardCharsets.UTF_8);
            int limit = (int) Math.min(keepInclusive, rawLines.size());
            for (int i = 0; i < limit; i++) {
                if (!rawLines.get(i).isEmpty()) {
                    kept.add(rawLines.get(i));
                }
            }
        }
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (String line : kept) {
                w.write(line);
                w.newLine();
            }
        }
    }
}
