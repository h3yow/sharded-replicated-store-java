package com.github.h3yow.shardedreplicatedstorejava.log;

import com.github.h3yow.shardedreplicatedstorejava.exception.DuplicateConflictException;
import com.github.h3yow.shardedreplicatedstorejava.exception.LogCorruptedException;
import com.github.h3yow.shardedreplicatedstorejava.model.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 基于本地文本文件的 {@link ReplicaLog} 实现（每行一个 {@link LogFormat} 条目）。
 *
 * <p>位点规则：index 从 1 开始且必须严格连续。读取校验时一旦遇到
 * 校验和错误（LOG_CHECKSUM_MISMATCH）、缺号（LOG_INDEX_GAP）或乱序
 * （LOG_INDEX_OUT_OF_ORDER），立即停止，不静默跳过，并把日志物理截断到
 * 最后一个一致位点。
 */
public class FileReplicaLog implements ReplicaLog {

    private static final Logger log = LoggerFactory.getLogger(FileReplicaLog.class);

    public static final String INDEX_GAP = "LOG_INDEX_GAP";
    public static final String INDEX_OUT_OF_ORDER = "LOG_INDEX_OUT_OF_ORDER";

    private final Path file;
    private final List<LogEntry> entries = new ArrayList<>();
    private final Map<String, LogEntry> byRequestId = new HashMap<>();
    private String corruptionReason;

    public FileReplicaLog(Path file) {
        this.file = file;
        try {
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            if (Files.exists(file)) {
                load();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot open log " + file, e);
        }
    }

    private void load() throws IOException {
        long expected = 1;
        long lastConsistent = 0;
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    // 末尾的空行（尾部换行产生）可接受；空文件后无内容
                    continue;
                }
                LogEntry entry;
                try {
                    entry = LogFormat.parse(line);
                } catch (LogCorruptedException ex) {
                    corruptionReason = ex.getReason();
                    log.warn("[log:{}] 损坏停在期望位点 {}：reason={}，最后一致位点={}",
                            file, expected, ex.getReason(), lastConsistent);
                    break;
                }
                if (entry.index() < expected) {
                    corruptionReason = INDEX_OUT_OF_ORDER;
                    log.warn("[log:{}] 乱序：index={}，期望={}，最后一致位点={}",
                            file, entry.index(), expected, lastConsistent);
                    break;
                }
                if (entry.index() > expected) {
                    corruptionReason = INDEX_GAP;
                    log.warn("[log:{}] 缺号：index={}，期望={}，最后一致位点={}",
                            file, entry.index(), expected, lastConsistent);
                    break;
                }
                entries.add(entry);
                byRequestId.put(entry.requestId(), entry);
                lastConsistent = entry.index();
                expected++;
            }
        }
        if (corruptionReason != null) {
            // 物理截断到最后一致位点：损坏尾部必须被剔除，不允许残留参与恢复
            truncateAfter(lastConsistent);
        }
    }

    @Override
    public synchronized long lastIndex() {
        return entries.size();
    }

    @Override
    public synchronized Optional<LogEntry> entryAt(long index) {
        if (index < 1 || index > entries.size()) {
            return Optional.empty();
        }
        return Optional.of(entries.get((int) index - 1));
    }

    @Override
    public synchronized List<LogEntry> entriesAfter(long index) {
        if (index < 0 || index >= entries.size()) {
            return List.of();
        }
        return List.copyOf(entries.subList((int) index, entries.size()));
    }

    @Override
    public synchronized Optional<LogEntry> findByRequestId(String requestId) {
        return Optional.ofNullable(byRequestId.get(requestId));
    }

    @Override
    public synchronized LogEntry append(LogEntry entry) {
        LogEntry existing = byRequestId.get(entry.requestId());
        if (existing != null) {
            // 幂等：同 requestId 视为重复提交。内容不同则是危险的非幂等重试。
            if (!samePayload(existing, entry)) {
                throw new DuplicateConflictException("DUPLICATE_REQUEST_CONFLICT",
                        "requestId " + entry.requestId()
                                + " already exists with different payload at index " + existing.index());
            }
            return existing;
        }
        long expectedIndex = entries.size() + 1L;
        if (entry.index() != expectedIndex) {
            // 防御性检查：正常路径由协调者在分片锁内分配连续位点
            throw new LogCorruptedException(INDEX_GAP,
                    "append index " + entry.index() + " but expected " + expectedIndex
                            + " in " + file);
        }
        try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            writer.write(LogFormat.serialize(entry));
            writer.newLine();
        } catch (IOException e) {
            throw new UncheckedIOException("append failed: " + file, e);
        }
        entries.add(entry);
        byRequestId.put(entry.requestId(), entry);
        return entry;
    }

    private static boolean samePayload(LogEntry a, LogEntry b) {
        return a.term() == b.term()
                && java.util.Objects.equals(a.key(), b.key())
                && java.util.Objects.equals(a.value(), b.value())
                && a.op() == b.op();
    }

    @Override
    public synchronized void truncateAfter(long index) {
        while (entries.size() > index) {
            LogEntry removed = entries.remove(entries.size() - 1);
            byRequestId.remove(removed.requestId());
        }
        try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (LogEntry e : entries) {
                writer.write(LogFormat.serialize(e));
                writer.newLine();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("truncate failed: " + file, e);
        }
    }

    @Override
    public synchronized long verifyAndRecover() {
        return entries.size();
    }

    @Override
    public synchronized String corruptionReason() {
        return corruptionReason;
    }

    @Override
    public void close() {
        // 所有写入均即时落盘，无内存缓冲需要刷新
    }
}
