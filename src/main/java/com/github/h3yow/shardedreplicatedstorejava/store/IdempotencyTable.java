package com.github.h3yow.shardedreplicatedstorejava.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * requestId -> 首次 {@link WriteResult} 的持久化去重表。
 *
 * <p>每个分片一个文件，每行一条已结案的写入（含成功与失败——失败也占用 requestId，
 * 保证同一 requestId 重试不会得到互相矛盾的结论）。文件格式为制表符分隔，
 * 字符串字段 Base64 编码，与日志行同样要求可逐行解析。</p>
 */
public final class IdempotencyTable {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyTable.class);

    private final Path file;
    private final Map<String, WriteResult> table = new LinkedHashMap<>();

    public IdempotencyTable(Path file) throws IOException {
        this.file = file;
        Files.createDirectories(file.getParent());
        if (Files.exists(file)) {
            int lineNo = 0;
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                lineNo++;
                if (line.isEmpty()) {
                    continue;
                }
                WriteResult parsed = deserialize(line);
                table.put(parsed.requestId(), parsed);
            }
            log.debug("幂等表载入 {} 条记录 ({})", table.size(), file);
        }
    }

    public synchronized boolean contains(String requestId) {
        return table.containsKey(requestId);
    }

    public synchronized WriteResult get(String requestId) {
        return table.get(requestId);
    }

    /** 本分片全部已结案请求的首次结论（再平衡时按 requestId 迁移去重用）。 */
    public synchronized java.util.Collection<WriteResult> allOutcomes() {
        return List.copyOf(table.values());
    }

    /**
     * 再平衡迁移专用：把“产生某键当前可见值的原始提交结论”登记到新分片的
     * 幂等表（保留原 requestId、原位点/版本结论）。只在该 requestId 尚未在
     * 新分片结案时写入；已是结论则不动（幂等），绝不覆盖已有首次结论。
     *
     * @return true 表示实际登记
     */
    public synchronized boolean adoptIfAbsent(WriteResult original) throws IOException {
        if (table.containsKey(original.requestId())) {
            return false;
        }
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            w.write(serialize(original));
            w.newLine();
        }
        table.put(original.requestId(), original);
        return true;
    }

    /** 仅在首次见到该 requestId 时结案；并发重复提交由副本集的有序锁挡住。 */
    public synchronized void put(WriteResult result) throws IOException {
        if (table.containsKey(result.requestId())) {
            WriteResult first = table.get(result.requestId());
            log.warn("requestId={} 已存在首次结论 status={}，拒绝重复结案（重复提交不重复应用）",
                    result.requestId(), first.status());
            throw new IllegalStateException("duplicated settlement for requestId="
                    + result.requestId());
        }
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            w.write(serialize(result));
            w.newLine();
        }
        table.put(result.requestId(), result);
    }

    private static String b64(String raw) {
        return Base64.getEncoder().encodeToString(
                (raw == null ? "" : raw).getBytes(StandardCharsets.UTF_8));
    }

    private static String unb64(String encoded) {
        return new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
    }

    static String serialize(WriteResult r) {
        return String.join("\t",
                "status=" + r.status().name(),
                "shard=" + b64(r.shardId()),
                "req=" + b64(r.requestId()),
                "key=" + b64(r.key() == null ? "" : r.key()),
                "idx=" + r.logIndex(),
                "ver=" + r.version(),
                "acks=" + r.acks(),
                "quorum=" + r.quorum(),
                "reason=" + b64(r.decisionReason()));
    }

    static WriteResult deserialize(String line) {
        String[] p = line.split("\t", -1);
        if (p.length != 9 && p.length != 8) {
            throw new IllegalStateException("幂等表行格式损坏: " + line);
        }
        try {
            WriteStatus status = WriteStatus.valueOf(p[0].substring("status=".length()));
            String shard = unb64(p[1].substring("shard=".length()));
            String req = unb64(p[2].substring("req=".length()));
            // 新格式第 4 段为 key；旧格式（无 key）从 idx 段开始。
            String key;
            int base;
            if (p.length == 9 && p[3].startsWith("key=")) {
                key = unb64(p[3].substring("key=".length()));
                base = 3;
            } else {
                key = null;
                base = 2;
            }
            long idx = Long.parseLong(p[base + 1].substring("idx=".length()));
            long ver = Long.parseLong(p[base + 2].substring("ver=".length()));
            int acks = Integer.parseInt(p[base + 3].substring("acks=".length()));
            int quorum = Integer.parseInt(p[base + 4].substring("quorum=".length()));
            String reason = unb64(p[base + 5].substring("reason=".length()));
            return new WriteResult(status, shard, req, key, idx, ver, acks, quorum, reason);
        } catch (RuntimeException e) {
            throw new IllegalStateException("幂等表行无法解析: " + line, e);
        }
    }
}
