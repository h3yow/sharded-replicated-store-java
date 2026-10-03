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
 *
 * <p>除首次结论外，每条记录还持久化该 requestId 首次提交所针对的 <b>key</b>：
 * 再平衡把键迁到新分片时，必须连同该键名下<strong>全部已结案 requestId</strong>
 * （无论首次结论是成功还是失败）一起搬到新分片的幂等表，否则新分片对旧
 * requestId 一无所知，重提会被当成全新写入——失败结论被翻案、成功结论被重复应用。</p>
 */
public final class IdempotencyTable {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyTable.class);

    /** 一条已结案记录：requestId 首次提交的键 + 首次结论。 */
    public record Settled(String key, WriteResult result) {
        public String requestId() {
            return result.requestId();
        }
    }

    private final Path file;
    private final Map<String, Settled> table = new LinkedHashMap<>();

    public IdempotencyTable(Path file) throws IOException {
        this.file = file;
        Files.createDirectories(file.getParent());
        if (Files.exists(file)) {
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (line.isEmpty()) {
                    continue;
                }
                Settled parsed = deserialize(line);
                table.put(parsed.requestId(), parsed);
            }
            log.debug("幂等表载入 {} 条记录 ({})", table.size(), file);
        }
    }

    public synchronized boolean contains(String requestId) {
        return table.containsKey(requestId);
    }

    public synchronized WriteResult get(String requestId) {
        Settled settled = table.get(requestId);
        return settled == null ? null : settled.result();
    }

    /** 再平衡迁移使用：返回本分片全部已结案记录（成功与失败都包含）。 */
    public synchronized List<Settled> allSettled() {
        return List.copyOf(table.values());
    }

    /**
     * 仅在首次见到该 requestId 时结案；并发重复提交由副本集的有序锁挡住。
     *
     * @param key 该 requestId 首次提交针对的键（随记录持久化，供再平衡按键迁移）
     */
    public synchronized void put(String key, WriteResult result) throws IOException {
        if (table.containsKey(result.requestId())) {
            WriteResult first = table.get(result.requestId()).result();
            log.warn("requestId={} 已存在首次结论 status={}，拒绝重复结案（重复提交不重复应用）",
                    result.requestId(), first.status());
            throw new IllegalStateException("duplicated settlement for requestId="
                    + result.requestId());
        }
        appendLine(serialize(key, result));
        table.put(result.requestId(), new Settled(key, result));
    }

    /**
     * 再平衡迁移专用：把一条<strong>来自旧分片的已结案记录</strong>导入本表。
     *
     * <p>幂等：同一 requestId 且首次结论完全一致时跳过（再平衡中途失败回滚后重试，
     * 或键仍落在同名分片时，记录可能已经存在）；requestId 相同但结论不同说明
     * “同一 requestId 在两个分片结过不同的案”，属于不变量破坏，直接拒绝，
     * 绝不静默覆盖首次结论。导入只追加一行，不触碰日志与状态机。</p>
     *
     * @return true 表示本次实际新导入；false 表示已有完全一致的记录而跳过
     */
    public synchronized boolean importIfAbsent(Settled settled) throws IOException {
        Settled existing = table.get(settled.requestId());
        if (existing != null) {
            // requestId 相同但结论不同，说明“同一 requestId 在两个分片结过不同的案”，
            // 属于不变量破坏，直接拒绝，绝不静默覆盖首次结论。
            if (!sameConclusion(existing.result(), settled.result())) {
                throw new IllegalStateException("requestId=" + settled.requestId()
                        + " 在目标分片已有冲突的首次结论: 旧 " + existing.result().status()
                        + " vs 迁入 " + settled.result().status());
            }
            log.debug("requestId={} 的首次结论已存在（status={}），迁移导入跳过",
                    settled.requestId(), existing.result().status());
            return false;
        }
        appendLine(serialize(settled.key(), settled.result()));
        table.put(settled.requestId(), settled);
        return true;
    }

    private void appendLine(String line) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            w.write(line);
            w.newLine();
        }
    }

    /** 结论一致：对外首次结论的状态、位点、版本必须相同（迁移不允许改写历史）。 */
    private static boolean sameConclusion(WriteResult a, WriteResult b) {
        return a.status() == b.status()
                && a.logIndex() == b.logIndex()
                && a.version() == b.version();
    }

    private static String b64(String raw) {
        return Base64.getEncoder().encodeToString(
                (raw == null ? "" : raw).getBytes(StandardCharsets.UTF_8));
    }

    private static String unb64(String encoded) {
        return new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
    }

    static String serialize(String key, WriteResult r) {
        return String.join("\t",
                "status=" + r.status().name(),
                "shard=" + b64(r.shardId()),
                "req=" + b64(r.requestId()),
                "idx=" + r.logIndex(),
                "ver=" + r.version(),
                "acks=" + r.acks(),
                "quorum=" + r.quorum(),
                "pos=" + r.contestPosition(),
                "size=" + r.contestSize(),
                "winner=" + b64(r.winnerRequestId() == null ? "" : r.winnerRequestId()),
                "reason=" + b64(r.decisionReason()),
                "key=" + b64(key == null ? "" : key));
    }

    static Settled deserialize(String line) {
        String[] p = line.split("\t", -1);
        if (p.length != 12 && p.length != 11 && p.length != 8) {
            throw new IllegalStateException("幂等表行格式损坏: " + line);
        }
        try {
            WriteStatus status = WriteStatus.valueOf(p[0].substring("status=".length()));
            String shard = unb64(p[1].substring("shard=".length()));
            String req = unb64(p[2].substring("req=".length()));
            long idx = Long.parseLong(p[3].substring("idx=".length()));
            long ver = Long.parseLong(p[4].substring("ver=".length()));
            int acks = Integer.parseInt(p[5].substring("acks=".length()));
            int quorum = Integer.parseInt(p[6].substring("quorum=".length()));
            int pos = -1;
            int size = 0;
            String winner = null;
            String reason;
            String key = null;
            if (p.length == 8) {
                // 旧格式（决胜字段引入前）：reason 是第 8 段；key 未知。
                reason = unb64(p[7].substring("reason=".length()));
            } else {
                pos = Integer.parseInt(p[7].substring("pos=".length()));
                size = Integer.parseInt(p[8].substring("size=".length()));
                String winnerRaw = unb64(p[9].substring("winner=".length()));
                winner = winnerRaw.isEmpty() ? null : winnerRaw;
                reason = unb64(p[10].substring("reason=".length()));
                if (p.length == 12) {
                    String keyRaw = unb64(p[11].substring("key=".length()));
                    key = keyRaw.isEmpty() ? null : keyRaw;
                }
            }
            WriteResult result = new WriteResult(status, shard, req, idx, ver, acks, quorum,
                    pos, size, winner, reason);
            return new Settled(key, result);
        } catch (RuntimeException e) {
            throw new IllegalStateException("幂等表行无法解析: " + line, e);
        }
    }
}
