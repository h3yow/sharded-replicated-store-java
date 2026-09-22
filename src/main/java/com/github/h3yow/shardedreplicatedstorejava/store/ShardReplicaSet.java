package com.github.h3yow.shardedreplicatedstorejava.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 一个分片的副本集合与写入多数派协调（单 leader、本地模拟）。
 *
 * <p>写入协议（两阶段的简化模拟）：</p>
 * <ol>
 *   <li>整个写入按 requestId 经 {@link DeterministicLock} 串行化 —— 并发提交的
 *       日志位点顺序确定可复现；</li>
 *   <li>先查 {@link IdempotencyTable}：requestId 已有结论直接返回首次结论
 *       （DUPLICATE），绝不二次应用；</li>
 *   <li>leader 先追加日志，再并行派发给其他副本；只有 UP 副本落盘才计 ack；</li>
 *   <li>ack 达到多数派：推进全体（已持有该条目的副本）提交位点并应用状态机，
 *       返回 COMMITTED；</li>
 *   <li>ack 不足：若所有失败都因为副本 DOWN/SLOW，且连“尽力可用”票数都凑不够，
 *       区分 REPLICA_UNAVAILABLE 与 INSUFFICIENT_ACKS：
 *       日志已在 leader 落盘只是票数不足为 INSUFFICIENT_ACKS；
 *       连 leader 都不可用为 REPLICA_UNAVAILABLE。</li>
 * </ol>
 */
public final class ShardReplicaSet {

    private static final Logger log = LoggerFactory.getLogger(ShardReplicaSet.class);

    private final String shardId;
    private final long term;
    private final Path shardDir;
    private final Map<String, Replica> replicas = new LinkedHashMap<>();
    private final Replica leader;
    private final int quorum;
    private final IdempotencyTable idempotency;
    private final DeterministicLock writeLock = new DeterministicLock();

    public ShardReplicaSet(String shardId, List<String> replicaIds,
                           long term, Path dataDir) throws IOException {
        this.shardId = shardId;
        this.term = term;
        this.shardDir = dataDir.resolve(shardId);
        Files.createDirectories(shardDir);
        for (String id : replicaIds) {
            Replica replica = new Replica(id, shardId, dataDir);
            replicas.put(id, replica);
        }
        this.leader = replicas.get(replicaIds.get(0));
        this.quorum = replicaIds.size() / 2 + 1;
        this.idempotency = new IdempotencyTable(shardDir.resolve("idempotency.txt"));
        log.info("初始化分片 {} term={} 副本数={} 多数派={} leader={}",
                shardId, term, replicaIds.size(), quorum, leader.replicaId());
    }

    public String shardId() {
        return shardId;
    }

    public Replica leader() {
        return leader;
    }

    public Replica replica(String id) {
        return replicas.get(id);
    }

    public List<Replica> replicas() {
        return List.copyOf(replicas.values());
    }

    public int quorum() {
        return quorum;
    }

    /**
     * 协调一次写入。线程安全，临界区互斥（普通路径，先竞争者先执行）。
     */
    public WriteResult write(String requestId, String key, String value) {
        return writeLock.runOrdered(requestId, () -> doWrite(requestId, key, value));
    }

    /**
     * 批量严格全序写入：声明本批 {@code batchSize} 个并发竞争者，
     * 到齐后整批按 requestId 升序依次提交，最终版本与日志位点确定可复现。
     * 仅供需要可复现并发结论的场景使用。
     */
    public WriteResult writeInBatch(String requestId, String key, String value, int batchSize) {
        return writeLock.runOrderedInBatch(requestId, batchSize,
                () -> doWrite(requestId, key, value));
    }

    private WriteResult doWrite(String requestId, String key, String value) {
        WriteResult prior = idempotency.get(requestId);
        if (prior != null) {
            WriteResult dup = new WriteResult(WriteStatus.DUPLICATE, shardId, requestId,
                    prior.logIndex(), prior.version(), prior.acks(), prior.quorum(),
                    "重复提交: requestId=" + requestId + " 已有首次结论 "
                            + prior.status() + "（index=" + prior.logIndex()
                            + ", version=" + prior.version() + "），直接复用，不重复应用");
            log.info("[{}] 重复提交 requestId={} -> 返回首次结论 status={} index={} version={}",
                    shardId, requestId, prior.status(), prior.logIndex(), prior.version());
            return dup;
        }

        // leader 不可用：无法产生任何日志条目，属于副本不可用而非确认不足。
        if (leader.status() != ReplicaStatus.UP) {
            WriteResult result = new WriteResult(WriteStatus.REPLICA_UNAVAILABLE, shardId,
                    requestId, 0, LogEntry.NO_VERSION, 0, quorum,
                    "leader " + leader.replicaId() + " 状态=" + leader.status()
                            + "，写入无法进入日志，区别于确认不足");
            settle(result);
            log.warn("[{}] 写入失败 requestId={} REPLICA_UNAVAILABLE（leader={} 状态={}）",
                    shardId, requestId, leader.replicaId(), leader.status());
            return result;
        }

        // 先确定本轮条目在 leader 日志中的 index 与新版本（leader 已确认 UP）。
        long index = leader.lastLogIndex() + 1;
        VersionedValue current = leader.get(key);
        long version = (current == null ? 0 : current.version()) + 1;
        LogEntry entry = LogEntry.put(index, term, requestId, key, value, version);

        // leader 先追加；再把“同步缺失前缀 + 追加本轮条目”原子派发给每个 follower。
        boolean leaderAppended = leader.append(entry);
        int acks = leaderAppended ? 1 : 0;
        int available = 1;

        for (Replica replica : replicas.values()) {
            if (replica == leader) {
                continue;
            }
            // “可参与投票”= 在线（UP 或 SLOW）；DOWN 才是不可用。
            if (replica.status() != ReplicaStatus.DOWN) {
                available++;
            }
            if (replica.syncAndAppend(leader, entry)) {
                acks++;
            }
        }

        if (acks >= quorum) {
            // 多数派落盘 -> 提交；副本原子地判断“持有该条目且 commit 更低”才推进。
            for (Replica replica : replicas.values()) {
                replica.advanceCommitIfHolds(index);
            }
            WriteResult result = new WriteResult(WriteStatus.COMMITTED, shardId, requestId,
                    index, version, acks, quorum,
                    "多数派确认: acks=" + acks + " >= quorum=" + quorum
                            + "，提交位点推进到 index=" + index
                            + "，key=" + key + " version=" + version);
            settle(result);
            log.info("[{}] 写入提交 requestId={} key={} index={} version={} acks={}/{}（判定依据: 多数派落盘）",
                    shardId, requestId, key, index, version, acks, replicas.size());
            return result;
        }

        // 票数不足，区分两类：
        //  - 在线（非 DOWN）副本数本身凑不够多数派 -> REPLICA_UNAVAILABLE；
        //  - 副本在线数足够，但 SLOW 副本本轮未确认导致 acks 不足 -> INSUFFICIENT_ACKS。
        WriteStatus status = available < quorum
                ? WriteStatus.REPLICA_UNAVAILABLE
                : WriteStatus.INSUFFICIENT_ACKS;
        String reason = status == WriteStatus.REPLICA_UNAVAILABLE
                ? "副本不可用: 在线副本数=" + available + " < quorum=" + quorum
                        + "（实际 acks=" + acks + "），区别于在线但确认不足"
                : "确认不足: 在线副本数=" + available + " 够多数派，但本轮 acks=" + acks
                        + " < quorum=" + quorum + "（存在 SLOW 副本未确认），"
                        + "条目停留在 index=" + index + " 未提交，对外不可见";
        WriteResult failed = new WriteResult(status, shardId, requestId,
                index, LogEntry.NO_VERSION, acks, quorum, reason);
        settle(failed);
        log.warn("[{}] 写入未提交 requestId={} key={} index={} acks={}/{} available={} -> {}",
                shardId, requestId, key, index, acks, replicas.size(), available, status);
        return failed;
    }

    private void settle(WriteResult result) {
        try {
            idempotency.put(result);
        } catch (IOException e) {
            throw new IllegalStateException("幂等表持久化失败", e);
        }
    }

    /** 只读已提交（已应用到 leader 状态机）的数据。 */
    public VersionedValue read(String key) {
        return leader.get(key);
    }
}
