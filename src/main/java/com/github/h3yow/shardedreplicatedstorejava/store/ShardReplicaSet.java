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
    // 普通写入路径的“同键决胜闸门”：调用方无需声明批大小，按 requestId 内容决胜。
    private final KeyContestGate contestGate = new KeyContestGate();

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
     * 协调一次普通写入（线程安全）。
     *
     * <p>同键并发决胜<strong>无需调用方声明批大小</strong>：请求先进入
     * {@link KeyContestGate}，静默窗口内到达的同键请求自动成批、按 requestId 升序
     * 逐个进入分片临界区。最终值只由本批请求内容决定（requestId 最大者的值），
     * 与到达顺序/线程调度无关；赢家拿到 {@link WriteStatus#COMMITTED}，落败但已落盘者
     * 拿到可区分的 {@link WriteStatus#SUPERSEDED}，谁都不会“拿到成功却读到别人的值”。</p>
     */
    public WriteResult write(String requestId, String key, String value) {
        // 快速幂等短路：已结案 requestId 不必再进决胜轮排队。
        if (idempotency.get(requestId) != null) {
            return duplicateResult(requestId);
        }
        KeyContestGate.Ticket ticket = contestGate.enter(key, requestId);
        if (ticket.duplicate()) {
            // 同一决胜轮内的重复 requestId：等先到者结案后复用首次结论，绝不二次应用。
            contestGate.awaitDuplicateSettled(ticket);
            return duplicateResult(requestId);
        }
        contestGate.awaitTurn(ticket);
        try {
            WriteResult result = writeLock.runOrdered(requestId,
                    () -> doWrite(requestId, key, value));
            return decorateContestResult(result, ticket);
        } finally {
            contestGate.finish(ticket);
        }
    }

    private WriteResult duplicateResult(String requestId) {
        WriteResult prior = idempotency.get(requestId);
        if (prior == null) {
            throw new IllegalStateException("重复请求等待后仍无首次结论: " + requestId);
        }
        WriteResult dup = new WriteResult(WriteStatus.DUPLICATE, shardId, requestId,
                prior.logIndex(), prior.version(), prior.acks(), prior.quorum(),
                prior.contestPosition(), prior.contestSize(), prior.winnerRequestId(),
                "重复提交: requestId=" + requestId + " 已有首次结论 "
                        + prior.status() + "（index=" + prior.logIndex()
                        + ", version=" + prior.version() + "），直接复用，不重复应用");
        log.info("[{}] 重复提交 requestId={} -> 返回首次结论 status={} index={} version={}",
                shardId, requestId, prior.status(), prior.logIndex(), prior.version());
        return dup;
    }

    /** 按决胜门票给已落盘的结论打 COMMITTED / SUPERSEDED 标记；失败结论原样透传。 */
    private WriteResult decorateContestResult(WriteResult r, KeyContestGate.Ticket ticket) {
        if (r.status() != WriteStatus.COMMITTED) {
            return r;
        }
        boolean iAmWinner = r.requestId().equals(ticket.winnerRequestId());
        if (iAmWinner) {
            return new WriteResult(WriteStatus.COMMITTED, r.shardId(), r.requestId(),
                    r.logIndex(), r.version(), r.acks(), r.quorum(),
                    ticket.position(), ticket.size(), ticket.winnerRequestId(),
                    "同键决胜获胜: 本批 " + ticket.size()
                            + " 个并发提交按 requestId 升序决胜，本请求 requestId="
                            + r.requestId() + " 最大，position=" + ticket.position()
                            + "，提交值即最终可见值 index=" + r.logIndex()
                            + " version=" + r.version());
        }
        return new WriteResult(WriteStatus.SUPERSEDED, r.shardId(), r.requestId(),
                r.logIndex(), r.version(), r.acks(), r.quorum(),
                ticket.position(), ticket.size(), ticket.winnerRequestId(),
                "同键决胜落败: 本批 " + ticket.size() + " 个并发提交按 requestId 升序决胜，"
                        + "本请求 requestId=" + r.requestId() + " position="
                        + ticket.position() + " 已多数派落盘（index=" + r.logIndex()
                        + " version=" + r.version() + "），但最终值由更大 requestId="
                        + ticket.winnerRequestId() + " 决定，本值不会对外可见");
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
            return duplicateResult(requestId);
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

        // 先确定本轮条目在 leader 日志中的 index 与新版本。
        // 安全重提前置：上一轮失败写入可能已在 leader（甚至跨重启）留下未提交条目，
        // 这些条目永不提交。用新 requestId 重提同一 key 时，必须先把 leader 上
        // index > leader.commitIndex 的残留物理截断，再从已提交位点之后重新追加：
        // 否则残留会占着连续 index 被在线同步带去 follower，重提的新版本反而被
        // 状态机以“同 version”幂等忽略，失败值就此“复活”对外可见。
        try {
            long leaderCommit = leader.commitIndex();
            leader.discardUncommittedSuffix(leaderCommit);
        } catch (IOException e) {
            throw new IllegalStateException("leader 未提交残留清理失败", e);
        }
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

    /**
     * 再平衡专用：把一个<strong>已确认</strong>键值以 MIGRATE 条目多数派写入本分片。
     *
     * <p>与普通写入走同一套安全规则：先丢弃目标 leader 上任何未提交残留（可能来自上
     * 一次中途失败的迁移尝试），index 从已提交位点连续分配；follower 经
     * {@link Replica#syncAndAppend} 清场 + 对齐已提交前缀后追加；只有多数派落盘才
     * 推进提交位点。迁移沿用原 version（不重生、不回退），因此失败的迁移条目与普通
     * 失败写入一样永远不会对外可见，也不会在重试时重复应用（同一逻辑键只迁移一次，
     * requestId 由世代与键确定）。</p>
     *
     * @return MIGRATE 条目在本分片日志中的 index；未获多数派时抛
     *         {@link IllegalStateException}（拓扑保持旧世代）
     */
    public long migrateCommitted(String migrateRequestId, String key, String value,
                                 long version, long epoch) {
        try {
            leader.discardUncommittedSuffix(leader.commitIndex());
        } catch (IOException e) {
            throw new IllegalStateException("迁移前 leader 未提交残留清理失败", e);
        }
        long index = leader.lastLogIndex() + 1;
        LogEntry entry = LogEntry.migrate(index, epoch, migrateRequestId, key, value, version);
        int acks = leader.append(entry) ? 1 : 0;
        for (Replica replica : replicas.values()) {
            if (replica == leader) {
                continue;
            }
            if (replica.syncAndAppend(leader, entry)) {
                acks++;
            }
        }
        if (acks < quorum) {
            throw new IllegalStateException("迁移未获多数派确认: key=" + key + " shard="
                    + shardId + " acks=" + acks + " quorum=" + quorum
                    + "（失败的 MIGRATE 条目未提交、不可见，可在旧拓扑上原样重试）");
        }
        for (Replica replica : replicas.values()) {
            replica.advanceCommitIfHolds(index);
        }
        log.info("[{}] 迁移提交 requestId={} key={} index={} version={} acks={}/{}（判定依据: 已确认快照+新拓扑路由+多数派落盘）",
                shardId, migrateRequestId, key, index, version, acks, replicas.size());
        return index;
    }

    /**
     * 丢弃本分片所有副本上 index 超过各自提交位点的未提交残留。
     * 再平衡打开（可能复用的）新分片目录后、迁移开始前调用一次，保证任何此前
     * 失败轮次的在途条目都不会混进迁移日志。
     */
    public void purgeAllUncommitted() {
        for (Replica replica : replicas.values()) {
            try {
                replica.discardUncommittedSuffix(replica.commitIndex());
            } catch (IOException e) {
                throw new IllegalStateException("迁移前副本未提交残留清理失败: "
                        + replica.replicaId(), e);
            }
        }
    }

    /** 只读已提交（已应用到 leader 状态机）的数据。 */
    public VersionedValue read(String key) {
        return leader.get(key);
    }
}
