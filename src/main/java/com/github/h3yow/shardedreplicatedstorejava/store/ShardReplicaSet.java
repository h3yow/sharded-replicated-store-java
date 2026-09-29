package com.github.h3yow.shardedreplicatedstorejava.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

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
    /** 普通路径的分片级互斥：决胜门内按 requestId 升序逐把获取，保证日志 index 连续。 */
    private final ReentrantLock appendLock = new ReentrantLock();
    /** 旧的批量严格全序接口（writeInBatch）仍使用，保持与现有用法兼容。 */
    private final DeterministicLock writeLock = new DeterministicLock();
    /** 普通写入路径的同键并发决胜门（无需调用方声明批大小）。 */
    private final ContentionGate contentionGate = new ContentionGate();

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
     * 协调一次写入（普通路径，<b>无需声明批大小</b>）。
     *
     * <p>同键并发由 {@link ContentionGate} 决胜：同一批竞争者中 requestId
     * 字典序最大者才有资格提交，其余直接得到 {@link WriteStatus#SUPERSEDED}
     * （不复制、不占版本）。决胜门再按 requestId 升序把成员串行放入分片临界区，
     * 因此不同键之间也不会并发分配日志 index。</p>
     */
    public WriteResult write(String requestId, String key, String value) {
        return contentionGate.runContended(key, requestId,
                ctx -> runInAppendLock(() -> doWrite(requestId, key, value, ctx)));
    }

    private WriteResult runInAppendLock(java.util.function.Supplier<WriteResult> body) {
        appendLock.lock();
        try {
            return body.get();
        } finally {
            appendLock.unlock();
        }
    }

    /**
     * 批量严格全序写入：声明本批 {@code batchSize} 个并发竞争者，
     * 到齐后整批按 requestId 升序依次提交，最终版本与日志位点确定可复现。
     * 保留以兼容现有批量用法；新场景可直接走普通 {@link #write} 路径
     * （无需声明批大小即可决胜）。
     */
    public WriteResult writeInBatch(String requestId, String key, String value, int batchSize) {
        // 批量路径维持原语义：声明了同批竞争者即按严格全序依次提交，
        // 不做 SUPERSEDED 判定，直接走 eligible=true 的内部提交。
        return writeLock.runOrderedInBatch(requestId, batchSize, () -> {
            try {
                return doWriteInternal(requestId, key, value, true);
            } catch (IOException e) {
                throw new IllegalStateException("批量写入中止失败尾部失败", e);
            }
        });
    }

    private WriteResult doWrite(String requestId, String key, String value,
                                ContentionGate.CohortContext ctx) {
        WriteResult prior = idempotency.get(requestId);
        if (prior != null) {
            WriteResult dup = new WriteResult(WriteStatus.DUPLICATE, shardId, requestId,
                    key, prior.logIndex(), prior.version(), prior.acks(), prior.quorum(),
                    "重复提交: requestId=" + requestId + " 已有首次结论 "
                            + prior.status() + "（index=" + prior.logIndex()
                            + ", version=" + prior.version() + "），直接复用，不重复应用");
            log.info("[{}] 重复提交 requestId={} -> 返回首次结论 status={} index={} version={}",
                    shardId, requestId, prior.status(), prior.logIndex(), prior.version());
            return dup;
        }

        // 同键并发决胜：只有本批 requestId 最大者有资格提交。落败者直接结案为
        // SUPERSEDED —— 不复制、不分配 index/version，结论与成功提交明确可区分。
        // 注意：决胜只在“同一批并发竞争者”之间进行；批次之间按封轮先后串行，
        // 后一批自然覆盖前一批，因此无需拿当前已确认胜者再做跨批比较。
        boolean eligible = requestId.equals(ctx.winnerRequestId());
        try {
            return doWriteInternal(requestId, key, value, eligible);
        } catch (IOException e) {
            throw new IllegalStateException("写入前中止失败尾部失败", e);
        }
    }

    /**
     * @param eligible 是否有资格真正提交；false 时直接结案 SUPERSEDED
     */
    private WriteResult doWriteInternal(String requestId, String key, String value,
                                        boolean eligible) throws IOException {
        if (!eligible) {
            WriteResult superseded = new WriteResult(WriteStatus.SUPERSEDED, shardId,
                    requestId, key, 0, LogEntry.NO_VERSION, 0, quorum,
                    "并发决胜落败: 同批存在 requestId 字典序更大的提交，"
                            + "本提交不复制、不占版本、不可见；判定依据=同批最大者胜");
            settle(superseded);
            log.info("[{}] 并发决胜落败 requestId={} key={} -> SUPERSEDED（不分配位点/版本）",
                    shardId, requestId, key);
            return superseded;
        }

        // 安全重提前置：任何新的提交尝试之前，先把本分片所有在线副本上
        // “提交位点之后的未决条目”全部中止。上一轮失败（确认不足/副本掉线）
        // 留在 leader/follower 日志里的值就此物理消失，之后无论追赶还是重启
        // 都不可能再读到它；已确认前缀原样保留。
        abortPendingWrites();

        // leader 不可用：无法产生任何日志条目，属于副本不可用而非确认不足。
        if (leader.status() != ReplicaStatus.UP) {
            WriteResult result = new WriteResult(WriteStatus.REPLICA_UNAVAILABLE, shardId,
                    requestId, key, 0, LogEntry.NO_VERSION, 0, quorum,
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
                    key, index, version, acks, quorum,
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
                key, index, LogEntry.NO_VERSION, acks, quorum, reason);
        settle(failed);
        log.warn("[{}] 写入未提交 requestId={} key={} index={} acks={}/{} available={} -> {}",
                shardId, requestId, key, index, acks, replicas.size(), available, status);
        return failed;
    }

    /**
     * 安全中止失败提交：截断 leader 与所有在线 follower 日志中“提交位点之后”
     * 的未决尾部。
     *
     * <p>leader 先截断，随后每个 UP follower 走恢复性对齐：其提交点之后若持有
     * 上一轮失败条目（与截断后的 leader 分叉），对齐会将其截断并补齐缺失。
     * DOWN 副本此刻无法处理，等其恢复后由 {@code syncAndAppend} 的分叉截断或
     * {@link Replica#catchUpFrom} 清掉同一失败尾部——无论走哪条路，失败值都不
     * 可能重新进入已提交区间。SLOW 副本本轮不参与，其残留同样在下一次在线同步
     * 时按分叉截断。</p>
     */
    private void abortPendingWrites() throws IOException {
        int leaderAborted = leader.abortUncommittedTail();
        for (Replica replica : replicas.values()) {
            if (replica == leader || replica.status() != ReplicaStatus.UP) {
                continue;
            }
            // catchUp 以 leader 为准做“从头逐 index 一致”的对齐：
            // follower 提交点之后的失败分叉被截断，已确认前缀保持不动。
            ReplicaCatchup.catchUp(replica, leader);
        }
        if (leaderAborted > 0) {
            log.warn("[{}] 安全中止失败提交: leader 丢弃 {} 条未决条目（index>commit={}），在线副本已对齐",
                    shardId, leaderAborted, leader.commitIndex());
        }
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

    /** 本分片幂等表中的全部首次结论（再平衡迁移用）。 */
    public java.util.Collection<WriteResult> allOutcomes() {
        return idempotency.allOutcomes();
    }

    /**
     * 再平衡迁移专用：把产生某键当前可见值的原始提交结论登记到本分片幂等表。
     * 只在该 requestId 尚未在本分片结案时写入。
     */
    public boolean adoptOutcomeIfAbsent(WriteResult original) {
        try {
            return idempotency.adoptIfAbsent(original);
        } catch (IOException e) {
            throw new IllegalStateException("迁移幂等结论持久化失败", e);
        }
    }
}
