package com.github.h3yow.shardedreplicatedstorejava.cluster;

import com.github.h3yow.shardedreplicatedstorejava.exception.DuplicateConflictException;
import com.github.h3yow.shardedreplicatedstorejava.exception.InsufficientAcksException;
import com.github.h3yow.shardedreplicatedstorejava.exception.ReplicaUnavailableException;
import com.github.h3yow.shardedreplicatedstorejava.model.LogEntry;
import com.github.h3yow.shardedreplicatedstorejava.model.WriteResult;
import com.github.h3yow.shardedreplicatedstorejava.replica.Replica;
import com.github.h3yow.shardedreplicatedstorejava.util.Checksums;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 单个分片的协调者，实现类 Raft 的多数派写入规则。
 *
 * <ul>
 *   <li>多数派 {@code quorum = floor(n/2) + 1}；只有多数派落盘才推进 commitIndex，
 *       数据才会应用到状态机并对外可见。</li>
 *   <li>{@code replica-down}：发起前可用副本数 &lt; quorum，抛
 *       {@link ReplicaUnavailableException}（REPLICAS_UNAVAILABLE）。</li>
 *   <li>{@code acks 不足}：发起时可用副本足够，但实际确认数 &lt; quorum，抛
 *       {@link InsufficientAcksException}（INSUFFICIENT_ACKS）。</li>
 *   <li>重复提交：同 requestId 且内容一致，复用首次结论（duplicate=true），
 *       不重复分配位点、不重复应用；内容冲突则抛
 *       {@link DuplicateConflictException}（DUPLICATE_REQUEST_CONFLICT）。</li>
 * </ul>
 */
public class ShardCoordinator {

    private static final Logger log = LoggerFactory.getLogger(ShardCoordinator.class);

    private final String shardId;
    private final List<Replica> replicas;
    private final Map<String, Replica> byId = new LinkedHashMap<>();
    private final Set<String> down = ConcurrentHashMap.newKeySet();
    /** 接收故障注入：节点视为可用（参与可用数统计），但 accept 抛错，用于区分 INSUFFICIENT_ACKS。 */
    private final Set<String> acceptFailing = ConcurrentHashMap.newKeySet();
    private final Map<String, LogEntry> committedByRequestId = new LinkedHashMap<>();
    private long commitIndex;
    private long term;

    public ShardCoordinator(String shardId, List<Replica> replicas) {
        this(shardId, replicas, 0);
    }

    public ShardCoordinator(String shardId, List<Replica> replicas, long term) {
        this.shardId = shardId;
        this.replicas = List.copyOf(replicas);
        for (Replica r : this.replicas) {
            byId.put(r.replicaId(), r);
        }
        this.term = term;
    }

    public String shardId() {
        return shardId;
    }

    public List<Replica> replicas() {
        return replicas;
    }

    public int quorum() {
        return replicas.size() / 2 + 1;
    }

    public synchronized long commitIndex() {
        return commitIndex;
    }

    public synchronized long term() {
        return term;
    }

    public Replica leader() {
        return replicas.get(0);
    }

    public synchronized WriteResult write(String requestId, String key, String value,
                                          LogEntry.Operation op, long writeTerm) {
        int available = availableReplicas().size();
        int q = quorum();

        // 1) 重复提交判定（已确认过的请求）：复用首次结论
        LogEntry committed = committedByRequestId.get(requestId);
        if (committed != null) {
            if (!samePayload(committed, key, value, op, writeTerm)) {
                throw new DuplicateConflictException("DUPLICATE_REQUEST_CONFLICT",
                        "requestId " + requestId + " already committed with a different payload");
            }
            log.info("[shard={}] 重复提交 requestId={} key={} 复用首次结论 commitIndex={}（不重复应用）",
                    shardId, requestId, key, committed.index());
            return new WriteResult(true, true, committed.index(), q, q,
                    "DUPLICATE_REQUEST: reuse first decision at index " + committed.index());
        }

        // 2) 发起前可用副本不足多数派
        if (available < q) {
            log.warn("[shard={}] 拒绝写入 requestId={} key={}：可用副本 {}/{} 不足多数派 {}",
                    shardId, requestId, key, available, replicas.size(), q);
            throw new ReplicaUnavailableException("REPLICAS_UNAVAILABLE",
                    "only " + available + " of " + replicas.size()
                            + " replicas available, quorum is " + q);
        }

        // 3) 未确认但领导日志里可能已有该 requestId（上次在确认后、返回前崩溃）：幂等续提
        Replica leader = leader();
        LogEntry existing = leader.log().findByRequestId(requestId).orElse(null);
        LogEntry entry;
        if (existing != null) {
            if (!samePayload(existing, key, value, op, writeTerm)) {
                throw new DuplicateConflictException("DUPLICATE_REQUEST_CONFLICT",
                        "requestId " + requestId + " exists on leader with a different payload");
            }
            entry = existing;
            log.info("[shard={}] 幂等续提 requestId={} 复用领导日志位点 {}",
                    shardId, requestId, entry.index());
        } else {
            long index = leader.lastIndex() + 1;
            long checksum = Checksums.of(index, writeTerm, requestId, key, value, op.name());
            entry = new LogEntry(index, writeTerm, requestId, key, value, op, checksum);
        }

        // 4) 向可用副本复制（领导先落盘，其余含前缀补齐），统计确认
        this.currentEntry = entry;
        List<Replica> targets = availableReplicas();
        int acks = 0;
        List<String> ackIds = new ArrayList<>();
        for (Replica r : targets) {
            try {
                if (acceptFailing.contains(r.replicaId())) {
                    throw new RuntimeException("injected accept failure on " + r.replicaId());
                }
                replicateFromLeader(r, entry.index());
                acks++;
                ackIds.add(r.replicaId());
            } catch (RuntimeException ex) {
                log.warn("[shard={}] 副本 {} 确认失败 requestId={} index={}：{}",
                        shardId, r.replicaId(), requestId, entry.index(), ex.getMessage());
            }
        }

        // 5) 确认不足多数派：保持未确认状态，可安全重试（追加是幂等的）
        if (acks < q) {
            log.warn("[shard={}] 确认不足 requestId={} key={}：acks={}/{}（可用={}），未推进 commitIndex={}",
                    shardId, requestId, key, acks, q, available, commitIndex);
            throw new InsufficientAcksException("INSUFFICIENT_ACKS",
                    "only " + acks + " acks, quorum is " + q
                            + "; entry at index " + entry.index() + " is uncommitted and safe to retry");
        }

        // 6) 多数派确认：推进 commitIndex，驱动全部可用副本应用到提交位点
        long newCommit = entry.index();
        long prevCommit = commitIndex;
        commitIndex = Math.max(commitIndex, newCommit);
        for (Replica r : targets) {
            r.applyUpTo(commitIndex);
        }
        // 登记新确认区间内全部请求（前缀补齐可能连带确认了更早的连续条目）
        for (long i = prevCommit + 1; i <= commitIndex; i++) {
            LogEntry e = leader.log().entryAt(i).orElseThrow();
            committedByRequestId.putIfAbsent(e.requestId(), e);
        }
        term = writeTerm;
        log.info("[shard={}] 多数派确认 requestId={} key={} index={} acks={}/{} 副本={} commitIndex={}",
                shardId, requestId, key, entry.index(), acks, q, ackIds, commitIndex);
        return new WriteResult(true, false, commitIndex, acks, q,
                "QUORUM_COMMITTED at index " + commitIndex + " with " + acks + "/" + q + " acks");
    }

    /** 把目标副本的日志补齐到 targetIndex（顺序、连续、幂等）。 */
    private void replicateFromLeader(Replica target, long targetIndex) {
        Replica leader = leader();
        if (target == leader) {
            // 领导自身：新条目必须先落领导日志；旧位点的补齐是空操作
            if (leader.log().entryAt(targetIndex).isEmpty()) {
                leader.accept(pendingEntry(targetIndex));
            }
            return;
        }
        long have = target.lastIndex();
        if (have < targetIndex) {
            for (LogEntry e : leader.log().entriesAfter(have)) {
                if (e.index() > targetIndex) {
                    break;
                }
                target.accept(e);
            }
        }
    }

    /** 取回本次待写入条目（幂等续提时为既有条目），由 write 调用前暂存。 */
    private LogEntry currentEntry;

    private LogEntry pendingEntry(long index) {
        if (currentEntry == null || currentEntry.index() != index) {
            throw new IllegalStateException("pending entry missing at index " + index);
        }
        return currentEntry;
    }

    public Optional<String> read(String key) {
        // 从已应用到 commitIndex 的可用副本读取（领导副本优先）
        List<Replica> targets = availableReplicas();
        if (targets.isEmpty()) {
            targets = replicas;
        }
        targets.get(0).applyUpTo(commitIndex);
        return targets.get(0).read(key);
    }

    /** 令指定落后副本从领导副本追赶至当前 commitIndex，返回新追加条目数。 */
    public synchronized int catchUpReplica(String replicaId) {
        Replica r = byId.get(replicaId);
        if (r == null) {
            throw new IllegalArgumentException("unknown replica " + replicaId + " for shard " + shardId);
        }
        int n = r.catchUp(leader(), commitIndex);
        log.info("[shard={}] 副本 {} 追赶完成：追加 {} 条，commitIndex={} appliedIndex={}",
                shardId, replicaId, n, commitIndex, r.stateMachine().appliedIndex());
        return n;
    }

    public void setAcceptFailing(String replicaId, boolean failing) {
        if (failing) {
            acceptFailing.add(replicaId);
            log.info("[shard={}] 副本 {} 注入接收故障（可用但确认失败）", shardId, replicaId);
        } else {
            acceptFailing.remove(replicaId);
            log.info("[shard={}] 副本 {} 解除接收故障", shardId, replicaId);
        }
    }

    public boolean isAcceptFailing(String replicaId) {
        return acceptFailing.contains(replicaId);
    }

    public void setReplicaDown(String replicaId, boolean isDown) {
        if (isDown) {
            down.add(replicaId);
            log.info("[shard={}] 副本 {} 标记为不可用", shardId, replicaId);
        } else {
            down.remove(replicaId);
            log.info("[shard={}] 副本 {} 恢复可用", shardId, replicaId);
        }
    }

    public boolean isReplicaDown(String replicaId) {
        return down.contains(replicaId);
    }

    private List<Replica> availableReplicas() {
        List<Replica> out = new ArrayList<>();
        for (Replica r : replicas) {
            if (!down.contains(r.replicaId())) {
                out.add(r);
            }
        }
        return out;
    }

    private static boolean samePayload(LogEntry e, String key, String value,
                                       LogEntry.Operation op, long writeTerm) {
        return e.term() == writeTerm
                && java.util.Objects.equals(e.key(), key)
                && java.util.Objects.equals(e.value(), value)
                && e.op() == op;
    }

    // ------------------------------------------------------------------
    // 恢复 / 重配支持
    // ------------------------------------------------------------------

    /** 已提交的连续日志条目（1..commitIndex），来自领导副本。 */
    public synchronized List<LogEntry> committedEntries() {
        List<LogEntry> out = new ArrayList<>();
        for (long i = 1; i <= commitIndex; i++) {
            leader().log().entryAt(i).ifPresent(out::add);
        }
        return out;
    }

    /**
     * 重启恢复后重建提交位点：
     * 先选日志最长的副本作为权威来源（模拟重选领导），再取“能在多数副本日志中
     * 找到”的最大连续位点 X 作为 commitIndex；超过 X 的尾部未确认条目一律截断，
     * 不会被重放。随后以权威副本 1..X 的条目重放各副本状态机并重建去重表。
     */
    public synchronized long rebuildCommitFromQuorum() {
        int q = quorum();
        // 权威副本：日志位点最大者；位点相同取 id 字典序最小，保证选择确定
        Replica authority = replicas.stream()
                .max(java.util.Comparator
                        .comparingLong(Replica::lastIndex)
                        .thenComparing(Replica::replicaId))
                .orElseThrow();
        long authorityLast = authority.log().lastIndex();
        long newCommit = 0;
        for (long x = 1; x <= authorityLast; x++) {
            final long idx = x;
            long count = replicas.stream()
                    .filter(r -> r.log().entryAt(idx).isPresent())
                    .count();
            if (count >= q) {
                newCommit = x;
            } else {
                break;
            }
        }
        // 截断各副本超过提交位点的未确认尾部（落后副本随后从权威副本补齐）
        for (Replica r : replicas) {
            if (r.log().lastIndex() > newCommit) {
                log.info("[shard={}] 恢复截断副本 {} 的未确认尾部：{} -> {}",
                        shardId, r.replicaId(), r.log().lastIndex(), newCommit);
                r.log().truncateAfter(newCommit);
            }
        }
        commitIndex = newCommit;
        committedByRequestId.clear();
        for (long i = 1; i <= newCommit; i++) {
            LogEntry e = authority.log().entryAt(i).orElseThrow();
            committedByRequestId.put(e.requestId(), e);
        }
        // 落后/被截断的副本先从权威副本补齐到提交位点，再统一重放到 newCommit
        if (authority != leader()) {
            log.info("[shard={}] 恢复重选权威副本：{}（原领导 {} 位点落后）",
                    shardId, authority.replicaId(), leader().replicaId());
        }
        for (Replica r : replicas) {
            if (r != authority && r.lastIndex() < newCommit) {
                r.catchUp(authority, newCommit);
            }
        }
        for (Replica r : replicas) {
            r.applyUpTo(newCommit);
        }
        log.info("[shard={}] 重建多数派提交位点 commitIndex={} quorum={}/{} 权威副本={}",
                shardId, newCommit, q, replicas.size(), authority.replicaId());
        return newCommit;
    }
}
