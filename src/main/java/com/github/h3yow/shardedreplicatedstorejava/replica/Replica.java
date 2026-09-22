package com.github.h3yow.shardedreplicatedstorejava.replica;

import com.github.h3yow.shardedreplicatedstorejava.log.ReplicaLog;
import com.github.h3yow.shardedreplicatedstorejava.model.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * 一个副本节点上的一个分片：拥有持久日志、内存状态机与已应用位点。
 */
public class Replica {

    private static final Logger log = LoggerFactory.getLogger(Replica.class);

    private final String replicaId;
    private final String shardId;
    private final ReplicaLog rlog;
    private final StateMachine stateMachine;

    public Replica(String replicaId, String shardId, ReplicaLog rlog) {
        this.replicaId = replicaId;
        this.shardId = shardId;
        this.rlog = rlog;
        this.stateMachine = new StateMachine(replicaId + "/" + shardId);
    }

    public String replicaId() {
        return replicaId;
    }

    public String shardId() {
        return shardId;
    }

    public ReplicaLog log() {
        return rlog;
    }

    public StateMachine stateMachine() {
        return stateMachine;
    }

    /**
     * 接收一条条目并落盘；随后把状态机应用到日志末尾（追赶场景下可能一次追多条）。
     * 同 requestId 的重复条目由日志层幂等处理，返回既有条目，不会重复应用。
     */
    /**
     * 接收一条条目并落盘，但不立即应用到状态机。
     * 应用必须由协调者在达成多数派、推进 commitIndex 后通过 {@link #applyUpTo} 驱动，
     * 避免未确认数据对外可见。同 requestId 重复条目幂等返回既有条目。
     */
    public synchronized LogEntry accept(LogEntry entry) {
        LogEntry stored = rlog.append(entry);
        log.debug("[{}/{}] accept index={} requestId={} lastIndex={}",
                replicaId, shardId, stored.index(), stored.requestId(), rlog.lastIndex());
        return stored;
    }

    /** 把状态机从当前 appliedIndex 应用到 target（通常是领导副本的 commitIndex）。 */
    public synchronized void applyUpTo(long target) {
        long t = Math.min(target, rlog.lastIndex());
        while (stateMachine.appliedIndex() < t) {
            long next = stateMachine.appliedIndex() + 1;
            LogEntry e = rlog.entryAt(next)
                    .orElseThrow(() -> new IllegalStateException(
                            "missing entry at index " + next + " on " + replicaId + "/" + shardId));
            stateMachine.apply(e);
        }
    }

    /**
     * 从领导副本追赶：拉取 (lastIndex, leaderCommit] 区间内本副本缺失的条目，
     * 顺序落盘并应用。返回新追加的条目数。
     */
    public synchronized int catchUp(Replica leader, long leaderCommit) {
        long have = rlog.lastIndex();
        long target = Math.min(leaderCommit, leader.log().lastIndex());
        if (target <= have) {
            applyUpTo(target);
            return 0;
        }
        List<LogEntry> missing = leader.log().entriesAfter(have);
        int applied = 0;
        for (LogEntry e : missing) {
            if (e.index() > target) {
                break;
            }
            LogEntry stored = rlog.append(e);
            if (stored.index() == e.index()) {
                applied++;
            }
        }
        applyUpTo(target);
        log.info("[{}/{}] 追赶自 {}：本地位点 {} -> 领导提交位点 {}，新追加 {} 条",
                replicaId, shardId, leader.replicaId(), have, target, applied);
        return applied;
    }

    public synchronized long lastIndex() {
        return rlog.lastIndex();
    }

    /** 仅读取已应用（已提交）状态，未达 commitIndex 的脏数据不可见。 */
    public Optional<String> read(String key) {
        return stateMachine.get(key);
    }
}
