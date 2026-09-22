package com.github.h3yow.shardedreplicatedstorejava.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * 副本日志对齐工具，区分两条路径：
 *
 * <ol>
 *   <li><b>写入前在线同步 {@link #alignLogOnly}</b>：follower 只可能落后，
 *       不可能在已提交区间与 leader 分叉。因此只追加缺失后缀，
 *       <b>不截断、不推进/回退提交位点</b>。提交位点只能由协调者在本轮条目
 *       获得多数派后统一推进，避免并发写入把 follower 的 commit 回写。</li>
 *   <li><b>恢复性追赶 {@link #catchUp}</b>：副本宕机期间落后，或日志曾损坏/分叉。
 *       先以实际扫描到的一致前缀为基准截断损坏与分叉后缀，再按 leader 补齐缺失
 *       条目，最后按 leader 的提交位点推进 follower 并重放为可见状态。</li>
 * </ol>
 */
public final class ReplicaCatchup {

    private static final Logger log = LoggerFactory.getLogger(ReplicaCatchup.class);

    private ReplicaCatchup() {
    }

    /** 恢复性追赶：对齐（含截断分叉/损坏）并按 leader 提交位点推进。 */
    public static int catchUp(Replica follower, Replica leader) throws IOException {
        List<LogEntry> leaderEntries = leader.logEntries();

        PersistentLog.ScanResult scan = follower.persistentLog().scanWithReport();
        List<LogEntry> followerEntries = scan.prefix();
        long followerUsableTail = scan.lastConsistentIndex();
        if (!scan.clean()) {
            follower.persistentLog().truncateSuffix(followerUsableTail);
            log.warn("[{}][{}] 追赶前检测到日志损坏({}，行 {})，截断到最后一致位点 index={} 后按 leader={} 重建",
                    follower.shardId(), follower.replicaId(), scan.reason(),
                    scan.lineNumber(), followerUsableTail, leader.replicaId());
        }

        long commonPrefix = 0;
        for (int i = followerEntries.size() - 1; i >= 0; i--) {
            LogEntry local = followerEntries.get(i);
            if (local.index() <= leaderEntries.size()
                    && entriesAgree(local, leaderEntries.get((int) local.index() - 1))) {
                commonPrefix = local.index();
                break;
            }
        }
        if (commonPrefix < followerUsableTail) {
            follower.persistentLog().truncateSuffix(commonPrefix);
            log.info("[{}][{}] 截断分叉后缀: 保留 index<={}（丢弃 {} 条）",
                    follower.shardId(), follower.replicaId(), commonPrefix,
                    followerUsableTail - commonPrefix);
        }

        int appended = appendSuffix(follower, leaderEntries, commonPrefix);
        long alignedTail = commonPrefix + appended;

        if (follower.commitIndex() > alignedTail) {
            // 分叉/损坏截断使旧 commit 超过现存日志长度：恢复路径允许向下钳制。
            follower.resetCommitIndexForCatchup(alignedTail);
        }
        long leaderCommit = Math.min(leader.commitIndex(), leaderEntries.size());
        if (leaderCommit > follower.commitIndex()) {
            follower.advanceCommitDuringCatchup(leaderCommit);
        }
        log.info("[{}][{}] 恢复性追赶完成: leader={} 公共前缀={} 补齐={} commit={}（判定依据: 与leader逐index比对）",
                follower.shardId(), follower.replicaId(), leader.replicaId(),
                commonPrefix, appended, follower.commitIndex());
        return appended;
    }

    /**
     * 写入前在线同步：把 follower 缺失的、leader 上<strong>已提交</strong>的连续
     * 后缀补齐。不截断、不推进 follower 的提交位点（状态机重放由随后的提交传播
     * 统一完成）。已提交区间不可能与 leader 分叉，因此只需顺序追加，安全无缺号。
     *
     * @return 实际追加的条目数
     */
    public static int alignCommittedSuffix(Replica follower, Replica leader)
            throws IOException {
        List<LogEntry> leaderEntries = leader.logEntries();
        PersistentLog.ScanResult scan = follower.persistentLog().scanWithReport();
        if (!scan.clean()) {
            throw new LogCorruptionException(scan.reason(), scan.lineNumber(),
                    scan.lastConsistentIndex(),
                    "在线同步前发现 follower 日志损坏(" + scan.reason()
                            + ", 行=" + scan.lineNumber()
                            + ", 最后一致位点=" + scan.lastConsistentIndex()
                            + ")，需先执行恢复性追赶");
        }
        int appended = 0;
        for (LogEntry e : leaderEntries) {
            if (e.index() <= scan.lastConsistentIndex()) {
                continue;
            }
            follower.persistentLog().append(e);
            appended++;
        }
        if (appended > 0) {
            log.info("[{}][{}] 在线同步: followerTail={} 对齐 leader 尾部追加 {} 条（不截断、不推进commit）",
                    follower.shardId(), follower.replicaId(),
                    scan.lastConsistentIndex(), appended);
        }
        return appended;
    }

    /** 顺序追加 leaderEntries 中 index &gt; afterIndex 的条目，返回追加数量。 */
    private static int appendSuffix(Replica follower, List<LogEntry> leaderEntries,
                                    long afterIndex) throws IOException {
        int appended = 0;
        for (LogEntry entry : leaderEntries) {
            if (entry.index() <= afterIndex) {
                continue;
            }
            follower.persistentLog().append(entry);
            appended++;
        }
        return appended;
    }

    private static boolean entriesAgree(LogEntry a, LogEntry b) {
        return a.index() == b.index()
                && a.term() == b.term()
                && a.type() == b.type()
                && a.requestId().equals(b.requestId())
                && a.key().equals(b.key())
                && java.util.Objects.equals(a.value(), b.value())
                && a.version() == b.version();
    }
}
