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

        // 公共前缀必须“从头连续一致”：失败提交被中止后，follower 可能在已提交点
        // 之后持有与 leader 不同的分叉条目（同 index、不同内容），从尾部反查会误把
        // 分叉尾部当成公共前缀。因此逐 index 从头比较，第一个不一致处即为分叉点。
        long commonPrefix = 0;
        long comparable = Math.min(followerUsableTail, leaderEntries.size());
        for (long idx = 1; idx <= comparable; idx++) {
            LogEntry local = followerEntries.get((int) idx - 1);
            if (entriesAgree(local, leaderEntries.get((int) idx - 1))) {
                commonPrefix = idx;
            } else {
                break;
            }
        }
        if (commonPrefix < followerUsableTail) {
            follower.persistentLog().truncateSuffix(commonPrefix);
            log.info("[{}][{}] 截断分叉/损坏后缀: 保留 index<={}（丢弃 {} 条）",
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
     * 写入前在线同步：让 follower 日志对齐 leader，再由调用方追加本轮新条目。
     *
     * <p>正常情况下 follower 只可能落后：逐 index 从头比较，重叠前缀必然一致，
     * 只需顺序追加缺失后缀，<b>不截断、不推进 follower 的提交位点</b>。</p>
     *
     * <p>但失败提交被“安全中止”后，曾掉线的 follower 可能在自己的提交点之后
     * 持有一条已被 leader 丢弃的失败条目（同 index、不同内容）。重叠区一旦发现
     * 这种分叉，立即截断到最后一个一致位点再补齐 —— 截断点严格大于 follower 的
     * 提交位点，因此任何已确认数据都不受影响。</p>
     *
     * @return 实际追加的条目数（截断不计）
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
        List<LogEntry> followerEntries = scan.prefix();
        long followerTail = scan.lastConsistentIndex();

        // 重叠区必须逐 index 完全一致；首个不一致位点即分叉点（只会发生在 follower
        // 已提交点之后——已提交区间与 leader 冲突属于不可恢复的不变量破坏）。
        long agreePrefix = 0;
        long comparable = Math.min(followerTail, leaderEntries.size());
        for (long idx = 1; idx <= comparable; idx++) {
            if (entriesAgree(followerEntries.get((int) idx - 1),
                    leaderEntries.get((int) idx - 1))) {
                agreePrefix = idx;
            } else {
                break;
            }
        }
        if (agreePrefix < followerTail) {
            if (agreePrefix < follower.commitIndex()) {
                throw new IllegalStateException("在线同步发现已提交区间与 leader 分叉: "
                        + follower.shardId() + "/" + follower.replicaId()
                        + " commit=" + follower.commitIndex()
                        + " 一致前缀=" + agreePrefix + "，已确认数据冲突，拒绝继续");
            }
            follower.persistentLog().truncateSuffix(agreePrefix);
            follower.refreshTailAfterTruncation();
            log.warn("[{}][{}] 在线同步发现提交点之后的分叉，截断到 index={}（丢弃失败提交残留 {} 条，不影响已确认数据 commit={}）",
                    follower.shardId(), follower.replicaId(), agreePrefix,
                    followerTail - agreePrefix, follower.commitIndex());
            followerTail = agreePrefix;
        }

        int appended = 0;
        for (LogEntry e : leaderEntries) {
            if (e.index() <= followerTail) {
                continue;
            }
            follower.persistentLog().append(e);
            appended++;
        }
        if (appended > 0) {
            log.info("[{}][{}] 在线同步: followerTail={} 对齐 leader 尾部追加 {} 条（不截断已提交数据、不推进commit）",
                    follower.shardId(), follower.replicaId(),
                    followerTail, appended);
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
                && a.version() == b.version()
                && java.util.Objects.equals(a.winnerRequestId(), b.winnerRequestId());
    }
}
