package com.github.h3yow.shardedreplicatedstorejava.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * 一个分片副本：本地追加日志 + 键值状态机 + 持久化提交位点。
 *
 * <p>可用性通过 {@link ReplicaStatus} 模拟：UP 正常接收追加；SLOW/DOWN 的
 * append 返回 false（协调者据此计票），状态本身不改变本地文件。</p>
 *
 * <p>{@link #recover()} 模拟重启：只信任 {@link PersistentLog#scanWithReport()}，
 * 日志损坏时停在最后一个一致位点，绝不跳过坏记录；随后把状态机重建到
 * min(最后一致位点, 提交位点) —— 未提交条目不对外可见。</p>
 */
public final class Replica {

    private static final Logger log = LoggerFactory.getLogger(Replica.class);

    private final String replicaId;
    private final String shardId;
    private final Path dir;
    private final PersistentLog persistentLog;
    private final Path commitFile;

    private KvStateMachine stateMachine = new KvStateMachine();
    private ReplicaStatus status = ReplicaStatus.UP;
    private long commitIndex;
    private long lastConsistentIndex;
    private boolean corrupted;
    private LogCorruptionReason corruptionReason;

    public Replica(String replicaId, String shardId, Path dataDir) throws IOException {
        this.replicaId = replicaId;
        this.shardId = shardId;
        this.dir = dataDir.resolve(shardId).resolve(replicaId);
        Files.createDirectories(this.dir);
        this.persistentLog = new PersistentLog(this.dir.resolve("log.txt"));
        this.commitFile = this.dir.resolve("commit.txt");
        if (Files.exists(commitFile)) {
            this.commitIndex = Long.parseLong(Files.readString(commitFile, StandardCharsets.UTF_8)
                    .trim());
        }
        // 启动即扫描一次，得到最后一致位点。
        PersistentLog.ScanResult initial = persistentLog.scanWithReport();
        this.lastConsistentIndex = initial.lastConsistentIndex();
        if (!initial.clean()) {
            this.corrupted = true;
            this.corruptionReason = initial.reason();
        }
        rebuildStateMachine();
    }

    private long currentTailIndex() throws IOException {
        return persistentLog.scanWithReport().lastConsistentIndex();
    }

    public String replicaId() {
        return replicaId;
    }

    public String shardId() {
        return shardId;
    }

    public ReplicaStatus status() {
        return status;
    }

    public void setStatus(ReplicaStatus status) {
        log.info("[{}][{}] 副本状态切换 -> {}（判定依据: 测试注入的可用性）",
                shardId, replicaId, status);
        this.status = status;
    }

    /**
     * 把条目追加到本地日志。
     *
     * @return true 表示成功落盘；false 表示未确认（SLOW 超时或 DOWN 不可用），不计入 ack。
     *         调用方可通过 {@link #status()} 区分“在线但慢（SLOW）”与“不可用（DOWN）”。
     */
    public synchronized boolean append(LogEntry entry) {
        if (status != ReplicaStatus.UP) {
            log.warn("[{}][{}] append 未确认: 副本状态={} index={} requestId={}",
                    shardId, replicaId, status, entry.index(), entry.requestId());
            return false;
        }
        try {
            persistentLog.append(entry);
            // 成功追加到一份此前一致的日志后，新日志必然仍一致；
            // 直接以文件实际扫描结果刷新缓存，避免任何陈旧标记挡住提交传播。
            PersistentLog.ScanResult scan = persistentLog.scanWithReport();
            this.lastConsistentIndex = scan.lastConsistentIndex();
            if (scan.clean()) {
                this.corrupted = false;
                this.corruptionReason = null;
            }
            log.debug("[{}][{}] 追加 index={} requestId={} version={}",
                    shardId, replicaId, entry.index(), entry.requestId(), entry.version());
            return true;
        } catch (IOException e) {
            throw new IllegalStateException("本地日志追加失败 " + dir, e);
        }
    }

    /**
     * 丢弃本地日志中 index &gt; {@code committedInclusive} 的<strong>未提交</strong>
     * 后缀（物理截断），并刷新最后一致位点。
     *
     * <p>用途：某一轮写入未获多数派确认时，条目可能已经落在 leader 或部分 follower
     * 本地（甚至跨重启仍在）。这些条目永远不会被提交；下一轮用<strong>新 requestId</strong>
     * 重提同一 key 时，必须先在每个参与副本上把这类残留清掉，再从已提交位点之后重新
     * 追加，否则残留会被当作缺失后缀同步给其他副本、越过提交判定被状态机应用，
     * 造成“失败值复活、新值被同版本幂等忽略”。</p>
     *
     * <p>状态机只应用到 {@code commitIndex}，而 {@code committedInclusive} 取协调者
     * （leader）的提交位点（单调不降、且不小于本副本 commit），因此截断不动任何已确认
     * 数据，已确认值不丢、不重。</p>
     *
     * @return 截断后的最后一致位点
     */
    public synchronized long discardUncommittedSuffix(long committedInclusive) throws IOException {
        long tailBefore = lastConsistentIndex;
        if (tailBefore > committedInclusive) {
            persistentLog.truncateSuffix(committedInclusive);
            PersistentLog.ScanResult scan = persistentLog.scanWithReport();
            this.lastConsistentIndex = scan.lastConsistentIndex();
            this.corrupted = !scan.clean();
            this.corruptionReason = scan.reason();
            log.info("[{}][{}] 丢弃未提交残留后缀: index {}..{} 截断（基点 commit={}，判定依据: 未达多数派的条目永不提交，新 requestId 重提前清场）",
                    shardId, replicaId, committedInclusive + 1, tailBefore, committedInclusive);
        }
        return lastConsistentIndex;
    }

    /**
     * 协调者派发写入时使用：原子地（1）清掉本副本所有未提交残留（index &gt; leader
     * commitIndex，它们只可能来自此前失败轮次），（2）补齐 follower 缺失的 leader
     * <em>已提交</em>后缀，（3）追加本轮条目。在副本内置锁内串行，保证日志既不缺号
     * 也不重复、失败值绝不混入已提交前缀。不推进提交位点（提交由多数派协调者在达成后
     * 统一推进）。
     *
     * @return true 表示本轮条目最终存在于本副本日志
     */
    public synchronized boolean syncAndAppend(Replica leader, LogEntry entry) {
        if (status != ReplicaStatus.UP) {
            // SLOW（在线但本轮不确认）与 DOWN（不可用）都不计入 ack；
            // 二者的区分由协调者按 status() 统计“在线副本数”完成。
            log.warn("[{}][{}] syncAndAppend 未确认: 副本状态={} index={}",
                    shardId, replicaId, status, entry.index());
            return false;
        }
        try {
            // 1) 清场：本副本上任何高于 leader 提交位点的条目都来自失败轮次
            //    （无论本轮是否重启过），先物理丢弃，杜绝失败值被对齐逻辑带走。
            long leaderCommit = leader.commitIndex();
            discardUncommittedSuffix(leaderCommit);
            // 2) 对齐已提交后缀（不截断已提交区间、不推进本副本 commit）。
            ReplicaCatchup.alignCommittedSuffix(this, leader);
            this.lastConsistentIndex = currentTailIndex();
            if (lastConsistentIndex >= entry.index()) {
                // 对齐后已持有同一位点条目：必须与本轮条目逐字段一致，否则说明
                // 已提交区间发生分叉（同 term 单 leader 不变量被破坏），拒绝静默混用。
                LogEntry local = persistentLog.scanWithReport()
                        .prefix().get((int) entry.index() - 1);
                if (!sameEntry(local, entry)) {
                    throw new IllegalStateException("在线同步发现已提交区间分叉: index="
                            + entry.index() + " 本地 requestId=" + local.requestId()
                            + " 本轮 requestId=" + entry.requestId()
                            + " (" + shardId + "/" + replicaId + ")");
                }
                return true;
            }
            // 3) 追加本轮条目（紧接已提交前缀，index 连续、version 单调）。
            return append(entry);
        } catch (IOException e) {
            throw new IllegalStateException("syncAndAppend 失败 " + dir, e);
        }
    }

    private static boolean sameEntry(LogEntry a, LogEntry b) {
        return a.index() == b.index()
                && a.term() == b.term()
                && a.type() == b.type()
                && a.requestId().equals(b.requestId())
                && a.key().equals(b.key())
                && java.util.Objects.equals(a.value(), b.value())
                && a.version() == b.version();
    }

    /**
     * 原子地“持有该条目则把提交位点推进到 targetIndex”（单调不降）。
     * 在同一把内置锁内完成快照判断与提交，避免并发写入时读到陈旧的
     * lastConsistentIndex / commitIndex。返回是否实际推进。
     */
    public synchronized boolean advanceCommitIfHolds(long targetIndex) {
        if (targetIndex <= commitIndex) {
            return false; // 已看到更高提交，单调跳过
        }
        if (targetIndex > lastConsistentIndex) {
            return false; // 本副本尚未持有该条目
        }
        setCommitIndex(targetIndex);
        return true;
    }

    public synchronized void setCommitIndex(long newCommitIndex) {
        if (newCommitIndex < commitIndex) {
            throw new IllegalStateException("提交位点不允许回退: " + commitIndex
                    + " -> " + newCommitIndex + " (" + shardId + "/" + replicaId + ")");
        }
        if (newCommitIndex > lastConsistentIndex) {
            throw new IllegalStateException("提交位点不能越过日志最后一致位点: commit="
                    + newCommitIndex + " lastConsistent=" + lastConsistentIndex);
        }
        this.commitIndex = newCommitIndex;
        try {
            Files.writeString(commitFile, Long.toString(newCommitIndex),
                    StandardCharsets.UTF_8);
            advanceStateMachine();
        } catch (IOException e) {
            throw new IllegalStateException("提交位点持久化失败 " + commitFile, e);
        }
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long lastLogIndex() {
        return lastConsistentIndex;
    }

    public boolean isCorrupted() {
        return corrupted;
    }

    public LogCorruptionReason corruptionReason() {
        return corruptionReason;
    }

    private List<LogEntry> consistentEntries() throws IOException {
        // 恢复语义：只取损坏点之前的一致前缀，绝不跳过坏行。
        return persistentLog.scanWithReport().prefix();
    }

    private void rebuildStateMachine() throws IOException {
        List<LogEntry> entries = consistentEntries();
        KvStateMachine sm = new KvStateMachine();
        long target = Math.min(commitIndex, lastConsistentIndex);
        int applied = 0;
        for (LogEntry entry : entries) {
            if (entry.index() > target) {
                break;
            }
            sm.apply(entry.index(), entry);
            applied++;
        }
        this.stateMachine = sm;
        log.info("[{}][{}] 重启重放完成: 最后一致位点={} commit={} 已应用条目数={} 可见位点={}",
                shardId, replicaId, lastConsistentIndex, commitIndex, applied, target);
    }

    private void advanceStateMachine() throws IOException {
        List<LogEntry> entries = consistentEntries();
        for (LogEntry entry : entries) {
            if (entry.index() <= stateMachine.appliedIndex()) {
                continue;
            }
            if (entry.index() > commitIndex) {
                break;
            }
            boolean changed = stateMachine.apply(entry.index(), entry);
            log.debug("[{}][{}] 提交应用 index={} key={} version={} 生效={}",
                    shardId, replicaId, entry.index(), entry.key(), entry.version(), changed);
        }
    }

    /**
     * 模拟重启：重新扫描日志并重建状态机。
     *
     * @return 恢复报告（停点、应用数、停止原因）
     */
    public synchronized RecoveryReport recover() throws IOException {
        boolean wasClean = !corrupted;
        PersistentLog.ScanResult scan = persistentLog.scanWithReport();
        LogCorruptionReason reason = scan.reason();
        this.lastConsistentIndex = scan.lastConsistentIndex();
        this.corrupted = !scan.clean();
        this.corruptionReason = reason;
        if (scan.clean()) {
            log.info("[{}][{}] 日志扫描干净: lastConsistentIndex={}",
                    shardId, replicaId, lastConsistentIndex);
        } else {
            log.warn("[{}][{}] 日志损坏，恢复停在 index={}，原因={}（行 {}）",
                    shardId, replicaId, lastConsistentIndex, reason, scan.lineNumber());
        }
        long before = stateMachine.appliedIndex();
        rebuildStateMachine();
        int appliedCount = (int) stateMachine.appliedIndex();
        RecoveryReport report = new RecoveryReport(shardId, replicaId,
                lastConsistentIndex, appliedCount, scan.clean(), reason);
        log.info("[{}][{}] RecoveryReport: lastConsistentIndex={} applied={} clean={} stopReason={}",
                shardId, replicaId, report.lastConsistentIndex(), report.appliedCount(),
                report.clean(), report.stopReason());
        log.debug("(recover) 重放前状态机位点={}", before);
        return report;
    }

    /** 恢复性追赶：对齐日志，按 leader 提交位点推进并重放。 */
    public synchronized int catchUpFrom(Replica leader) throws IOException {
        int applied = ReplicaCatchup.catchUp(this, leader);
        this.lastConsistentIndex = currentTailIndex();
        this.corrupted = false;
        this.corruptionReason = null;
        // 追赶后状态机按新日志与新 commit 从头重建。
        rebuildStateMachine();
        return applied;
    }

    /** 追赶流程内部使用：推进提交位点（绕过来自身陈旧 lastConsistent 缓存的越界校验）。 */
    synchronized void advanceCommitDuringCatchup(long newCommit) {
        try {
            this.commitIndex = newCommit;
            Files.writeString(commitFile, Long.toString(newCommit), StandardCharsets.UTF_8);
            advanceStateMachine();
        } catch (IOException e) {
            throw new IllegalStateException("追赶提交位点推进失败 " + commitFile, e);
        }
    }

    /** 修复性追赶专用：分叉截断后按对齐结果钳制提交位点（允许向下修正）。 */
    synchronized void resetCommitIndexForCatchup(long newCommit) {
        this.commitIndex = newCommit;
        try {
            Files.writeString(commitFile, Long.toString(newCommit), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("提交位点重置失败 " + commitFile, e);
        }
    }

    /**
     * 测试/运维支持：把本副本模拟为“落后到 {@code index}（含）位点”——物理截断其后的
     * 全部日志并把提交位点钳制到该位点、重建状态机。随后应通过
     * {@link #catchUpFrom(Replica)} 追赶。
     */
    public synchronized void simulateLagBehind(long index) throws IOException {
        persistentLog.truncateSuffix(index);
        resetCommitIndexForCatchup(index);
        recover();
    }

    PersistentLog persistentLog() {
        return persistentLog;
    }

    // ---- 测试支持：直接改写底层日志文件以模拟磁盘损坏 ----

    /** 读入日志全部文本（不做校验），供测试制造损坏。 */
    public String rawLogTextForTest() throws IOException {
        return Files.exists(persistentLog.file())
                ? Files.readString(persistentLog.file(), StandardCharsets.UTF_8)
                : "";
    }

    /** 用损坏后的文本整体覆盖日志文件。 */
    public void overwriteRawLogForTest(String raw) throws IOException {
        Files.writeString(persistentLog.file(), raw, StandardCharsets.UTF_8);
        // 文件被外部改写：丢弃内存中的损坏结论与状态机，使下一次 recover()
        // 从全新扫描开始（模拟在停机状态下磁盘被改动后重新启动）。
        this.corrupted = false;
        this.corruptionReason = null;
        this.stateMachine = new KvStateMachine(0);
    }

    public List<LogEntry> logEntries() {
        try {
            return consistentEntries();
        } catch (IOException e) {
            throw new IllegalStateException("读取日志失败 " + dir, e);
        }
    }

    public VersionedValue get(String key) {
        return stateMachine.get(key);
    }

    public KvStateMachine stateMachine() {
        return stateMachine;
    }
}
