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
     * 协调者派发写入时使用：原子地（1）补齐 follower 缺失的 leader 已提交后缀，
     * （2）追加本轮条目。在副本内置锁内串行，对并发派发幂等，保证日志既不缺号
     * 也不重复。不推进提交位点（提交由多数派协调者在达成后统一推进）。
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
            if (currentTailIndex() >= entry.index()) {
                return true;
            }
            ReplicaCatchup.alignCommittedSuffix(this, leader);
            this.lastConsistentIndex = currentTailIndex();
            if (lastConsistentIndex >= entry.index()) {
                return true;
            }
            return append(entry);
        } catch (IOException e) {
            throw new IllegalStateException("syncAndAppend 失败 " + dir, e);
        }
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

    /**
     * 丢弃提交位点之后的全部未决（从未确认）条目，使本副本日志尾部恰好停在
     * commitIndex。用于“写入失败后安全重提”：失败条目绝不能留在任何副本上，
     * 否则后续同步/重启可能让它重新可见。已确认前缀（index &le; commitIndex）
     * 原样保留，状态机不变。
     *
     * @return 被丢弃的条目数
     */
    public synchronized int abortUncommittedTail() throws IOException {
        long tail = currentTailIndex();
        long aborted = tail - commitIndex;
        if (aborted > 0) {
            persistentLog.truncateSuffix(commitIndex);
            this.lastConsistentIndex = currentTailIndex();
            this.corrupted = false;
            this.corruptionReason = null;
            log.warn("[{}][{}] 中止未确认尾部: 截断 index>{} 共 {} 条（已确认数据保留，状态机不变）",
                    shardId, replicaId, commitIndex, aborted);
        }
        return (int) aborted;
    }

    /** 在线同步发生提交点之后分叉截断后，刷新内存中的一致位点缓存。 */
    synchronized void refreshTailAfterTruncation() throws IOException {
        this.lastConsistentIndex = currentTailIndex();
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
