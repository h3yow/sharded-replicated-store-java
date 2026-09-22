package com.github.h3yow.shardedreplicatedstorejava.cluster;

import com.github.h3yow.shardedreplicatedstorejava.exception.StoreException;
import com.github.h3yow.shardedreplicatedstorejava.log.FileReplicaLog;
import com.github.h3yow.shardedreplicatedstorejava.model.LogEntry;
import com.github.h3yow.shardedreplicatedstorejava.model.RecoveryReport;
import com.github.h3yow.shardedreplicatedstorejava.model.ShardSpec;
import com.github.h3yow.shardedreplicatedstorejava.model.Topology;
import com.github.h3yow.shardedreplicatedstorejava.model.WriteResult;
import com.github.h3yow.shardedreplicatedstorejava.recovery.RecoveryManager;
import com.github.h3yow.shardedreplicatedstorejava.replica.Replica;
import com.github.h3yow.shardedreplicatedstorejava.util.Checksums;
import com.github.h3yow.shardedreplicatedstorejava.util.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 本地模拟的分片复制集群。所有“副本”“网络”“磁盘”均由本地目录与内存对象模拟，
 * 不依赖任何外部服务。
 *
 * <p>目录布局：
 * <pre>
 * root/
 *   CURRENT
 *   gen-{g}/topology.properties
 *   gen-{g}/{shardId}__{replicaId}.log
 * </pre>
 */
public class Cluster {

    private static final Logger log = LoggerFactory.getLogger(Cluster.class);

    private final Path rootDir;
    private final TopologyStore store;
    private Topology topology;
    private final Map<String, ShardCoordinator> coordinators = new LinkedHashMap<>();

    public Cluster(Path rootDir) {
        this.rootDir = rootDir;
        this.store = new TopologyStore(rootDir);
    }

    public synchronized void bootstrap(Topology initial) {
        TopologyValidator.validate(initial);
        store.commit(initial);
        this.topology = initial;
        coordinators.clear();
        coordinators.putAll(buildCoordinators(initial));
        log.info("集群已引导：代际 {}，分片 {}，ringSize={}",
                initial.generation(), initial.shards().size(), initial.ringSize());
    }

    public synchronized Topology topology() {
        return topology;
    }

    public synchronized WriteResult write(String requestId, String key, String value,
                                          LogEntry.Operation op) {
        ShardCoordinator coord = coordinatorFor(key);
        return coord.write(requestId, key, value, op, topology.generation());
    }

    public Optional<String> read(String key) {
        ShardCoordinator coord;
        synchronized (this) {
            coord = coordinatorFor(key);
        }
        return coord.read(key);
    }

    /**
     * 应用新一代拓扑（动态分片 / 副本归属调整）。
     * <ol>
     *   <li>校验新拓扑：重叠、空洞、未覆盖、配置非法分别给出不同 reason；</li>
     *   <li>仅搬迁各旧分片的已确认条目（未确认尾部不迁移，避免脏数据）；</li>
     *   <li>按确定性顺序（旧分片起点、旧位点）为新分片重建连续日志；</li>
     *   <li>条数与 requestId 集合逐一核对：不丢失、不重复、不落错分片；</li>
     *   <li>全部成功后才原子切换 CURRENT 指针并替换内存协调者。</li>
     * </ol>
     */
    public synchronized Topology reconfigure(Topology next) {
        if (topology == null) {
            throw new StoreException("NO_TOPOLOGY", "cluster not bootstrapped");
        }
        if (next.generation() <= topology.generation()) {
            throw new StoreException("INVALID_GENERATION_ORDER",
                    "new generation " + next.generation()
                            + " must be greater than current " + topology.generation());
        }
        TopologyValidator.validate(next);
        long newGen = next.generation();

        // 1) 收集旧拓扑全部已确认条目，确定性排序
        List<LogEntry> migrated = new ArrayList<>();
        Set<String> oldRequestIds = new HashSet<>();
        for (ShardSpec oldShard : topology.shardsSortedByRange()) {
            ShardCoordinator c = coordinators.get(oldShard.shardId());
            for (LogEntry e : c.committedEntries()) {
                migrated.add(e);
                oldRequestIds.add(e.requestId());
            }
        }
        // committedEntries 已按 (旧分片起点, 旧位点) 有序遍历，天然确定

        // 2) 按新拓扑路由分组，并校验没有落错分片
        Map<String, List<LogEntry>> byNewShard = new LinkedHashMap<>();
        for (ShardSpec s : next.shards()) {
            byNewShard.put(s.shardId(), new ArrayList<>());
        }
        for (LogEntry e : migrated) {
            int hash = Hashing.hashToRing(e.key(), next.ringSize());
            ShardSpec target = next.shardForHash(hash);
            if (target == null) {
                throw new StoreException("MIGRATION_UNCOVERED_KEY",
                        "key " + e.key() + " hash=" + hash + " uncovered by new topology");
            }
            if (!target.range().contains(hash)) {
                throw new StoreException("MIGRATION_WRONG_SHARD",
                        "key " + e.key() + " would land in wrong shard " + target.shardId());
            }
            byNewShard.get(target.shardId()).add(e);
        }

        // 3) 为新一代每个副本构建全新日志（位点在新分片内从 1 重新连续编号）
        Map<String, ShardCoordinator> newCoordinators = buildCoordinators(next);
        Set<String> newRequestIds = new HashSet<>();
        long totalMigrated = 0;
        for (ShardSpec s : next.shardsSortedByRange()) {
            List<LogEntry> group = byNewShard.get(s.shardId());
            ShardCoordinator coord = newCoordinators.get(s.shardId());
            Replica leader = coord.leader();
            long index = 0;
            for (LogEntry origin : group) {
                index++;
                long checksum = Checksums.of(index, origin.term(), origin.requestId(),
                        origin.key(), origin.value(), origin.op().name());
                LogEntry ne = new LogEntry(index, origin.term(), origin.requestId(),
                        origin.key(), origin.value(), origin.op(), checksum);
                for (Replica r : coord.replicas()) {
                    r.accept(ne);
                }
                newRequestIds.add(ne.requestId());
                totalMigrated++;
            }
            // 搬迁数据来自旧代已确认多数派，视为在新代已确认，直接建立提交位点
            coord.rebuildCommitFromQuorum();
            if (coord.commitIndex() != index) {
                throw new StoreException("MIGRATION_COMMIT_MISMATCH",
                        "shard " + s.shardId() + " commitIndex " + coord.commitIndex()
                                + " != migrated " + index);
            }
            log.info("[重配] 新分片 {} 搬迁 {} 条，提交位点={}", s.shardId(), group.size(), index);
        }

        // 4) 不丢失、不重复核对
        if (totalMigrated != oldRequestIds.size() || !newRequestIds.equals(oldRequestIds)) {
            throw new StoreException("MIGRATION_DATA_MISMATCH",
                    "migrated requests differ: old=" + oldRequestIds.size()
                            + " new=" + totalMigrated);
        }

        // 5) 原子切换代际指针与内存状态
        store.commit(next);
        topology = next;
        coordinators.clear();
        coordinators.putAll(newCoordinators);
        log.info("重配完成：代际 {} -> {}，迁移已确认条目 {} 条，零丢失零重复",
                topology.generation() - 1, newGen, totalMigrated);
        return next;
    }

    /**
     * 模拟重启：从 CURRENT 指针读取代际，扫描并校验所有日志
     * （损坏停在最后一致位点并截断尾部），再按多数派重建每个分片的提交位点。
     */
    public synchronized RecoveryReport restart() {
        long gen = store.currentGeneration();
        RecoveryManager recovery = new RecoveryManager(rootDir);
        RecoveryReport report = recovery.recover(gen);

        Topology loaded = store.load(gen);
        this.topology = loaded;
        coordinators.clear();
        coordinators.putAll(buildCoordinators(loaded));
        for (ShardCoordinator c : coordinators.values()) {
            c.rebuildCommitFromQuorum();
        }
        log.info("重启完成：代际 {}，分片 {}，日志损坏={} 原因={}",
                gen, coordinators.size(), report.corruptionDetected(),
                report.corruptionReason());
        return report;
    }

    public ShardCoordinator coordinatorFor(String key) {
        int hash = Hashing.hashToRing(key, topology.ringSize());
        ShardSpec shard = topology.shardForHash(hash);
        if (shard == null) {
            throw new StoreException("KEY_UNMAPPED",
                    "key " + key + " hash=" + hash + " is not covered by any shard");
        }
        return coordinators.get(shard.shardId());
    }

    public synchronized void setReplicaDown(String replicaId, boolean down) {
        for (ShardCoordinator c : coordinators.values()) {
            if (c.replicas().stream().anyMatch(r -> r.replicaId().equals(replicaId))) {
                c.setReplicaDown(replicaId, down);
            }
        }
    }

    /** 接收故障注入：节点可用但写入确认失败（用于区分确认不足与节点不可用）。 */
    public synchronized void setAcceptFailing(String replicaId, boolean failing) {
        for (ShardCoordinator c : coordinators.values()) {
            if (c.replicas().stream().anyMatch(r -> r.replicaId().equals(replicaId))) {
                c.setAcceptFailing(replicaId, failing);
            }
        }
    }

    public synchronized Map<String, Long> commitIndexes() {
        Map<String, Long> out = new LinkedHashMap<>();
        coordinators.forEach((id, c) -> out.put(id, c.commitIndex()));
        return out;
    }

    public synchronized ShardCoordinator coordinator(String shardId) {
        return coordinators.get(shardId);
    }

    public void close() {
        // 文件句柄均即时关闭，无额外资源
    }

    private Map<String, ShardCoordinator> buildCoordinators(Topology t) {
        Map<String, ShardCoordinator> out = new HashMap<>();
        for (ShardSpec s : t.shards()) {
            List<Replica> replicas = new ArrayList<>();
            for (String replicaId : s.replicas()) {
                Path f = RecoveryManager.logFile(rootDir, t.generation(), s.shardId(), replicaId);
                FileReplicaLog rlog = new FileReplicaLog(f);
                // 构造副本时即执行位点校验：截断/校验和/缺号/乱序停在最后一致位点
                rlog.verifyAndRecover();
                replicas.add(new Replica(replicaId, s.shardId(), rlog));
            }
            // 副本列表首个节点作为领导（模拟固定领导）
            out.put(s.shardId(), new ShardCoordinator(s.shardId(), replicas, t.generation()));
        }
        return out;
    }
}
