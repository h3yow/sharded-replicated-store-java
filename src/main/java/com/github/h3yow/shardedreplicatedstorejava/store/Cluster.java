package com.github.h3yow.shardedreplicatedstorejava.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 本地模拟集群：持有当前世代拓扑与每个分片的 {@link ShardReplicaSet}，
 * 提供按 key 路由的写入/读取、整集群重启恢复、副本追赶以及再平衡。
 *
 * <p>磁盘布局：</p>
 * <pre>
 * dataDir/
 *   topology.txt                 当前世代拓扑（原子替换）
 *   &lt;shardId&gt;/
 *     idempotency.txt            requestId 首次结案表
 *     &lt;replicaId&gt;/log.txt        追加日志（带校验和）
 *     &lt;replicaId&gt;/commit.txt     提交位点
 * </pre>
 */
public final class Cluster {

    private static final Logger log = LoggerFactory.getLogger(Cluster.class);

    private final Path dataDir;
    private final TopologyManager topologyManager;
    private Topology topology;
    private final Map<String, ShardReplicaSet> shardSets = new LinkedHashMap<>();

    /**
     * 再平衡钩子：新拓扑分片集已激活、未提交残留已清场之后，<strong>正式迁移之前</strong>
     * 回调（参数为集群自身）。默认空操作；测试用它在“调整中途”注入副本故障/压力。
     */
    private java.util.function.Consumer<Cluster> beforeMigrateHook = c -> {
    };

    /** 安装再平衡迁移前钩子（返回 this 便于链式）。 */
    public Cluster setBeforeMigrateHook(java.util.function.Consumer<Cluster> hook) {
        this.beforeMigrateHook = hook == null ? c -> {
        } : hook;
        return this;
    }

    public Cluster(Path dataDir, Topology topology) throws IOException {
        this.dataDir = dataDir;
        Files.createDirectories(dataDir);
        this.topologyManager = new TopologyManager(dataDir.resolve("topology.txt"));
        TopologyValidator.validate(topology);
        this.topology = topology;
        topologyManager.save(topology);
        activateTopology(topology);
    }

    private Cluster(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        this.topologyManager = new TopologyManager(dataDir.resolve("topology.txt"));
    }

    /** 从磁盘恢复整个集群：读取拓扑，打开所有分片副本集（副本构造时即重放）。 */
    public static Cluster open(Path dataDir) throws IOException {
        Cluster cluster = new Cluster(dataDir);
        cluster.topology = cluster.topologyManager.load();
        cluster.activateTopology(cluster.topology);
        return cluster;
    }

    /** 为拓扑中的每个分片构建/打开副本集。老世代分片目录保留不删（迁移可追溯）。 */
    public void activateTopology(Topology newTopology) throws IOException {
        this.topology = newTopology;
        for (ShardRange range : newTopology.ranges()) {
            String shardId = range.shardId();
            if (!shardSets.containsKey(shardId)) {
                shardSets.put(shardId, new ShardReplicaSet(shardId,
                        newTopology.replicasOf(shardId), newTopology.epoch(), dataDir));
            }
        }
    }

    public Topology topology() {
        return topology;
    }

    public ShardReplicaSet shard(String shardId) {
        ShardReplicaSet set = shardSets.get(shardId);
        if (set == null) {
            throw new IllegalArgumentException("当前世代不存在分片: " + shardId);
        }
        return set;
    }

    public List<ShardReplicaSet> shards() {
        return List.copyOf(shardSets.values());
    }

    /** 按 key 路由写入；键落在未覆盖空间（理论上被拓扑校验排除）时显式拒绝。 */
    public WriteResult write(String requestId, String key, String value) {
        ShardRange range = topology.shardForKey(key);
        if (range == null) {
            throw new TopologyException(TopologyViolation.UNCOVERED_HASH_GAP,
                    "键 " + key + " 哈希位点 " + KeyHasher.hash(key)
                            + " 未被当前拓扑 epoch=" + topology.epoch() + " 覆盖");
        }
        return shard(range.shardId()).write(requestId, key, value);
    }

    /**
     * 批量严格全序写入：同批竞争者到齐后按 requestId 升序提交，
     * 并发最终版本确定可复现（通常用于同一分片/同一键的并发验证）。
     */
    public WriteResult writeInBatch(String requestId, String key, String value, int batchSize) {
        ShardRange range = topology.shardForKey(key);
        if (range == null) {
            throw new TopologyException(TopologyViolation.UNCOVERED_HASH_GAP,
                    "键 " + key + " 哈希位点 " + KeyHasher.hash(key)
                            + " 未被当前拓扑 epoch=" + topology.epoch() + " 覆盖");
        }
        return shard(range.shardId()).writeInBatch(requestId, key, value, batchSize);
    }

    public VersionedValue read(String key) {
        ShardRange range = topology.shardForKey(key);
        if (range == null) {
            return null;
        }
        return shard(range.shardId()).read(key);
    }

    /**
     * 再平衡（分片归属/副本归属动态调整）。
     *
     * <p>流程：</p>
     * <ol>
     *   <li>用 {@link TopologyValidator} 校验新拓扑：重叠、空洞、越界、非法副本集
     *       全部拒绝，旧拓扑保持不变；</li>
     *   <li>世代号必须递增，防止旧拓扑回滚覆盖新归属；</li>
     *   <li>枚举旧拓扑各分片 leader 状态机中的已确认数据，按新拓扑重新归属；
     *       对每个新分片以 MIGRATE 条目追加并要求多数派确认（版本沿用，不回退、
     *       不重生）；同一逻辑键只迁移一次，杜绝重复；</li>
     *   <li>全部迁移成功后才把新拓扑 staged 并原子替换 topology.txt，对外切换。</li>
     * </ol>
     *
     * @return 实际迁移（写入新分片）的键值对数量
     */
    public synchronized int reshard(List<ShardRange> newRanges,
                                    Map<String, List<String>> newReplicas) throws IOException {
        Topology newTopology = new Topology(topology.epoch() + 1, newRanges, newReplicas);
        TopologyValidator.validate(newTopology);
        if (newTopology.epoch() <= topology.epoch()) {
            throw new TopologyException(TopologyViolation.RANGE_INVALID_BOUNDARY,
                    "新世代号必须大于当前世代 " + topology.epoch());
        }
        log.info("== 开始再平衡: epoch {} -> {} 旧分片数={} 新分片数={}",
                topology.epoch(), newTopology.epoch(),
                topology.ranges().size(), newTopology.ranges().size());

        // 1) 收集旧拓扑的全部已确认数据（只从各分片 leader 的已应用状态机取）。
        //    必须在 activateTopology 切换 this.topology 之前完成，否则会按新拓扑
        //    去枚举分片，旧分片中的已确认数据将被漏迁。
        Topology oldTopology = this.topology;
        Map<String, VersionedValue> committed = new LinkedHashMap<>();
        for (ShardRange range : oldTopology.ranges()) {
            ShardReplicaSet oldSet = shardSets.get(range.shardId());
            if (oldSet == null) {
                continue;
            }
            for (Map.Entry<String, VersionedValue> e :
                    oldSet.leader().stateMachine().snapshot().entrySet()) {
                // 分片互不重叠，key 不可能出现在两个旧分片中；防御性检查：
                if (committed.containsKey(e.getKey())) {
                    throw new IllegalStateException("迁移前发现键跨分片重复: " + e.getKey());
                }
                committed.put(e.getKey(), e.getValue());
            }
        }

        // 新分片的副本集目录此时才创建，旧分片副本集原样保留（可追溯、可重放）。
        activateTopology(newTopology);

        // 迁移前对新拓扑下每个分片清掉未提交残留（复用的分片目录里可能留有上一次
        // 中途失败的在途条目），保证迁移只从干净的已提交位点之后开始。
        for (ShardRange range : newTopology.ranges()) {
            shardSets.get(range.shardId()).purgeAllUncommitted();
        }

        // 迁移前钩子：在“调整中途”注入故障/压力（测试支持，生产为空操作）。
        beforeMigrateHook.accept(this);

        // 2) 按新拓扑归属，对每个新分片做多数派 MIGRATE 写入。任一键失败都不替换
        //    拓扑文件，并把内存路由回滚到旧世代：对外仍按旧拓扑读写，已确认数据
        //    原样保留；失败的 MIGRATE 条目停留在未提交状态（随后可清场重试）。
        int migrated = 0;
        List<String> keysInOrder = new ArrayList<>(committed.keySet());
        keysInOrder.sort(String::compareTo); // 确定的迁移顺序，崩溃后重放也一致
        try {
            for (String key : keysInOrder) {
                VersionedValue vv = committed.get(key);
                ShardRange target = newTopology.shardForKey(key);
                if (target == null) {
                    throw new TopologyException(TopologyViolation.UNCOVERED_HASH_GAP,
                            "迁移时键 " + key + " 在新拓扑中无归属");
                }
                ShardReplicaSet targetSet = shardSets.get(target.shardId());
                String migrateRequestId = "migrate@" + oldTopology.epoch() + "->"
                        + newTopology.epoch() + ":" + key;
                long index = targetSet.migrateCommitted(migrateRequestId, key,
                        vv.value(), vv.version(), newTopology.epoch());
                migrated++;
                log.info("迁移 key={} version={} -> 新分片={} index={}（判定依据: 已确认状态机快照+新拓扑路由）",
                        key, vv.version(), target.shardId(), index);
            }
        } catch (RuntimeException e) {
            // 路由回滚：拓扑文件从未被替换，磁盘仍是旧世代；内存 this.topology
            // 也切回旧世代，保证重启/继续读写都以旧归属为准，已确认数据不丢不重。
            this.topology = oldTopology;
            log.error("再平衡中途失败，已回滚到旧世代 epoch={}（已迁移尝试 {} 键均为未达多数派的在途条目，不可见）: {}",
                    oldTopology.epoch(), migrated, e.getMessage());
            throw e;
        }

        // 3) 全部迁移完成：staged 新拓扑文件并原子替换。
        Path staged = dataDir.resolve("topology.epoch" + newTopology.epoch() + ".staged");
        new TopologyManager(staged).save(newTopology);
        topologyManager.commitSwap(staged);
        topology = newTopology;
        log.info("== 再平衡完成: 已迁移键数={} 当前 epoch={}", migrated, topology.epoch());
        return migrated;
    }

    /**
     * 模拟整集群重启：每个副本重新扫描本地日志、重放到最后一个一致位点。
     *
     * @return 各副本恢复报告
     */
    public List<RecoveryReport> restartAll() throws IOException {
        log.info("==== 模拟整集群重启，开始日志重放 ====");
        List<RecoveryReport> reports = new ArrayList<>();
        for (ShardReplicaSet set : shardSets.values()) {
            for (Replica replica : set.replicas()) {
                reports.add(replica.recover());
            }
        }
        long stopped = reports.stream().filter(r -> !r.clean()).count();
        log.info("==== 重启完成: 副本数={} 干净恢复={} 损坏停点={} ====",
                reports.size(), reports.size() - stopped, stopped);
        return reports;
    }

    /** 让某个分片的指定 follower 向 leader 追赶，返回补齐条目数。 */
    public int catchUpReplica(String shardId, String replicaId) throws IOException {
        ShardReplicaSet set = shard(shardId);
        Replica follower = set.replica(replicaId);
        if (follower == set.leader()) {
            throw new IllegalArgumentException("不能让 leader 追赶自己: " + replicaId);
        }
        return follower.catchUpFrom(set.leader());
    }

    public Path dataDir() {
        return dataDir;
    }
}
