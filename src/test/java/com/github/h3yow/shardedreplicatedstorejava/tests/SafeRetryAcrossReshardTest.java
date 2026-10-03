package com.github.h3yow.shardedreplicatedstorejava.tests;

import com.github.h3yow.shardedreplicatedstorejava.store.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 安全重提在<strong>分片归属调整（再平衡）前后</strong>的语义保障。
 *
 * <p>覆盖业务场景：</p>
 * <ol>
 *   <li>写失败 -> 换新 requestId 重提成功（版本严格递增、失败值清场）；</li>
 *   <li>再平衡（键被迁到别的分片）后，用旧 requestId 重提：
 *       <ul>
 *         <li>首次结论为失败：仍返回 DUPLICATE(首次失败)，绝不翻案成新提交，
 *             失败那轮的值在迁移/重启/追赶后都不被任何读取看到；</li>
 *         <li>首次结论为成功：仍返回 DUPLICATE(首次成功)，不多提交、不多出版本、
 *             不多写一条日志记录；</li>
 *       </ul>
 *   </li>
 *   <li>再平衡中途失败回滚后重试、整集群重启重放、落后副本追赶后再重提，结论不变；</li>
 *   <li>只有失败结案、从无已确认值的键，其失败 requestId 同样随键搬迁，结论不翻案；</li>
 *   <li>再平衡后换新 requestId 重提，版本严格大于此前所有已确认版本，无重复/跳号/回退。</li>
 * </ol>
 *
 * <p>日志以 [位点]/[版本]/[判定依据] 前缀打印关键中间状态，便于演练时核对。</p>
 */
class SafeRetryAcrossReshardTest {

    private static final Logger log = LoggerFactory.getLogger(SafeRetryAcrossReshardTest.class);

    @TempDir
    Path tempDir;

    // ---- 拓扑工具：2 分片 -> 4 分片，且显式挑选会跨分片迁移的键 ----

    private static List<ShardRange> twoShards() {
        int max = KeyHasher.HASH_SPACE;
        return List.of(new ShardRange("old-a", 0, max / 2),
                new ShardRange("old-b", max / 2, max));
    }

    private static List<ShardRange> fourShards() {
        int max = KeyHasher.HASH_SPACE;
        int q = max / 4;
        return List.of(new ShardRange("n0", 0, q),
                new ShardRange("n1", q, 2 * q),
                new ShardRange("n2", 2 * q, 3 * q),
                new ShardRange("n3", 3 * q, max));
    }

    private static Map<String, List<String>> replicas(String... shardIds) {
        Map<String, List<String>> m = new LinkedHashMap<>();
        for (String s : shardIds) {
            m.put(s, List.of(s + "-r0", s + "-r1", s + "-r2"));
        }
        return m;
    }

    private Cluster twoShardCluster() throws Exception {
        return new Cluster(tempDir,
                new Topology(1, twoShards(), replicas("old-a", "old-b")));
    }

    /** 在候选前缀中找到一个在 2->4 再平衡后分片 ID 发生变化的键。 */
    private static String keyThatMoves(Cluster cluster, String prefix) {
        String before = null;
        for (int i = 0; i < 10_000; i++) {
            String candidate = prefix + "-" + i;
            String oldShard = cluster.topology().shardForKey(candidate).shardId();
            String newShard = null;
            for (ShardRange r : fourShards()) {
                if (r.contains(KeyHasher.hash(candidate))) {
                    newShard = r.shardId();
                    break;
                }
            }
            if (newShard != null && !newShard.equals(oldShard)) {
                before = candidate;
                log.info("[位点] 选用跨分片迁移键 {} hash={} 旧分片={} -> 新分片={}",
                        candidate, KeyHasher.hash(candidate), oldShard, newShard);
                return candidate;
            }
        }
        throw new IllegalStateException("未找到跨分片迁移键: " + prefix);
    }

    private int reshardToFour(Cluster cluster) throws Exception {
        int migrated = cluster.reshard(fourShards(), replicas("n0", "n1", "n2", "n3"));
        log.info("[位点] 再平衡完成 epoch={} 迁移键数={}", cluster.topology().epoch(), migrated);
        return migrated;
    }

    private void assertNoValueAnywhere(Cluster cluster, String bannedValue) {
        for (ShardReplicaSet set : cluster.shards()) {
            for (Replica r : set.replicas()) {
                assertThat(r.logEntries().stream().noneMatch(e -> bannedValue.equals(e.value())))
                        .as("任何副本日志不得出现失败值 %s (%s/%s)",
                                bannedValue, set.shardId(), r.replicaId())
                        .isTrue();
            }
        }
    }

    @Test
    void failedFirstConclusionStaysFailedAfterReshardAndFailedValueNeverResurfaces()
            throws Exception {
        Cluster cluster = twoShardCluster();
        String key = keyThatMoves(cluster, "failkey");
        String oldShard = cluster.topology().shardForKey(key).shardId();
        ShardReplicaSet set = cluster.shard(oldShard);

        // 1) 已确认基准版本 v1/version=1。
        WriteResult base = cluster.write("req-base", key, "v1");
        assertThat(base.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(base.version()).isEqualTo(1);

        // 2) 两个 follower 掉线：写入失败（REPLICA_UNAVAILABLE），失败条目只在 leader
        //    本地残留，永不提交。
        set.replica(oldShard + "-r1").setStatus(ReplicaStatus.DOWN);
        set.replica(oldShard + "-r2").setStatus(ReplicaStatus.DOWN);
        WriteResult failed = cluster.write("req-fail", key, "EVIL-FAILED");
        assertThat(failed.status()).isEqualTo(WriteStatus.REPLICA_UNAVAILABLE);
        assertThat(failed.version()).isEqualTo(LogEntry.NO_VERSION);
        assertThat(cluster.read(key).value()).isEqualTo("v1");
        log.warn("[判定依据] 首次失败 requestId={} status={} 残留index={}（未提交，当前对外仍 v1）",
                failed.requestId(), failed.status(), failed.logIndex());

        // 3) 恢复后【换新 requestId】重提成功，version=2 严格大于已确认基准 1。
        set.replica(oldShard + "-r1").setStatus(ReplicaStatus.UP);
        set.replica(oldShard + "-r2").setStatus(ReplicaStatus.UP);
        WriteResult good = cluster.write("req-good", key, "v2-good");
        assertThat(good.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(good.version()).isEqualTo(2);
        assertThat(cluster.read(key).value()).isEqualTo("v2-good");
        log.info("[版本] 新 requestId={} 重提成功 index={} version=2（严格 > 已确认基准 1）",
                good.requestId(), good.logIndex());

        // 4) 再平衡，键被迁到别的分片。
        String newShard = null;
        for (ShardRange r : fourShards()) {
            if (r.contains(KeyHasher.hash(key))) {
                newShard = r.shardId();
            }
        }
        assertThat(newShard).isNotEqualTo(oldShard);
        reshardToFour(cluster);
        assertThat(cluster.topology().shardForKey(key).shardId()).isEqualTo(newShard);
        assertThat(cluster.read(key).value()).isEqualTo("v2-good");
        assertThat(cluster.read(key).version()).isEqualTo(2);
        log.info("[位点/版本] 再平衡后 key={} 归属 {} 读=v2-good/2（MIGRATE 沿用版本，失败值未随迁）",
                key, newShard);

        // 5) 用当初失败的旧 requestId 重提：必须是 DUPLICATE(首次失败)，
        //    不能变成新的成功提交；位点/版本沿用首次失败结论（index=残留位点，version=-1）。
        WriteResult replayedFail = cluster.write("req-fail", key, "EVIL-FAILED-NEWARG");
        assertThat(replayedFail.status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(replayedFail.logIndex()).isEqualTo(failed.logIndex());
        assertThat(replayedFail.version()).isEqualTo(LogEntry.NO_VERSION);
        // 失败那轮的值没有复活；即便本次重提换了“新值”也绝不应用。
        assertThat(cluster.read(key).value()).isEqualTo("v2-good");
        assertThat(cluster.read(key).version()).isEqualTo(2);
        log.warn("[判定依据] 再平衡后旧失败 requestId 重提 -> DUPLICATE(首次={}) index={}，未翻案、未应用，读仍 v2-good/2",
                failed.status(), replayedFail.logIndex());

        // 6) 新分片日志不得多出任何条目（结论来自幂等表短路，不写日志），
        //    且全集群任何副本都没有失败值。
        long entriesAfterReshard = cluster.shard(newShard).leader().logEntries().size();
        cluster.write("req-fail", key, "EVIL-FAILED-NEWARG");
        assertThat(cluster.shard(newShard).leader().logEntries()).hasSize((int) entriesAfterReshard);
        assertNoValueAnywhere(cluster, "EVIL-FAILED");
        assertNoValueAnywhere(cluster, "EVIL-FAILED-NEWARG");
        log.info("[判定依据] 旧失败 requestId 重提不新增日志（{} 条不变），全集群无失败值",
                entriesAfterReshard);

        // 7) 整集群重启重放后结论仍成立。
        cluster.restartAll();
        Cluster reopened = Cluster.open(tempDir);
        assertThat(reopened.topology().epoch()).isEqualTo(2);
        assertThat(reopened.write("req-fail", key, "X").status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(reopened.read(key).value()).isEqualTo("v2-good");
        log.info("[判定依据] 重启重放+重载后旧失败 requestId 仍 DUPLICATE，失败值未复活");

        // 8) 落后副本追赶后再重提：结论不变，追赶内容不含失败值。
        ShardReplicaSet newSet = reopened.shard(newShard);
        Replica laggard = newSet.replicas().get(1);
        laggard.simulateLagBehind(0);
        int appended = reopened.catchUpReplica(newShard, laggard.replicaId());
        assertThat(appended).isGreaterThan(0);
        assertThat(laggard.get(key).value()).isEqualTo("v2-good");
        assertThat(laggard.logEntries().stream().noneMatch(e -> "EVIL-FAILED".equals(e.value())))
                .isTrue();
        assertThat(reopened.write("req-fail", key, "X").status()).isEqualTo(WriteStatus.DUPLICATE);
        log.info("[位点/版本] 落后副本 {} 追赶补齐 {} 条后值=v2-good/2、无失败值，旧失败 requestId 仍 DUPLICATE",
                laggard.replicaId(), appended);
    }

    @Test
    void committedFirstConclusionStaysCommittedAfterReshardWithoutExtraVersionOrRecord()
            throws Exception {
        Cluster cluster = twoShardCluster();
        String key = keyThatMoves(cluster, "goodkey");
        String oldShard = cluster.topology().shardForKey(key).shardId();

        WriteResult first = cluster.write("req-ok", key, "good-v1");
        assertThat(first.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(first.version()).isEqualTo(1);
        long oldIndex = first.logIndex();
        log.info("[位点/版本] 首次成功 requestId={} 分片={} index={} version=1",
                first.requestId(), oldShard, oldIndex);

        reshardToFour(cluster);
        String newShard = cluster.topology().shardForKey(key).shardId();
        assertThat(newShard).isNotEqualTo(oldShard);
        long newShardEntriesBefore = cluster.shard(newShard).leader().logEntries().size();

        // 再平衡后重提旧成功 requestId：DUPLICATE(首次成功)，返回首次位点/版本，
        // 不产生新版本、不多写记录。
        WriteResult replay = cluster.write("req-ok", key, "good-v1");
        assertThat(replay.status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(replay.logIndex()).isEqualTo(oldIndex);
        assertThat(replay.version()).isEqualTo(1);
        assertThat(cluster.read(key).version()).isEqualTo(1);
        assertThat(cluster.read(key).value()).isEqualTo("good-v1");
        assertThat(cluster.shard(newShard).leader().logEntries())
                .as("重提旧成功 requestId 不许多写一条日志")
                .hasSize((int) newShardEntriesBefore);
        log.info("[判定依据] 再平衡后旧成功 requestId 重提 -> DUPLICATE(首次 index={} ver=1)，新分片日志仍 {} 条、版本仍 1",
                oldIndex, newShardEntriesBefore);

        // 重启 + 重载后再重提，结论一致。
        cluster.restartAll();
        Cluster reopened = Cluster.open(tempDir);
        WriteResult replay2 = reopened.write("req-ok", key, "good-v1");
        assertThat(replay2.status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(replay2.version()).isEqualTo(1);
        assertThat(reopened.read(key).version()).isEqualTo(1);
        log.info("[判定依据] 重启重放后旧成功 requestId 仍 DUPLICATE/ver=1，未重复应用");
    }

    @Test
    void reshardFailureRollbackThenRetryPreservesAllConclusions() throws Exception {
        Cluster cluster = twoShardCluster();
        // 选两个落在 old-a 不同四等分区间的键，使 epoch=2 的 n0（或 n1）必然承接它们，
        // 注入目标分片故障即可让迁移在多数派上失败。这里直接固定键并定位其目标分片。
        String keyA = "alpha";
        String keyB = "beta";
        String targetShard = null;
        for (ShardRange r : fourShards()) {
            if (r.contains(KeyHasher.hash(keyA))) {
                targetShard = r.shardId();
            }
        }
        // 若两个键不同目标分片，则对 keyA 的目标分片注入故障即可在迁移 keyA 时失败。
        log.info("[位点] keyA={} hash={} keyB={} hash={}",
                keyA, KeyHasher.hash(keyA), keyB, KeyHasher.hash(keyB));

        // keyA：先失败再换新 requestId 成功（version=2），存在失败与成功两条结论。
        ShardReplicaSet setA = cluster.shard(cluster.topology().shardForKey(keyA).shardId());
        cluster.write("a-base", keyA, "a-v1");
        setA.replicas().get(1).setStatus(ReplicaStatus.DOWN);
        setA.replicas().get(2).setStatus(ReplicaStatus.DOWN);
        WriteResult aFailed = cluster.write("a-fail", keyA, "A-EVIL");
        assertThat(aFailed.status()).isEqualTo(WriteStatus.REPLICA_UNAVAILABLE);
        setA.replicas().get(1).setStatus(ReplicaStatus.UP);
        setA.replicas().get(2).setStatus(ReplicaStatus.UP);
        WriteResult aGood = cluster.write("a-good", keyA, "a-v2");
        assertThat(aGood.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(aGood.version()).isEqualTo(2);
        // keyB：一次普通成功。
        WriteResult bGood = cluster.write("b-good", keyB, "b-v1");
        assertThat(bGood.status()).isEqualTo(WriteStatus.COMMITTED);

        // 中途故障：目标分片两 follower DOWN -> 迁移该键时多数派不足，再平衡回滚。
        final String failTarget = targetShard;
        cluster.setBeforeMigrateHook(c -> {
            ShardReplicaSet t = c.shard(failTarget);
            t.replica(failTarget + "-r1").setStatus(ReplicaStatus.DOWN);
            t.replica(failTarget + "-r2").setStatus(ReplicaStatus.DOWN);
        });
        assertThatThrownBy(() -> reshardToFour(cluster))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("多数派");
        assertThat(cluster.topology().epoch()).isEqualTo(1);
        assertThat(Cluster.open(tempDir).topology().epoch()).isEqualTo(1);
        log.warn("[判定依据] 再平衡中途失败并回滚 epoch 仍=1（磁盘拓扑未替换）");

        // 回滚后旧拓扑上结论不变：失败 requestId 仍失败、成功 requestId 不重复。
        assertThat(cluster.write("a-fail", keyA, "A-EVIL").status())
                .isEqualTo(WriteStatus.DUPLICATE);
        assertThat(cluster.write("a-good", keyA, "a-v2").status())
                .isEqualTo(WriteStatus.DUPLICATE);
        assertThat(cluster.read(keyA).value()).isEqualTo("a-v2");
        assertThat(cluster.read(keyA).version()).isEqualTo(2);

        // 恢复目标副本、撤销注入后重试再平衡成功；结论随键搬迁。
        cluster.setBeforeMigrateHook(null);
        cluster.shard(failTarget).replica(failTarget + "-r1").setStatus(ReplicaStatus.UP);
        cluster.shard(failTarget).replica(failTarget + "-r2").setStatus(ReplicaStatus.UP);
        int migrated = reshardToFour(cluster);
        assertThat(cluster.topology().epoch()).isEqualTo(2);
        assertThat(migrated).isEqualTo(2);
        log.info("[位点] 回滚后重试再平衡成功，迁移键数={}（幂等结论导入在重试时去重）", migrated);

        assertThat(cluster.write("a-fail", keyA, "A-EVIL").status())
                .as("失败 requestId 跨一次失败再平衡后仍保留首次失败结论")
                .isEqualTo(WriteStatus.DUPLICATE);
        assertThat(cluster.write("a-good", keyA, "a-v2").status())
                .as("成功 requestId 不重复应用")
                .isEqualTo(WriteStatus.DUPLICATE);
        assertThat(cluster.write("b-good", keyB, "b-v1").status())
                .isEqualTo(WriteStatus.DUPLICATE);
        assertThat(cluster.read(keyA).value()).isEqualTo("a-v2");
        assertThat(cluster.read(keyA).version()).isEqualTo(2);
        assertThat(cluster.read(keyB).value()).isEqualTo("b-v1");
        assertNoValueAnywhere(cluster, "A-EVIL");
        log.info("[判定依据] 回滚->重试成功后：a-fail 仍 DUPLICATE(失败)、a-good/b-good DUPLICATE(成功)，A-EVIL 全集群不存在");
    }

    @Test
    void failedOnlyKeyWithoutCommittedValueCarriesFailureConclusionAcrossReshard()
            throws Exception {
        Cluster cluster = twoShardCluster();
        String key = keyThatMoves(cluster, "onlyfail");
        String oldShard = cluster.topology().shardForKey(key).shardId();
        ShardReplicaSet set = cluster.shard(oldShard);

        // 该键从无任何已确认值；唯一一次写入失败。
        set.replicas().get(1).setStatus(ReplicaStatus.DOWN);
        set.replicas().get(2).setStatus(ReplicaStatus.DOWN);
        WriteResult failed = cluster.write("only-fail", key, "GHOST");
        assertThat(failed.status()).isEqualTo(WriteStatus.REPLICA_UNAVAILABLE);
        assertThat(cluster.read(key)).isNull();
        set.replicas().get(1).setStatus(ReplicaStatus.UP);
        set.replicas().get(2).setStatus(ReplicaStatus.UP);

        // 再平衡：无 KV 可迁，但失败结论必须随键搬到新分片。
        int migrated = reshardToFour(cluster);
        assertThat(migrated).isZero();
        String newShard = cluster.topology().shardForKey(key).shardId();
        assertThat(newShard).isNotEqualTo(oldShard);
        assertThat(cluster.read(key)).isNull();

        WriteResult replay = cluster.write("only-fail", key, "GHOST-2");
        assertThat(replay.status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(replay.logIndex()).isEqualTo(failed.logIndex());
        assertThat(cluster.read(key)).isNull();

        // 新归属分片的任何副本都不得迁移/写入失败值（它只可能随已确认 MIGRATE 流动）。
        for (Replica r : cluster.shard(newShard).replicas()) {
            assertThat(r.logEntries().stream()
                    .noneMatch(e -> "GHOST".equals(e.value()) || "GHOST-2".equals(e.value())))
                    .as("新归属分片 %s/%s 不得出现失败值", newShard, r.replicaId())
                    .isTrue();
            assertThat(r.get(key)).as("失败值不得被任何副本状态机应用（不可读）").isNull();
        }
        // 旧分片上失败条目即便物理残留也始终未提交：leader 状态机从未应用，重启亦不可见。
        assertThat(cluster.shard(oldShard).leader().get(key))
                .as("旧分片 leader 状态机不得应用未提交失败值")
                .isNull();
        assertThat(cluster.shard(oldShard).leader().commitIndex())
                .as("失败条目不得推进旧分片提交位点")
                .isZero();
        log.warn("[判定依据] 从无已确认值的键：失败 requestId 随键搬迁后重提仍 DUPLICATE(失败)，GHOST 未冒头（迁移键数=0，结论单独随迁；旧分片未提交残留不可见、不迁移）");
    }

    @Test
    void newRequestIdAfterReshardGetsVersionStrictlyGreaterThanAllCommitted()
            throws Exception {
        Cluster cluster = twoShardCluster();
        String key = keyThatMoves(cluster, "verkey");
        cluster.write("v1-req", key, "v1");
        cluster.write("v2-req", key, "v2");
        long versionBefore = cluster.read(key).version();
        assertThat(versionBefore).isEqualTo(2);

        reshardToFour(cluster);
        assertThat(cluster.read(key).version()).isEqualTo(2);

        // 再平衡后用全新 requestId 再写：version 必须严格大于此前所有已确认版本（=2），
        // 不允许重复/跳号/回退；新分片日志位点在 MIGRATE 之后连续分配。
        WriteResult v3 = cluster.write("v3-req", key, "v3");
        assertThat(v3.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(v3.version()).isEqualTo(versionBefore + 1);
        assertThat(v3.version()).isEqualTo(3);
        assertThat(cluster.read(key).value()).isEqualTo("v3");
        assertThat(cluster.read(key).version()).isEqualTo(3);
        log.info("[版本] 再平衡后新 requestId={} 提交 version=3（严格 > 迁移沿用版本 2），index={}",
                v3.requestId(), v3.logIndex());

        // 重启重放后版本不回退；再次新 requestId 继续严格递增。
        cluster.restartAll();
        Cluster reopened = Cluster.open(tempDir);
        WriteResult v4 = reopened.write("v4-req", key, "v4");
        assertThat(v4.version()).isEqualTo(4);
        assertThat(reopened.read(key).version()).isEqualTo(4);
        log.info("[版本] 重启后新 requestId 提交 version=4，严格递增无重复/跳号/回退");
    }
}
