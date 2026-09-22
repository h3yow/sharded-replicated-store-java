package com.github.h3yow.shardedreplicatedstorejava.recovery;

import com.github.h3yow.shardedreplicatedstorejava.cluster.TopologyStore;
import com.github.h3yow.shardedreplicatedstorejava.log.FileReplicaLog;
import com.github.h3yow.shardedreplicatedstorejava.model.RecoveryReport;
import com.github.h3yow.shardedreplicatedstorejava.model.ShardSpec;
import com.github.h3yow.shardedreplicatedstorejava.model.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 恢复管理器：扫描某个代际目录，按拓扑逐个打开（并校验）每个副本的分片日志。
 *
 * <p>{@link FileReplicaLog} 在打开时即完成校验：遇到截断 / 校验和不符 / 缺号 /
 * 乱序会停在最后一个一致位点并物理截断损坏尾部。本类汇总每个
 * (副本,分片) 的最后一致位点，并报告首个损坏原因（不静默跳过任何异常）。
 */
public class RecoveryManager {

    private static final Logger log = LoggerFactory.getLogger(RecoveryManager.class);

    private final Path rootDir;

    public RecoveryManager(Path rootDir) {
        this.rootDir = rootDir;
    }

    /** 打开副本分片日志的标准路径：gen-{g}/{shardId}__{replicaId}.log */
    public static Path logFile(Path rootDir, long generation, String shardId, String replicaId) {
        return rootDir.resolve("gen-" + generation)
                .resolve(shardId + "__" + replicaId + ".log");
    }

    public RecoveryReport recover(long generation) {
        TopologyStore store = new TopologyStore(rootDir);
        Topology topology = store.load(generation);
        Map<String, Long> positions = new LinkedHashMap<>();
        boolean corruption = false;
        String firstReason = null;

        log.info("==== 开始恢复代际 {}：ringSize={} 分片数={} ====",
                generation, topology.ringSize(), topology.shards().size());
        for (ShardSpec shard : topology.shardsSortedByRange()) {
            for (String replicaId : shard.replicas()) {
                Path f = logFile(rootDir, generation, shard.shardId(), replicaId);
                FileReplicaLog rlog = new FileReplicaLog(f);
                long last = rlog.verifyAndRecover();
                String reason = rlog.corruptionReason();
                String key = replicaId + "/" + shard.shardId();
                positions.put(key, last);
                if (reason != null) {
                    corruption = true;
                    if (firstReason == null) {
                        firstReason = reason + " @ " + key;
                    }
                    log.warn("[恢复] {} 最后一致位点={} 损坏原因={}", key, last, reason);
                } else {
                    log.info("[恢复] {} 最后一致位点={} 校验通过", key, last);
                }
            }
        }
        log.info("==== 恢复扫描完成 代际 {}：损坏={} 原因={} ====",
                generation, corruption, firstReason);
        return new RecoveryReport(generation, positions, corruption, firstReason);
    }
}
