package com.github.h3yow.shardedreplicatedstorejava.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 拓扑的持久化与解析。真正的数据迁移与世代切换由
 * {@link Cluster#reshard(List, Map)} 编排，本类只负责：
 * <ol>
 *   <li>加载/保存文本格式拓扑（shard 行 + replica 行）；</li>
 *   <li>保存前用 {@link TopologyValidator} 校验，重叠/空洞/越界直接拒绝；</li>
 *   <li>{@link #commitSwap(Path, Path)} 用“临时文件 + 原子 move”切换拓扑文件，
 *       保证任何时刻磁盘上的拓扑都是完整的一份。</li>
 * </ol>
 */
public final class TopologyManager {

    private static final Logger log = LoggerFactory.getLogger(TopologyManager.class);

    private final Path topologyFile;

    public TopologyManager(Path topologyFile) {
        this.topologyFile = topologyFile;
    }

    public Path topologyFile() {
        return topologyFile;
    }

    public Topology load() throws IOException {
        List<String> lines = Files.readAllLines(topologyFile, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            throw new TopologyException(TopologyViolation.EMPTY_TOPOLOGY,
                    "拓扑文件为空: " + topologyFile);
        }
        int epoch;
        try {
            epoch = Integer.parseInt(lines.get(0).trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("拓扑文件首行 epoch 不是数字: " + lines.get(0));
        }
        List<ShardRange> ranges = new ArrayList<>();
        Map<String, List<String>> replicas = new LinkedHashMap<>();
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.isBlank()) {
                continue;
            }
            String[] parts = line.split("\t", -1);
            if (parts.length < 4) {
                throw new IllegalStateException("拓扑文件第 " + (i + 1) + " 行格式损坏: " + line);
            }
            if ("SHARD".equals(parts[0])) {
                String shardId = b64decode(parts[1]);
                int start = Integer.parseInt(parts[2]);
                int end = Integer.parseInt(parts[3]);
                ranges.add(new ShardRange(shardId, start, end));
            } else if ("REPLICAS".equals(parts[0])) {
                String shardId = b64decode(parts[1]);
                List<String> ids = new ArrayList<>();
                for (int j = 2; j < parts.length; j++) {
                    ids.add(b64decode(parts[j]));
                }
                replicas.put(shardId, ids);
            } else {
                throw new IllegalStateException("拓扑文件出现未知记录类型: " + parts[0]);
            }
        }
        Topology topology = new Topology(epoch, ranges, replicas);
        TopologyValidator.validate(topology);
        log.info("载入拓扑 epoch={} 分片数={}（{}）", epoch, ranges.size(), topologyFile);
        return topology;
    }

    /** 校验通过后，以临时文件 + 原子 move 的方式保存为一份完整拓扑。 */
    public void save(Topology topology) throws IOException {
        TopologyValidator.validate(topology);
        Path tmp = topologyFile.resolveSibling(topologyFile.getFileName() + ".tmp");
        Files.createDirectories(topologyFile.getParent());
        try (BufferedWriter w = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8)) {
            w.write(Integer.toString(topology.epoch()));
            w.newLine();
            for (ShardRange range : topology.sortedRanges()) {
                w.write(String.join("\t", "SHARD",
                        b64(range.shardId()),
                        Integer.toString(range.startInclusive()),
                        Integer.toString(range.endExclusive())));
                w.newLine();
            }
            for (ShardRange range : topology.ranges()) {
                List<String> tokens = new ArrayList<>();
                tokens.add("REPLICAS");
                tokens.add(b64(range.shardId()));
                for (String replicaId : topology.replicasOf(range.shardId())) {
                    tokens.add(b64(replicaId));
                }
                w.write(String.join("\t", tokens));
                w.newLine();
            }
        }
        Files.move(tmp, topologyFile, StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
        log.info("拓扑已持久化 epoch={} 分片数={}", topology.epoch(), topology.ranges().size());
    }

    /** 将新拓扑临时文件原子替换当前拓扑文件。 */
    public void commitSwap(Path stagedFile) throws IOException {
        Files.move(stagedFile, topologyFile, StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
        log.info("拓扑世代原子切换完成 -> {}", topologyFile);
    }

    private static String b64(String raw) {
        return Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    private static String b64decode(String encoded) {
        return new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
    }
}
