package com.github.h3yow.shardedreplicatedstorejava.cluster;

import com.github.h3yow.shardedreplicatedstorejava.exception.StoreException;
import com.github.h3yow.shardedreplicatedstorejava.model.KeyRange;
import com.github.h3yow.shardedreplicatedstorejava.model.ShardSpec;
import com.github.h3yow.shardedreplicatedstorejava.model.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 代际拓扑的持久化：
 * <pre>
 * root/
 *   gen-{generation}/topology.properties
 *   CURRENT            （单行：当前代际号，原子替换）
 * </pre>
 * 切换代际时先把新拓扑落盘，再原子替换 CURRENT，保证重启时不会读到“半套”配置。
 */
public class TopologyStore {

    private static final Logger log = LoggerFactory.getLogger(TopologyStore.class);
    public static final String CURRENT_FILE = "CURRENT";

    private final Path rootDir;

    public TopologyStore(Path rootDir) {
        this.rootDir = rootDir;
        try {
            Files.createDirectories(rootDir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create root " + rootDir, e);
        }
    }

    public Path genDir(long generation) {
        return rootDir.resolve("gen-" + generation);
    }

    public Path topologyFile(long generation) {
        return genDir(generation).resolve("topology.properties");
    }

    /** 持久化拓扑并原子切换 CURRENT 指针。 */
    public synchronized void commit(Topology topology) {
        Path dir = genDir(topology.generation());
        try {
            Files.createDirectories(dir);
            Path target = topologyFile(topology.generation());
            Path tmp = dir.resolve("topology.properties.tmp");
            Files.writeString(tmp, render(topology), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

            Path current = rootDir.resolve(CURRENT_FILE);
            Path currentTmp = rootDir.resolve(CURRENT_FILE + ".tmp");
            Files.writeString(currentTmp, Long.toString(topology.generation()),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(currentTmp, current, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            log.info("拓扑代际 {} 已持久化并切换 CURRENT（{} 个分片）",
                    topology.generation(), topology.shards().size());
        } catch (IOException e) {
            throw new UncheckedIOException("commit topology failed", e);
        }
    }

    public boolean hasCurrent() {
        return Files.exists(rootDir.resolve(CURRENT_FILE));
    }

    public long currentGeneration() {
        try {
            String s = Files.readString(rootDir.resolve(CURRENT_FILE), StandardCharsets.UTF_8).trim();
            return Long.parseLong(s);
        } catch (IOException e) {
            throw new StoreException("TOPOLOGY_STATE_MISSING", "cannot read CURRENT pointer");
        }
    }

    public Topology loadCurrent() {
        return load(currentGeneration());
    }

    public Topology load(long generation) {
        try {
            Path f = topologyFile(generation);
            if (!Files.exists(f)) {
                throw new StoreException("TOPOLOGY_STATE_MISSING",
                        "topology file missing for generation " + generation);
            }
            return parse(generation, Files.readString(f, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException("load topology failed", e);
        }
    }

    private String render(Topology t) {
        StringBuilder sb = new StringBuilder();
        sb.append("generation=").append(t.generation()).append('\n');
        sb.append("ringSize=").append(t.ringSize()).append('\n');
        for (ShardSpec s : t.shards()) {
            sb.append("shard=").append(s.shardId())
                    .append(':').append(s.range().start())
                    .append(':').append(s.range().end())
                    .append(':').append(String.join(",", s.replicas()))
                    .append('\n');
        }
        return sb.toString();
    }

    private Topology parse(long generation, String content) {
        int ringSize = -1;
        List<ShardSpec> shards = new ArrayList<>();
        for (String raw : content.split("\n")) {
            String line = raw.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            if (line.startsWith("generation=")) {
                continue;
            }
            if (line.startsWith("ringSize=")) {
                ringSize = Integer.parseInt(line.substring("ringSize=".length()));
                continue;
            }
            if (line.startsWith("shard=")) {
                String[] parts = line.substring("shard=".length()).split(":", -1);
                if (parts.length != 4) {
                    throw new StoreException("TOPOLOGY_STATE_CORRUPT", "bad shard line: " + line);
                }
                List<String> replicas = Arrays.stream(parts[3].split(",", -1))
                        .map(String::trim).filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());
                shards.add(new ShardSpec(parts[0],
                        new KeyRange(Integer.parseInt(parts[1]), Integer.parseInt(parts[2])),
                        replicas));
            }
        }
        if (ringSize <= 0) {
            throw new StoreException("TOPOLOGY_STATE_CORRUPT", "ringSize missing or invalid");
        }
        return new Topology(generation, ringSize, shards);
    }
}
