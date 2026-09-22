package com.github.h3yow.shardedreplicatedstorejava.tests.support;

import com.github.h3yow.shardedreplicatedstorejava.model.KeyRange;
import com.github.h3yow.shardedreplicatedstorejava.model.ShardSpec;
import com.github.h3yow.shardedreplicatedstorejava.model.Topology;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import com.github.h3yow.shardedreplicatedstorejava.util.Checksums;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** 测试公共工具：临时目录、标准拓扑构造、日志损坏手段。 */
public final class TestKit {

    public static final int RING = 1000;

    private TestKit() {
    }

    public static Path newTempDir() {
        try {
            return Files.createTempDirectory("srstore-test-");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** 构造 n 个均匀分片、每分片 3 副本（r0/r1/r2）的拓扑。 */
    public static Topology topology(long generation, int n, int ring) {
        List<ShardSpec> shards = new ArrayList<>();
        int step = ring / n;
        for (int i = 0; i < n; i++) {
            int start = i * step;
            int end = (i == n - 1) ? ring : (i + 1) * step;
            shards.add(new ShardSpec("s" + i, new KeyRange(start, end),
                    List.of("r0", "r1", "r2")));
        }
        return new Topology(generation, ring, shards);
    }

    public static Topology topology(long generation, int n) {
        return topology(generation, n, RING);
    }

    public static Path logFile(Path root, long gen, String shardId, String replicaId) {
        return root.resolve("gen-" + gen).resolve(shardId + "__" + replicaId + ".log");
    }

    public static List<String> readLines(Path f) {
        try {
            return Files.readAllLines(f, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeLines(Path f, List<String> lines) {
        try {
            Files.createDirectories(f.getParent());
            Files.write(f, lines, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** 物理截断：只保留前 keep 行，模拟进程崩溃导致的半截日志。 */
    public static void truncateLines(Path f, int keep) {
        List<String> lines = readLines(f);
        writeLines(f, lines.subList(0, Math.min(keep, lines.size())));
    }

    /** 破坏指定行（0 基）的 checksum 字段（最后一列），模拟位翻转。 */
    public static void corruptChecksum(Path f, int lineIndex) {
        List<String> lines = readLines(f);
        String[] parts = lines.get(lineIndex).split("\t", -1);
        parts[parts.length - 1] = "0";
        lines.set(lineIndex, String.join("\t", parts));
        writeLines(f, lines);
    }

    /**
     * 重写指定行的 index 字段（第 2 列）<b>并重算校验和</b>，
     * 用于构造“内容自洽但缺号/乱序”的日志（不会先被校验和拦截）。
     */
    public static void rewriteIndex(Path f, int lineIndex, long newIndex) {
        List<String> lines = readLines(f);
        String[] parts = lines.get(lineIndex).split("\t", -1);
        long term = Long.parseLong(parts[2]);
        String op = parts[3];
        String requestId = parts[4];
        String key = parts[5];
        String value = "-".equals(parts[6]) ? null
                : new String(java.util.Base64.getDecoder().decode(parts[6]), StandardCharsets.UTF_8);
        parts[1] = Long.toString(newIndex);
        parts[7] = Long.toString(Checksums.of(newIndex, term, requestId, key, value, op));
        lines.set(lineIndex, String.join("\t", parts));
        writeLines(f, lines);
    }
}
