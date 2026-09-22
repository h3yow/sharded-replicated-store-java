package com.github.h3yow.shardedreplicatedstorejava.tests;

import com.github.h3yow.shardedreplicatedstorejava.cluster.Cluster;
import com.github.h3yow.shardedreplicatedstorejava.cluster.ShardCoordinator;
import com.github.h3yow.shardedreplicatedstorejava.model.LogEntry;
import com.github.h3yow.shardedreplicatedstorejava.replica.Replica;
import com.github.h3yow.shardedreplicatedstorejava.tests.support.TestKit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 落后副本追赶场景：
 * - 副本宕机期间写入照常多数派确认；恢复后 catchUp 逐条补齐并重放；
 * - 追赶幂等：重复追赶不重复追加、不重复应用；
 * - 追赶后重启，结论仍一致。
 */
class CatchUpTest {

    private static final Logger log = LoggerFactory.getLogger(CatchUpTest.class);

    private Path root;
    private Cluster cluster;

    @BeforeEach
    void setUp() {
        root = TestKit.newTempDir();
        cluster = new Cluster(root);
        cluster.bootstrap(TestKit.topology(1, 2));
    }

    @AfterEach
    void tearDown() {
        cluster.close();
    }

    private Replica replica(ShardCoordinator c, String id) {
        return c.replicas().stream().filter(r -> r.replicaId().equals(id))
                .findFirst().orElseThrow();
    }

    @Test
    @DisplayName("宕机副本恢复后逐条追赶至提交位点，状态机一致")
    void catchUpAfterOutage() {
        ShardCoordinator coord = cluster.coordinatorFor("fixed-key");
        cluster.setReplicaDown("r2", true);
        for (int i = 1; i <= 6; i++) {
            cluster.write("cu-" + i, "fixed-key", "v" + i, LogEntry.Operation.PUT);
        }
        assertEquals(6, coord.commitIndex());
        Replica r2 = replica(coord, "r2");
        assertEquals(0, r2.lastIndex());
        assertEquals(Optional.empty(), r2.read("fixed-key"), "未追赶前宕机副本无数据");

        cluster.setReplicaDown("r2", false);
        int caught = coord.catchUpReplica("r2");
        assertEquals(6, caught);
        assertEquals(6, r2.lastIndex());
        assertEquals(6, r2.stateMachine().appliedIndex());
        assertEquals(Optional.of("v6"), r2.read("fixed-key"));
        log.info("[位点] r2 追赶 0 -> 6，appliedIndex=6，最终版本依据 requestId 最大=cu-6");

        // 再次追赶必须是幂等空操作
        int again = coord.catchUpReplica("r2");
        assertEquals(0, again, "重复追赶不得重复追加或应用");
        assertEquals(Optional.of("v6"), r2.read("fixed-key"));
        log.info("[版本] 重复追赶追加 {} 条，版本不回退、不重复", again);
    }

    @Test
    @DisplayName("部分落后：r2 只持有前 2 条，追赶补齐后 4 条，位点连续")
    void partialCatchUp() {
        cluster.setReplicaDown("r2", true);
        cluster.write("p-1", "pk", "1", LogEntry.Operation.PUT);
        cluster.write("p-2", "pk", "2", LogEntry.Operation.PUT);
        ShardCoordinator coord = cluster.coordinatorFor("pk");
        cluster.setReplicaDown("r2", false);
        coord.catchUpReplica("r2");

        cluster.setReplicaDown("r2", true);
        for (int i = 3; i <= 6; i++) {
            cluster.write("p-" + i, "pk", Integer.toString(i), LogEntry.Operation.PUT);
        }
        Replica r2 = replica(coord, "r2");
        assertEquals(2, r2.lastIndex(), "二次宕机期间 r2 停留在位点 2");
        cluster.setReplicaDown("r2", false);
        int caught = coord.catchUpReplica("r2");
        assertEquals(4, caught);
        assertEquals(6, r2.lastIndex());
        for (long i = 1; i <= 6; i++) {
            long idx = i;
            assertTrue(r2.log().entryAt(idx).isPresent(), "位点 " + idx + " 必须连续存在");
        }
        assertEquals(Optional.of("6"), r2.read("pk"));
        log.info("[位点] 部分落后追赶：2 -> 6，追加 {} 条且位点连续无空洞", caught);
    }

    @Test
    @DisplayName("追赶后重启：日志重放，三个副本结论完全一致")
    void catchUpThenRestart() {
        cluster.setReplicaDown("r2", true);
        for (int i = 1; i <= 4; i++) {
            cluster.write("rt-" + i, "rk", "w" + i, LogEntry.Operation.PUT);
        }
        cluster.setReplicaDown("r2", false);
        ShardCoordinator coord = cluster.coordinatorFor("rk");
        coord.catchUpReplica("r2");

        var report = cluster.restart();
        assertFalse(report.corruptionDetected());
        assertEquals(4, coord.commitIndex());
        for (String id : new String[]{"r0", "r1", "r2"}) {
            Replica r = replica(coord, id);
            assertEquals(4, r.lastIndex());
            assertEquals(Optional.of("w4"), r.read("rk"));
        }
        log.info("[位点] 追赶后重启重放：r0/r1/r2 均到位点 4，版本=rt-4，结论一致");
    }
}
