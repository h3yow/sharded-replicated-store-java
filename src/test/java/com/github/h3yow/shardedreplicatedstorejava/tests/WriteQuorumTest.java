package com.github.h3yow.shardedreplicatedstorejava.tests;

import com.github.h3yow.shardedreplicatedstorejava.cluster.Cluster;
import com.github.h3yow.shardedreplicatedstorejava.exception.DuplicateConflictException;
import com.github.h3yow.shardedreplicatedstorejava.exception.InsufficientAcksException;
import com.github.h3yow.shardedreplicatedstorejava.exception.ReplicaUnavailableException;
import com.github.h3yow.shardedreplicatedstorejava.model.LogEntry;
import com.github.h3yow.shardedreplicatedstorejava.model.WriteResult;
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
 * 写入场景（3 副本，多数派 = 2）：
 * - 多数派确认后才可见；
 * - REPLICAS_UNAVAILABLE（发起前可用不足）与 INSUFFICIENT_ACKS（可用但确认失败）明确区分；
 * - 重复提交复用首次结论、不重复应用；同 requestId 不同内容必须拒绝；
 * - 未确认写入重试后可达成确认，不会重复应用或版本回退。
 */
class WriteQuorumTest {

    private static final Logger log = LoggerFactory.getLogger(WriteQuorumTest.class);

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

    @Test
    @DisplayName("多数派确认后可见，提交位点推进")
    void quorumCommitVisible() {
        WriteResult r = cluster.write("w1", "k", "v1", LogEntry.Operation.PUT);
        assertTrue(r.committed());
        assertEquals(3, r.acks(), "三个副本均正常时全部确认");
        assertEquals(2, r.quorum());
        assertEquals(Optional.of("v1"), cluster.read("k"));
        log.info("[位点] 正常写入 commitIndex={} acks={}/{} 判定={}",
                r.commitIndex(), r.acks(), r.quorum(), r.decision());
    }

    @Test
    @DisplayName("1 副本宕机仍可多数派写入；宕机恢复后追赶一致")
    void oneDownStillCommitsThenCatchUp() {
        cluster.setReplicaDown("r2", true);
        WriteResult r = cluster.write("w2", "k", "v2", LogEntry.Operation.PUT);
        assertTrue(r.committed());
        assertEquals(2, r.acks(), "领导+1 个跟随者仍构成多数派");

        var coord = cluster.coordinatorFor("k");
        assertEquals(0, coord.replicas().stream()
                .filter(x -> x.replicaId().equals("r2")).findFirst().orElseThrow().lastIndex(),
                () -> "宕机副本不应有数据");
        log.info("[位点] r2 宕机期间写入：commitIndex={} r2 本地日志位点=0", r.commitIndex());

        cluster.setReplicaDown("r2", false);
        int caught = coord.catchUpReplica("r2");
        assertEquals(1, caught);
        assertEquals(Optional.of("v2"),
                coord.replicas().stream().filter(x -> x.replicaId().equals("r2"))
                        .findFirst().orElseThrow().read("k"));
        log.info("[追赶] r2 恢复后追加 {} 条，与领导一致于位点 {}", caught, coord.commitIndex());
    }

    @Test
    @DisplayName("2 副本宕机：发起前可用不足 -> REPLICAS_UNAVAILABLE")
    void twoDownUnavailable() {
        cluster.setReplicaDown("r1", true);
        cluster.setReplicaDown("r2", true);
        ReplicaUnavailableException ex = assertThrows(ReplicaUnavailableException.class,
                () -> cluster.write("w3", "k", "v3", LogEntry.Operation.PUT));
        assertEquals("REPLICAS_UNAVAILABLE", ex.getReason());
        assertEquals(Optional.empty(), cluster.read("k"), "未确认数据不可见");
        log.info("[判定依据] 副本不可用 reason={} msg={}", ex.getReason(), ex.getMessage());
    }

    @Test
    @DisplayName("可用但确认失败：acks=1 < quorum=2 -> INSUFFICIENT_ACKS，与不可用区分")
    void acceptFailuresInsufficientAcks() {
        // r0 领导 + r1 可用成功；r2 接收故障 -> acks=2 -> 实际成功
        // 改为让领导之外两个跟随者之一失败，再制造第二次失败：先注入 r1 与 r2 均故障
        cluster.setAcceptFailing("r1", true);
        cluster.setAcceptFailing("r2", true);
        InsufficientAcksException ex = assertThrows(InsufficientAcksException.class,
                () -> cluster.write("w4", "k", "v4", LogEntry.Operation.PUT));
        assertEquals("INSUFFICIENT_ACKS", ex.getReason());
        assertEquals(Optional.empty(), cluster.read("k"), "acks 不足时数据不可见");
        log.info("[判定依据] 确认不足 reason={} msg={}", ex.getReason(), ex.getMessage());

        // 恢复一个跟随者后重试同 requestId：幂等续提成功，不产生第二条
        cluster.setAcceptFailing("r1", false);
        WriteResult r = cluster.write("w4", "k", "v4", LogEntry.Operation.PUT);
        assertTrue(r.committed());
        assertEquals(Optional.of("v4"), cluster.read("k"));
        assertEquals(1L, cluster.coordinatorFor("k").commitIndex(), "重试不得产生重复位点");
        log.info("[位点] 幂等续提成功 commitIndex={} 判定={}", r.commitIndex(), r.decision());
    }

    @Test
    @DisplayName("重复提交：同 requestId 同内容返回 duplicate=true 且位点不变")
    void duplicateSubmissionReusesDecision() {
        WriteResult first = cluster.write("dup-1", "k", "A", LogEntry.Operation.PUT);
        WriteResult second = cluster.write("dup-1", "k", "A", LogEntry.Operation.PUT);
        WriteResult third = cluster.write("dup-1", "k", "A", LogEntry.Operation.PUT);
        assertFalse(first.duplicate());
        assertTrue(second.duplicate());
        assertTrue(third.duplicate());
        assertEquals(first.commitIndex(), second.commitIndex());
        assertEquals(1L, cluster.coordinatorFor("k").commitIndex());
        assertEquals(Optional.of("A"), cluster.read("k"));
        log.info("[版本] 重复提交位点={} 三次结论一致，版本不回退；判定2={} 判定3={}",
                second.commitIndex(), second.decision(), third.decision());
    }

    @Test
    @DisplayName("同 requestId 不同内容：DUPLICATE_REQUEST_CONFLICT 必须拒绝")
    void duplicateWithDifferentPayloadRejected() {
        cluster.write("dup-2", "k", "A", LogEntry.Operation.PUT);
        DuplicateConflictException ex = assertThrows(DuplicateConflictException.class,
                () -> cluster.write("dup-2", "k", "B", LogEntry.Operation.PUT));
        assertEquals("DUPLICATE_REQUEST_CONFLICT", ex.getReason());
        assertEquals(Optional.of("A"), cluster.read("k"), "冲突重试不得改变已确认版本");
        log.info("[判定依据] 重复冲突 reason={} msg={}", ex.getReason(), ex.getMessage());
    }
}
