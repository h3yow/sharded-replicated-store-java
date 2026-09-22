package com.github.h3yow.shardedreplicatedstorejava.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * 按顺序键串行化临界区，支持两种确定性级别。
 *
 * <ul>
 *   <li>{@link #runOrdered}：普通互斥。用于生产的单次写入路径，保证临界区
 *       绝不重叠（从而日志 index 分配、版本号递增、提交位点推进都是原子的）。</li>
 *   <li>{@link #runOrderedInBatch}：批量严格全序。调用方声明本批竞争者数量
 *       {@code batchSize}：到齐之前所有线程只登记、不执行；到齐后整批严格按
 *       orderKey 升序逐个放行，与线程到达先后、调度无关，结果逐次可复现。
 *       用于“同一键并发写入最终版本确定可复现”的验证。</li>
 * </ul>
 *
 * <p>状态机（单把 {@link ReentrantLock} + Condition，无丢失唤醒）：</p>
 * <ol>
 *   <li>登记阶段：把本线程加入当前批次 members，循环等待 members 到齐；</li>
 *   <li>到齐后：按 orderKey 排序确定每个成员的 position；</li>
 *   <li>放行阶段：成员循环等待 {@code turn == 自己的 position}，持有者退出时
 *       {@code turn++} 并 signalAll，下一位进入。</li>
 * </ol>
 */
public final class DeterministicLock {

    private static final Logger log = LoggerFactory.getLogger(DeterministicLock.class);

    private static final class Member {
        final String orderKey;
        int position = -1;

        Member(String orderKey) {
            this.orderKey = orderKey;
        }
    }

    /** 一个批次的收集与放行状态。 */
    private static final class Batch {
        final int expected;
        final List<Member> members = new ArrayList<>();
        boolean sorted;
        int turn; // 当前应执行的 position

        Batch(int expected) {
            this.expected = expected;
        }
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final java.util.concurrent.locks.Condition changed = lock.newCondition();
    private Batch currentBatch;

    /** 普通互斥路径：每次调用构成一个 batchSize=1 的批次。 */
    public <T> T runOrdered(String orderKey, Supplier<T> action) {
        return runOrderedInBatch(orderKey, 1, action);
    }

    public <T> T runOrderedInBatch(String orderKey, int batchSize, Supplier<T> action) {
        Member me = new Member(orderKey);
        lock.lock();
        try {
            // 1) 登记到当前批次；若没有进行中的批次则开一个。
            if (currentBatch == null || currentBatch.sorted) {
                currentBatch = new Batch(batchSize);
            }
            Batch batch = currentBatch;
            batch.members.add(me);
            changed.signalAll();

            // 2) 等待本批到齐。
            while (batch.members.size() < batch.expected) {
                changed.await();
            }

            // 3) 到齐：只排序一次，确定每个成员的 position。
            if (!batch.sorted) {
                batch.members.sort((a, b) -> a.orderKey.compareTo(b.orderKey));
                for (int i = 0; i < batch.members.size(); i++) {
                    batch.members.get(i).position = i;
                }
                batch.sorted = true;
                changed.signalAll();
            }

            // 4) 严格按 position 逐个放行。
            while (batch.turn != me.position) {
                changed.await();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }

        // 此刻我是本批的当前持有者，临界区独占。
        boolean success = false;
        try {
            T result = action.get();
            success = true;
            return result;
        } finally {
            lock.lock();
            try {
                Batch batch = currentBatch;
                batch.turn++;
                // 本批最后一个退出：关闭批次，允许下一批开始。
                if (batch.turn >= batch.expected) {
                    currentBatch = null;
                }
                log.debug("确定性锁完成 orderKey={} position 结束 success={} turn={}",
                        orderKey, success, batch.turn);
                changed.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }
}
