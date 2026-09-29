package com.github.h3yow.shardedreplicatedstorejava.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 普通写入路径上的“同键并发决胜闸门”：<strong>调用方无需声明批大小</strong>。
 *
 * <p>规则（只依赖请求内容，不依赖线程调度/到达顺序）：</p>
 * <ol>
 *   <li>两段式自动成批。每个成员先经历很短的<strong>探测窗口</strong>
 *       {@code probeNanos}：窗口内始终没有同伴则判定“无人竞争”，立即单走，串行写几乎
 *       不付额外延迟；一旦探测窗口内出现第二个请求，本轮升级为<strong>竞争轮</strong>，
 *       进入更长的收集窗口 {@code collectNanos}，每来一个新成员就把窗口顺延，直到连续
 *       {@code collectNanos} 没有新成员才关轮。收集窗口取到足够覆盖线程池调度抖动
 *       （默认几十毫秒），保证被同一起跑线同时放行的一批并发请求（它们在做磁盘 I/O
 *       之前就会全部完成闸门登记）必落在同一轮。</li>
 *   <li>关轮后按 <strong>requestId 升序</strong>排序，逐位放行临界区。于是同一批同键
 *       提交的 index、version、最终值只由这批请求的内容决定：与谁先到、哪个线程先被
 *       调度完全无关，反复运行逐次一致。</li>
 *   <li>每个成员拿到 {@link Ticket}：position（0 起）、size、winnerRequestId
 *       （requestId 最大者）。调用方据此把结论标记为 COMMITTED（自己即赢家）或
 *       SUPERSEDED（被更大 requestId 覆盖，但已多数派落盘）。</li>
 *   <li>同一个 requestId 在同一轮内重复出现：后到者不重复参与决胜，只等待先到者的
 *       结论（重复提交由分片幂等表最终定性），避免对同一逻辑请求分配两个版本。</li>
 * </ol>
 *
 * <p>为什么可以不声明批大小：真正并发的请求在同一起跑线后、做任何磁盘 I/O 之前就会
 * 完成闸门登记，登记间隔是微秒级纯内存操作；收集窗口（毫秒级、且随到达不断顺延）远
 * 大于该抖动，因而能把“这一批并发”稳定收全。串行（上一轮结束后才发起）的请求与上
 * 一批之间隔着整轮处理时间，探测窗口内看不到同伴，绝不被错误合并，happens-before
 * 语义保持不变。</p>
 *
 * <p>本闸门只在“同 key”维度排队，不持有分片锁；真正的日志/状态机互斥仍由
 * {@link DeterministicLock} 在动作内部保证，因此不存在跨 key、跨分片的死锁环。</p>
 */
public final class KeyContestGate {

    private static final Logger log = LoggerFactory.getLogger(KeyContestGate.class);

    /** 决胜门票：成员在本轮的位置、批大小、最终赢家。 */
    public record Ticket(int position, int size, String winnerRequestId, boolean duplicate,
                         String key, String requestId) {
        public boolean winner() {
            return !duplicate && position == size - 1; // 已按 requestId 升序，最后一位即赢家
        }
    }

    private static final class Member {
        final String requestId;
        int position = -1;

        Member(String requestId) {
            this.requestId = requestId;
        }
    }

    private static final class Round {
        final ReentrantLock lock = new ReentrantLock();
        final java.util.concurrent.locks.Condition changed = lock.newCondition();
        final List<Member> members = new ArrayList<>();
        boolean contested;   // 探测窗口内是否已发现 >=2 名成员
        boolean closed;
        int turn;            // 当前应进入临界区的 position（仅计非重复成员）
        List<Member> ordered;
        String winnerRequestId;
    }

    /** 探测窗口：无人竞争时尽快单走。 */
    public static final long DEFAULT_PROBE_NANOS =
            Long.getLong("store.contest.probeNanos", 2_000_000L); // 2ms

    /** 竞争收集窗口：发现竞争后等待同伴的静默阈值，覆盖调度抖动，可随到达顺延。 */
    public static final long DEFAULT_COLLECT_NANOS =
            Long.getLong("store.contest.collectNanos", 40_000_000L); // 40ms

    /** 防御性上限：等待关轮/排位的最长时间，杜绝任何悬挂。 */
    public static final long DEFAULT_MAX_WAIT_NANOS =
            Long.getLong("store.contest.maxWaitNanos", 30_000_000_000L); // 30s

    private final long probeNanos;
    private final long collectNanos;
    private final long maxWaitNanos;
    private final ConcurrentHashMap<String, Round> rounds = new ConcurrentHashMap<>();

    public KeyContestGate() {
        this(DEFAULT_PROBE_NANOS, DEFAULT_COLLECT_NANOS, DEFAULT_MAX_WAIT_NANOS);
    }

    public KeyContestGate(long probeNanos, long collectNanos, long maxWaitNanos) {
        this.probeNanos = probeNanos;
        this.collectNanos = collectNanos;
        this.maxWaitNanos = maxWaitNanos;
    }

    /** 登记并等待本决胜轮关闭、排序完成，返回门票。 */
    public Ticket enter(String key, String requestId) {
        Round round = rounds.computeIfAbsent(key, k -> new Round());
        round.lock.lock();
        try {
            Member existing = find(round, requestId);
            boolean duplicate = existing != null;
            Member me = duplicate ? existing : new Member(requestId);
            if (!duplicate) {
                round.members.add(me);
            }
            round.changed.signalAll();

            long deadline = System.nanoTime() + maxWaitNanos;
            if (!duplicate && round.members.size() >= 2) {
                round.contested = true;
                round.changed.signalAll();
            }
            waitForClose(round, deadline);

            if (round.ordered == null) {
                round.ordered = new ArrayList<>(round.members);
                round.ordered.sort(Comparator.comparing(m -> m.requestId));
                for (int i = 0; i < round.ordered.size(); i++) {
                    round.ordered.get(i).position = i;
                }
                round.winnerRequestId = round.ordered.get(round.ordered.size() - 1).requestId;
                round.changed.signalAll();
            }
            return new Ticket(me.position, round.ordered.size(),
                    round.winnerRequestId, duplicate, key, requestId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("同键决胜等待被中断: key=" + key, e);
        } finally {
            round.lock.unlock();
        }
    }

    /**
     * 关轮等待（锁内，可被打断/超时）。每个成员各自等一个静默窗口：
     * 无人竞争（始终 1 人）用短探测窗口；一旦竞争出现，所有成员改用长收集窗口，
     * 窗口内每来一个新成员都顺延。这样既不让串行写付长等待，又能稳定收齐并发批。
     */
    private void waitForClose(Round round, long deadline) throws InterruptedException {
        while (!round.closed) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                round.closed = true; // 防御：超时强制关轮，绝不悬挂
                round.changed.signalAll();
                return;
            }
            int sizeBefore = round.members.size();
            boolean contestedBefore = round.contested;
            long wait = contestedBefore
                    ? Math.min(collectNanos, remaining)
                    : Math.min(probeNanos, remaining);
            round.changed.await(wait, java.util.concurrent.TimeUnit.NANOSECONDS);

            if (round.members.size() >= 2) {
                round.contested = true;
            }
            boolean noNewcomer = round.members.size() == sizeBefore;
            if (round.closed) {
                return;
            }
            // 关轮条件：竞争轮在收集窗口内没有新成员；或非竞争轮探测窗口内仍独自一人。
            if (round.contested) {
                if (contestedBefore && noNewcomer) {
                    round.closed = true;
                    round.changed.signalAll();
                    return;
                }
                // 刚升级为竞争轮 / 收集窗口内又来人：用收集窗口再等一轮。
            } else if (noNewcomer && round.members.size() == 1) {
                round.closed = true;
                round.changed.signalAll();
                return;
            }
        }
    }

    private static Member find(Round round, String requestId) {
        for (Member m : round.members) {
            if (m.requestId.equals(requestId)) {
                return m;
            }
        }
        return null;
    }

    /** 非重复成员：等待自己的 position，获得临界区入场许可。 */
    public void awaitTurn(Ticket ticket) {
        Round round = rounds.get(ticket.key());
        if (round == null) {
            throw new IllegalStateException("决胜轮已不存在: key=" + ticket.key());
        }
        round.lock.lock();
        try {
            long deadline = System.nanoTime() + maxWaitNanos;
            while (round.turn != ticket.position()) {
                round.changed.await(deadline - System.nanoTime(),
                        java.util.concurrent.TimeUnit.NANOSECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("等待决胜排位被中断: " + ticket.requestId(), e);
        } finally {
            round.lock.unlock();
        }
    }

    /** 重复成员：等待先到的同 requestId 成员完成动作（turn 越过其 position）。 */
    public void awaitDuplicateSettled(Ticket ticket) {
        Round round = rounds.get(ticket.key());
        if (round == null) {
            return; // 轮已结束，先到者必然已结案
        }
        round.lock.lock();
        try {
            long deadline = System.nanoTime() + maxWaitNanos;
            while (rounds.containsKey(ticket.key()) && round.turn <= ticket.position()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                round.changed.await(Math.min(remaining, collectNanos),
                        java.util.concurrent.TimeUnit.NANOSECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("等待重复请求结案被中断: " + ticket.requestId(), e);
        } finally {
            round.lock.unlock();
        }
    }

    /** 非重复成员完成临界区动作后通知下一位；最后一人负责销毁轮。 */
    public void finish(Ticket ticket) {
        Round round = rounds.get(ticket.key());
        if (round == null) {
            return;
        }
        round.lock.lock();
        try {
            round.turn++;
            log.debug("决胜放行完成 key={} requestId={} position={}/{} winner={} turn={}",
                    ticket.key(), ticket.requestId(), ticket.position(), ticket.size(),
                    ticket.winnerRequestId(), round.turn);
            round.changed.signalAll();
            if (round.closed && round.turn >= round.ordered.size()) {
                rounds.remove(ticket.key(), round);
                log.debug("决胜轮结束并移除 key={} size={} winner={}",
                        ticket.key(), ticket.size(), ticket.winnerRequestId());
            }
        } finally {
            round.lock.unlock();
        }
    }
}
