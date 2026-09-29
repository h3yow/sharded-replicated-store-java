package com.github.h3yow.shardedreplicatedstorejava.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * 普通写入路径上的“同键并发决胜门”：<b>不需要调用方声明批大小</b>。
 *
 * <p>每个 key 同时最多有一个活跃 episode（决胜轮次）。挤入同一轮的同键写请求
 * 在 episode 内登记并互相等待，封轮后：</p>
 * <ul>
 *   <li>本批 requestId 字典序最大者获胜，结论与到达先后、线程调度无关；</li>
 *   <li>成员按 requestId 升序逐个进入临界区执行（复用分片互斥语义，
 *       保证多键之间也不会并发改同一分片日志）；</li>
 *   <li>落败者由调用方判定为 {@link WriteStatus#SUPERSEDED}，
 *       不复制、不占版本；</li>
 *   <li>最后一个成员离开时回收 episode，之后到来的写请求开启新的一轮。</li>
 * </ul>
 *
 * <p><b>轮次识别（两段式自适应安静窗口）</b>：在没有任何外部信息（批大小/屏障）
 * 时，“同一批”只能按时间上的并发关系识别：</p>
 * <ol>
 *   <li><b>探测窗口</b>：轮次里第一个请求先等一个很短的探测窗口；窗口内没有第二
 *       个请求到来就判定为无竞争的单独写，立即放行（顺序写只付极小延迟）；</li>
 *   <li><b>批次窗口</b>：一旦探测到竞争者，轮次升级为批次模式，此后每来一个新
 *       竞争者都把安静窗口重新计时，必须连续 {@code cohortGraceMillis} 没有新
 *       竞争者才封轮。该窗口按“覆盖十几路线程同时唤醒时的 OS 调度抖动”取值，
 *       可吸收几十到上百毫秒的到达抖动，避免一批被拆成多批。</li>
 * </ol>
 * <p>可用系统属性 {@code contention.probe.millis}（默认 2ms）与
 * {@code contention.cohort.grace.millis}（默认 200ms）调整。需要严格形式化、
 * 零时间依赖的并发全序时，请使用 {@code writeInBatch} 显式批次接口。</p>
 */
public final class ContentionGate {

    private static final Logger log = LoggerFactory.getLogger(ContentionGate.class);

    /**
     * 决胜上下文。
     *
     * @param cohortSize      本批竞争者数量
     * @param winnerRequestId 本批 requestId 字典序最大者（单成员批次即其自身）
     */
    public record CohortContext(int cohortSize, String winnerRequestId) {
    }

    private static final class Episode {
        // requestId 去重并保留登记顺序
        final Map<String, Thread> members = new LinkedHashMap<>();
        boolean sealed;
        boolean retired;
        List<String> orderedRequestIds = List.of();
        String winnerRequestId;
        int turn;
    }

    private final long probeMillis;
    private final long cohortGraceMillis;
    private final Map<String, Episode> episodes = new ConcurrentHashMap<>();

    public ContentionGate() {
        this(Long.getLong("contention.probe.millis", 2L),
                Long.getLong("contention.cohort.grace.millis", 200L));
    }

    ContentionGate(long probeMillis, long cohortGraceMillis) {
        this.probeMillis = probeMillis;
        this.cohortGraceMillis = cohortGraceMillis;
    }

    /**
     * 以“同键并发决胜门”执行一次写入。
     *
     * @param action 临界区动作；入参为决胜上下文。动作内部仍需自行查幂等表，
     *               并据 {@link CohortContext#winnerRequestId()} 判定
     *               COMMITTED / SUPERSEDED
     */
    public <T> T runContended(String key, String requestId,
                              Function<CohortContext, T> action) {
        while (true) {
            Episode episode = episodes.computeIfAbsent(key, k -> new Episode());
            int myPosition;
            int cohortSize;
            String winner;
            synchronized (episode) {
                if (episode.retired) {
                    // 极端竞态：拿到的是刚被上一轮回收的 episode，换新一轮重试。
                    continue;
                }
                if (episode.sealed) {
                    // 本轮已封轮：封轮后绝不接纳新成员（否则它不在本轮顺序里，
                    // 会拿到非法 position 永久等待）。等上一轮回收后开启新一轮。
                    while (!episode.retired) {
                        await(episode);
                    }
                    continue;
                }
                episode.members.putIfAbsent(requestId, Thread.currentThread());
                episode.notifyAll();

                while (!episode.sealed) {
                    trySeal(key, episode);
                    if (!episode.sealed) {
                        await(episode);
                    }
                }
                myPosition = episode.orderedRequestIds.indexOf(requestId);
                cohortSize = episode.orderedRequestIds.size();
                winner = episode.winnerRequestId;
            }

            // 按 requestId 升序逐个进入临界区（index 分配、版本递增仍严格串行）。
            synchronized (episode) {
                while (episode.turn != myPosition) {
                    await(episode);
                }
            }
            try {
                return action.apply(new CohortContext(cohortSize, winner));
            } finally {
                synchronized (episode) {
                    episode.turn++;
                    episode.notifyAll();
                    // 最后一个离开临界区的成员回收本轮，后续写开启新一轮。
                    if (episode.turn >= episode.orderedRequestIds.size()) {
                        episode.retired = true;
                        episodes.remove(key, episode);
                        episode.notifyAll();
                        log.debug("同键决胜轮次回收 key={} 批次大小={}", key, cohortSize);
                    }
                }
            }
        }
    }

    private static void await(Episode episode) {
        try {
            episode.wait();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * 两段式封轮判定（调用方持 episode 锁）。每个被唤醒的成员都可尝试：
     * <ol>
     *   <li>当前仅 1 个成员：等满 {@code probeMillis} 探测窗口，期间无后来者即
     *       封轮为单独写；出现后来者则进入批次等待；</li>
     *   <li>已有多个成员：必须连续 {@code cohortGraceMillis} 无新成员登记才封轮；
     *       等待期间有新成员（被 notifyAll 提前唤醒）则重新计时。</li>
     * </ol>
     */
    private void trySeal(String key, Episode episode) {
        if (episode.sealed) {
            return;
        }
        try {
            if (episode.members.size() == 1) {
                episode.wait(probeMillis);
                if (episode.sealed) {
                    return;
                }
                if (episode.members.size() == 1) {
                    finishSeal(key, episode);
                    return;
                }
                // 探测到竞争者：落入下面的批次窗口等待。
            }
            int sizeAtStart = episode.members.size();
            episode.wait(cohortGraceMillis);
            if (episode.sealed) {
                return;
            }
            if (episode.members.size() == sizeAtStart) {
                finishSeal(key, episode);
            }
            // 否则窗口被新登记打断：返回外层，下一次被唤醒时重新计满一个窗口。
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void finishSeal(String key, Episode episode) {
        episode.orderedRequestIds = new ArrayList<>(episode.members.keySet());
        episode.orderedRequestIds.sort(String::compareTo);
        episode.winnerRequestId =
                episode.orderedRequestIds.get(episode.orderedRequestIds.size() - 1);
        episode.sealed = true;
        log.info("同键决胜封轮 key={} 批次大小={} 升序={} 胜者={}（判定依据: requestId 字典序最大）",
                key, episode.orderedRequestIds.size(), episode.orderedRequestIds,
                episode.winnerRequestId);
        episode.notifyAll();
    }
}
