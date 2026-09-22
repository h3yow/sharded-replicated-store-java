package com.github.h3yow.shardedreplicatedstorejava.store;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 键值状态机。
 *
 * <p>不变量：</p>
 * <ul>
 *   <li>apply 的 index 必须比上一次恰好大 1 —— 乱序/跳号由日志层拦截，这里做双保险；</li>
 *   <li>同一个键只接受 version 更大的条目，version 相同（重复迁移）幂等忽略，
 *       version 更小直接拒绝 —— 任何路径都不允许版本回退；</li>
 *   <li>只有日志意义上连续的条目才会被应用，保证多副本重放得到同一结果。</li>
 * </ul>
 */
public final class KvStateMachine {

    private final Map<String, VersionedValue> data = new LinkedHashMap<>();
    private long appliedIndex;

    public KvStateMachine() {
        this(0);
    }

    public KvStateMachine(long startIndex) {
        this.appliedIndex = startIndex;
    }

    /**
     * 应用一条已提交/迁移日志。
     *
     * @return 该键是否真正产生了新版本（false 表示同版本幂等忽略）
     */
    public synchronized boolean apply(long index, LogEntry entry) {
        if (index != appliedIndex + 1) {
            throw new IllegalStateException("状态机拒绝非连续应用: 期望 index="
                    + (appliedIndex + 1) + "，实际 index=" + index
                    + "，requestId=" + entry.requestId());
        }
        appliedIndex = index;

        VersionedValue current = data.get(entry.key());
        if (current != null) {
            if (entry.version() < current.version()) {
                throw new IllegalStateException("状态机拒绝版本回退: key=" + entry.key()
                        + " 当前 version=" + current.version()
                        + "，收到 version=" + entry.version()
                        + "，requestId=" + entry.requestId());
            }
            if (entry.version() == current.version()) {
                // 重复的迁移条目携带相同版本：幂等，不改变对外结论。
                return false;
            }
        }
        data.put(entry.key(), new VersionedValue(entry.value(), entry.version()));
        return true;
    }

    public synchronized VersionedValue get(String key) {
        return data.get(key);
    }

    public synchronized long appliedIndex() {
        return appliedIndex;
    }

    /** 不可变快照，供再平衡迁移枚举已确认数据。 */
    public synchronized Map<String, VersionedValue> snapshot() {
        return Map.copyOf(data);
    }
}
