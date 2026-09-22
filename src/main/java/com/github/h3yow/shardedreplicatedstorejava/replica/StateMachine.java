package com.github.h3yow.shardedreplicatedstorejava.replica;

import com.github.h3yow.shardedreplicatedstorejava.model.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * 分片状态机：按日志条目顺序应用。
 *
 * <p><b>并发写入决胜规则（确定性、可复现）</b>：同一 key 的最终版本取
 * {@code requestId} 字典序最大的已提交条目，与日志位点分配先后、线程调度
 * 均无关。因此多副本只要提交了相同的一组条目，最终状态必然相同；
 * 重复提交（同 requestId、同内容）不会重复应用，也不会导致版本回退。
 *
 * <p>DELETE 同样是一个带 requestId 的版本；每个 key 额外记录当前生效的
 * requestId（版本号），只会被更大的 requestId 覆盖。
 */
public class StateMachine {

    private static final Logger log = LoggerFactory.getLogger(StateMachine.class);

    private record Version(String requestId, String value) {
    }

    private final Map<String, Version> versions = new HashMap<>();
    private long appliedIndex;
    private final String displayName;

    public StateMachine() {
        this("statemachine");
    }

    public StateMachine(String displayName) {
        this.displayName = displayName;
    }

    public synchronized void apply(LogEntry entry) {
        // 幂等：同 requestId 已应用过则直接跳过（重复提交不重复应用）
        Version current = versions.get(entry.key());
        if (current != null && current.requestId().equals(entry.requestId())) {
            log.debug("[{}] 跳过重复应用 key={} requestId={} index={}",
                    displayName, entry.key(), entry.requestId(), entry.index());
            appliedIndex = Math.max(appliedIndex, entry.index());
            return;
        }
        boolean wins = current == null || entry.requestId().compareTo(current.requestId()) > 0;
        if (wins) {
            Version next = new Version(entry.requestId(),
                    entry.op() == LogEntry.Operation.DELETE ? null : entry.value());
            versions.put(entry.key(), next);
            log.info("[{}] 应用 key={} index={} version={} op={} 判定=新版本更大(当前={})",
                    displayName, entry.key(), entry.index(), entry.requestId(), entry.op(),
                    current == null ? "-" : current.requestId());
        } else {
            log.info("[{}] 应用 key={} index={} version={} op={} 判定=旧版本更大(当前={})，保持现状",
                    displayName, entry.key(), entry.index(), entry.requestId(), entry.op(),
                    current.requestId());
        }
        appliedIndex = Math.max(appliedIndex, entry.index());
    }

    public synchronized Optional<String> get(String key) {
        Version v = versions.get(key);
        return v == null ? Optional.empty() : Optional.ofNullable(v.value());
    }

    /** 当前生效版本号（requestId），用于测试断言与日志解释。 */
    public synchronized Optional<String> versionOf(String key) {
        Version v = versions.get(key);
        return v == null ? Optional.empty() : Optional.of(v.requestId());
    }

    public synchronized Map<String, String> snapshot() {
        Map<String, String> out = new TreeMap<>();
        versions.forEach((k, v) -> {
            if (v.value() != null) {
                out.put(k, v.value());
            }
        });
        return out;
    }

    public synchronized long appliedIndex() {
        return appliedIndex;
    }
}
