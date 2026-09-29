package com.github.h3yow.shardedreplicatedstorejava.store;

/**
 * 键值状态机中的一个版本值。
 *
 * @param winnerRequestId 这个可见值最初由哪个请求决胜产生（再平衡迁移后保持不变）
 */
public record VersionedValue(String value, long version, String winnerRequestId) {

    public VersionedValue(String value, long version) {
        this(value, version, null);
    }
}
