package com.github.h3yow.shardedreplicatedstorejava.store;

/** 键值状态机中的一个版本值。 */
public record VersionedValue(String value, long version) {
}
