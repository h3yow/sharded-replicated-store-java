package com.github.h3yow.shardedreplicatedstorejava.model;

import java.util.List;

/**
 * 一个分片的静态描述：分片 id、键区间、副本节点 id 列表。
 */
public record ShardSpec(String shardId, KeyRange range, List<String> replicas) {
}
