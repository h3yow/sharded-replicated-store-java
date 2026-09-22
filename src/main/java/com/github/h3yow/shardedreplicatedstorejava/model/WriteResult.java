package com.github.h3yow.shardedreplicatedstorejava.model;

/**
 * 写操作对外结论。
 * duplicate=true 表示这是重复提交，复用首次结论，不会被再次应用。
 */
public record WriteResult(boolean committed, boolean duplicate, long commitIndex,
                          int acks, int quorum, String decision) {
}
