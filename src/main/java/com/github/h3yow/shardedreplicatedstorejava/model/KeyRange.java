package com.github.h3yow.shardedreplicatedstorejava.model;

/**
 * 键空间分片区间：左闭右开 [{@link #start()}, {@link #end()})。
 * hash 空间整体为 [0, ringSize) 的线性（非环形）空间，区间不回绕。
 */
public record KeyRange(int start, int end) {

    public KeyRange {
        if (start < 0 || end < 0) {
            throw new IllegalArgumentException("range bounds must be non-negative: " + start + "," + end);
        }
        if (end < start) {
            throw new IllegalArgumentException("range end before start: " + start + "," + end);
        }
    }

    /** 是否包含 hash；空区间（start == end）不包含任何值。 */
    public boolean contains(int hash) {
        return hash >= start && hash < end;
    }

    public int length() {
        return end - start;
    }

    @Override
    public String toString() {
        return "[" + start + "," + end + ")";
    }
}
