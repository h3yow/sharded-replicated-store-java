package com.github.h3yow.shardedreplicatedstorejava.store;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * 将任意键稳定映射到分片哈希空间 [0, HASH_SPACE)。
 *
 * <p>使用 JDK 自带 CRC32（确定性、无外部依赖）；映射结果只依赖键字节，
 * 因此任意进程、任意时刻对同一个键得到的位点完全一致。</p>
 */
public final class KeyHasher {

    /** 分片哈希空间上界（不含）。取 2^20 便于区间校验与肉眼检查。 */
    public static final int HASH_SPACE = 1 << 20;

    private KeyHasher() {
    }

    /**
     * 返回键对应的哈希位点，范围 [0, HASH_SPACE)。
     */
    public static int hash(String key) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        CRC32 crc = new CRC32();
        crc.update(key.getBytes(StandardCharsets.UTF_8));
        return (int) (crc.getValue() % HASH_SPACE);
    }
}
