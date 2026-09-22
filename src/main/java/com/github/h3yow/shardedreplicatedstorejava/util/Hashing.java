package com.github.h3yow.shardedreplicatedstorejava.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * 键哈希工具：把字符串键稳定映射到 [0, ringSize) 整数空间。
 *
 * <p>使用 SHA-256 的前 4 字节（无符号）取模，保证：
 * <ul>
 *   <li>同一键在任何进程、任何时间映射结果一致（可复现）；</li>
 *   <li>不同键均匀散布在固定整数键空间上，便于用离散区间描述分片。</li>
 * </ul>
 */
public final class Hashing {
    private Hashing() {
    }

    public static int hashToRing(String key, int ringSize) {
        if (ringSize <= 0) {
            throw new IllegalArgumentException("ringSize must be positive: " + ringSize);
        }
        byte[] digest;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
        // 取前 4 字节视为无符号 32 位整数
        long h = ((long) (digest[0] & 0xFF) << 24)
                | ((digest[1] & 0xFF) << 16)
                | ((digest[2] & 0xFF) << 8)
                | (digest[3] & 0xFF);
        return (int) (h % ringSize);
    }
}
