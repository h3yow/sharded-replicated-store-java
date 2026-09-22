package com.github.h3yow.shardedreplicatedstorejava.util;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * 日志条目校验和工具。
 *
 * <p>对条目的逻辑内容（index/term/requestId/key/value/op）做规范化拼接后计算 CRC32，
 * 任一字节被篡改都会导致校验和不符；value 为 null（DELETE）也纳入计算。
 */
public final class Checksums {
    private Checksums() {
    }

    public static long of(long index, long term, String requestId, String key,
                          String value, String op) {
        String canonical = index + "|" + term + "|" + nz(requestId) + "|"
                + nz(key) + "|" + (value == null ? "\0" : value) + "|" + nz(op);
        CRC32 crc = new CRC32();
        crc.update(canonical.getBytes(StandardCharsets.UTF_8));
        return crc.getValue();
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }
}
