package com.github.h3yow.shardedreplicatedstorejava.exception;

/** 拓扑非法时抛出，reason 区分重叠 / 空洞 / 未覆盖 / 配置错误。 */
public class TopologyValidationException extends StoreException {
    public TopologyValidationException(String reason, String message) {
        super(reason, message);
    }
}
