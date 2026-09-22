package com.github.h3yow.shardedreplicatedstorejava.store;

/** 拓扑校验失败，携带可区分的违例原因与解释信息。 */
public class TopologyException extends RuntimeException {

    private final TopologyViolation violation;

    public TopologyException(TopologyViolation violation, String message) {
        super(message);
        this.violation = violation;
    }

    public TopologyViolation violation() {
        return violation;
    }
}
