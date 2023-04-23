package com.baidu.hugegraph.store.util;

import com.alipay.sofa.jraft.Status;

import java.util.HashMap;
import java.util.Map;

public enum HgRaftError{
    UNKNOWN(-1, "unknown"),
    OK(0, "OK"),
    NOT_LEADER(20000, "This partition is not leader"),
    WAIT_LEADER_TIMEOUT(20001, "Waiting for leader timeout"),
    NOT_LOCAL(20002, "This partition is not local"),
    CLUSTER_NOT_READY(20003, "The cluster is not ready, please check active stores number!"),

    TASK_CONTINUE(21000, "Task is continue"),
    TASK_ERROR(21001, "Task is error, need to retry"),
    END(30000, "HgStore error is end");

    private static final Map<Integer, HgRaftError> RAFT_ERROR_MAP = new HashMap<>();

    static {
        for (final HgRaftError error : HgRaftError.values()) {
            RAFT_ERROR_MAP.put(error.getNumber(), error);
        }
    }

    private final int value;

    private final String msg;

    HgRaftError(final int value, final String msg) {
        this.value = value;
        this.msg = msg;
    }

    public final int getNumber() {
        return this.value;
    }

    public static HgRaftError forNumber(final int value) {
        return RAFT_ERROR_MAP.getOrDefault(value, UNKNOWN);
    }

    public final String getMsg() {
        return this.msg;
    }

    public Status toStatus() {
        return new Status(value, msg);
    }
}

