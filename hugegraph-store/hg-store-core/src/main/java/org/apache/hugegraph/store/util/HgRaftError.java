/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.util;

import java.util.HashMap;
import java.util.Map;

import com.alipay.sofa.jraft.Status;

public enum HgRaftError {
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

    public static HgRaftError forNumber(final int value) {
        return RAFT_ERROR_MAP.getOrDefault(value, UNKNOWN);
    }

    public final int getNumber() {
        return this.value;
    }

    public final String getMsg() {
        return this.msg;
    }

    public Status toStatus() {
        return new Status(value, msg);
    }
}
