/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.store.client;

import java.util.Objects;

/**
 * Immutable Object Pattern
 * <p>
 * created on 2021/10/26
 */
public final class HgNodePartition {
    private final Long nodeId;
    //当前key的hashcode
    private final Integer keyCode;

    //分区的开始结束范围
    private final Integer startKey;
    private final Integer endKey;
    private int hash = -1;

    HgNodePartition(Long nodeId, Integer keyCode) {
        this.nodeId = nodeId;
        this.keyCode = keyCode;
        this.startKey = this.endKey = keyCode;
    }

    HgNodePartition(Long nodeId, Integer keyCode, Integer startKey, Integer endKey) {
        this.nodeId = nodeId;
        this.keyCode = keyCode;
        this.startKey = startKey;
        this.endKey = endKey;
    }

    public static HgNodePartition of(Long nodeId, Integer keyCode) {
        return new HgNodePartition(nodeId, keyCode);
    }

    public static HgNodePartition of(Long nodeId, Integer keyCode, Integer startKey,
                                     Integer endKey) {
        return new HgNodePartition(nodeId, keyCode, startKey, endKey);
    }

    public Long getNodeId() {
        return nodeId;
    }

    public Integer getKeyCode() {
        return keyCode;
    }

    public Integer getStartKey() {
        return startKey;
    }

    public Integer getEndKey() {
        return endKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HgNodePartition that = (HgNodePartition) o;
        return Objects.equals(nodeId, that.nodeId) && Objects.equals(keyCode, that.keyCode);
    }

    @Override
    public int hashCode() {
        if (this.hash == -1) {
            this.hash = Objects.hash(nodeId, keyCode);
        }
        return this.hash;
    }

    @Override
    public String toString() {
        return "HgNodePartition{" +
               "nodeId=" + nodeId +
               ", partitionId=" + keyCode +
               '}';
    }
}
