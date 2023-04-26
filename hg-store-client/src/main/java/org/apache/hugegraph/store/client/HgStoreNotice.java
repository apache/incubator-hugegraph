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

import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hugegraph.store.client.type.HgNodeStatus;
import org.apache.hugegraph.store.client.util.HgAssert;

/**
 * 2021/11/16
 */
@NotThreadSafe
public class HgStoreNotice {
    private final Long nodeId;
    private final HgNodeStatus nodeStatus;
    private final String message;
    private Map<Integer, Long> partitionLeaders;
    private List<Integer> partitionIds;

    private HgStoreNotice(Long nodeId, HgNodeStatus nodeStatus, String message) {
        this.nodeId = nodeId;
        this.nodeStatus = nodeStatus;
        this.message = message;
    }

    public static HgStoreNotice of(Long nodeId, HgNodeStatus nodeStatus) {
        HgAssert.isArgumentNotNull(nodeId, "nodeId");
        HgAssert.isArgumentNotNull(nodeStatus, "nodeStatus");
        return new HgStoreNotice(nodeId, nodeStatus, "");
    }

    public static HgStoreNotice of(Long nodeId, HgNodeStatus nodeStatus, String message) {
        HgAssert.isArgumentNotNull(nodeId, "nodeId");
        HgAssert.isArgumentNotNull(nodeStatus, "nodeStatus");
        HgAssert.isArgumentNotNull(message, "message");

        return new HgStoreNotice(nodeId, nodeStatus, message);
    }

    public Long getNodeId() {
        return nodeId;
    }

    public HgNodeStatus getNodeStatus() {
        return nodeStatus;
    }

    public String getMessage() {
        return message;
    }

    public Map<Integer, Long> getPartitionLeaders() {
        return partitionLeaders;
    }

    public HgStoreNotice setPartitionLeaders(Map<Integer, Long> partitionLeaders) {
        this.partitionLeaders = partitionLeaders;
        return this;
    }

    public List<Integer> getPartitionIds() {
        return partitionIds;
    }

    public HgStoreNotice setPartitionIds(List<Integer> partitionIds) {
        this.partitionIds = partitionIds;
        return this;
    }

    @Override
    public String toString() {
        return "HgStoreNotice{" +
               "nodeId=" + nodeId +
               ", nodeStatus=" + nodeStatus +
               ", message='" + message + '\'' +
               ", partitionLeaders=" + partitionLeaders +
               ", partitionIds=" + partitionIds +
               '}';
    }
}
