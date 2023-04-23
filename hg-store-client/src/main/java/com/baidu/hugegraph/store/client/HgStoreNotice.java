package com.baidu.hugegraph.store.client;

import com.baidu.hugegraph.store.client.type.HgNodeStatus;
import com.baidu.hugegraph.store.client.util.HgAssert;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author lynn.bond@hotmail.com on 2021/11/16
 */
@NotThreadSafe
public class HgStoreNotice {
    private Long nodeId;
    private HgNodeStatus nodeStatus;
    private String message;
    private Map<Integer, Long> partitionLeaders;
    private List<Integer> partitionIds;

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

    private HgStoreNotice(Long nodeId, HgNodeStatus nodeStatus, String message) {
        this.nodeId = nodeId;
        this.nodeStatus = nodeStatus;
        this.message = message;
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
