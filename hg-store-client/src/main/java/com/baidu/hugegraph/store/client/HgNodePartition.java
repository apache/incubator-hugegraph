package com.baidu.hugegraph.store.client;

import java.util.Objects;

/**
 * Immutable Object Pattern
 *
 * @author lynn.bond@hotmail.com created on 2021/10/26
 */
public final class HgNodePartition {
    private Long nodeId;
    //当前key的hashcode
    private Integer keyCode;

    //分区的开始结束范围
    private Integer startKey;
    private Integer endKey;
    private int hash = -1;

    public static HgNodePartition of(Long nodeId, Integer keyCode) {
        return new HgNodePartition(nodeId, keyCode);
    }
    public static HgNodePartition of(Long nodeId, Integer keyCode, Integer startKey, Integer endKey) {
        return new HgNodePartition(nodeId, keyCode, startKey, endKey);
    }

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
    public Long getNodeId() {
        return nodeId;
    }

    public Integer getKeyCode() {
        return keyCode;
    }

    public Integer getStartKey(){
        return startKey;
    }

    public Integer getEndKey(){
        return endKey;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HgNodePartition that = (HgNodePartition) o;
        return Objects.equals(nodeId, that.nodeId) && Objects.equals(keyCode, that.keyCode);
    }

    @Override
    public int hashCode() {
        if(this.hash==-1)this.hash=Objects.hash(nodeId, keyCode);
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
