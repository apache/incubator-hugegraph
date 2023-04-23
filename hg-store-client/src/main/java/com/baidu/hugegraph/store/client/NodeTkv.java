package com.baidu.hugegraph.store.client;

import com.baidu.hugegraph.store.HgOwnerKey;
import com.baidu.hugegraph.store.HgStoreSession;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Arrays;
import java.util.Objects;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/26
 */
@ThreadSafe
class NodeTkv {
    private final HgNodePartition nodePartition;
    private final String table;
    private final HgOwnerKey key;
    private final HgOwnerKey endKey;
    private HgStoreSession session;
    NodeTkv(HgNodePartition nodePartition, String table, HgOwnerKey key) {
        this.nodePartition = nodePartition;
        this.table = table;
        this.key = key;
        this.endKey = key;
        this.key.setKeyCode(this.nodePartition.getKeyCode());
    }
    NodeTkv(HgNodePartition nodePartition, String table, HgOwnerKey key, int keyCode) {
        this.nodePartition = nodePartition;
        this.table = table;
        this.key = key;
        this.endKey = key;

        this.key.setKeyCode(keyCode);
    }
    NodeTkv(HgNodePartition nodePartition, String table, HgOwnerKey startKey,
            HgOwnerKey endKey) {
        this.nodePartition = nodePartition;
        this.table = table;
        this.key = startKey;
        this.endKey = endKey;
        this.key.setKeyCode(nodePartition.getStartKey());
        this.endKey.setKeyCode(nodePartition.getEndKey());
    }

    public Long getNodeId() {
        return this.nodePartition.getNodeId();
    }

    public String getTable() {
        return table;
    }

    public HgOwnerKey getKey() {
        return key;
    }

    public HgOwnerKey getEndKey() {
        return endKey;
    }

    public NodeTkv setKeyCode(int code){
        this.key.setKeyCode(code);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeTkv nptKv = (NodeTkv) o;
        return Objects.equals(nodePartition, nptKv.nodePartition) && Objects.equals(table, nptKv.table)
                && Objects.equals(key, nptKv.key)
                && Objects.equals(endKey, nptKv.endKey);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(nodePartition, table, key, endKey);
        return result;
    }

    @Override
    public String toString() {
        return "NptKv{" +
                "nodePartition=" + nodePartition +
                ", table='" + table + '\'' +
                ", key=" + key +
                ", endKey=" + endKey +
                '}';
    }

    public HgStoreSession getSession() {
        return session;
    }

    public void setSession(HgStoreSession session) {
        this.session = session;
    }
}
