package com.baidu.hugegraph.election;

import java.util.Objects;

public class MetaData {

    String node;
    long count;
    int epoch;

    public MetaData(String node, int epoch) {
        this.node = node;
        this.epoch = epoch;
        this.count = 1;
    }

    public void increaseCount() {
        this.count ++;
    }

    public boolean isMaster(String node) {
        return Objects.equals(this.node, node);
    }

    public int epoch() {
        return this.epoch;
    }

    public long count() {
        return this.count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetaData)) return false;
        MetaData metaData = (MetaData) o;
        return count == metaData.count && epoch == metaData.epoch && Objects.equals(node, metaData.node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(node, count, epoch);
    }
}
