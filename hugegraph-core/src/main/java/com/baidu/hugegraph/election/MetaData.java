package com.baidu.hugegraph.election;

import java.util.Objects;

public class MetaData {

    String node;
    long count;
    int epoch;

    public MetaData(String node, int epoch) {
        this(node, epoch, 1);
    }

    public MetaData(String node, int epoch, long count) {
        this.node = node;
        this.epoch = epoch;
        this.count = count;
    }

    public void increaseCount() {
        this.count++;
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

    public void count(long count) {
        this.count = count;
    }

    public String node() {
        return this.node;
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

    @Override
    public String toString() {
        return "MetaData{" +
                "node='" + node + '\'' +
                ", count=" + count +
                ", epoch=" + epoch +
                '}';
    }
}
