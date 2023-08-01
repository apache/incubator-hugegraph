package org.apache.hugegraph.pd.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hugegraph.pd.grpc.Metapb.Graph;
import org.apache.hugegraph.pd.grpc.Metapb.Partition;

import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import lombok.Data;

@Data
public class GraphCache {

    private Graph graph;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private AtomicBoolean writing = new AtomicBoolean(false);
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Map<Integer, AtomicBoolean> state = new ConcurrentHashMap<>();
    private Map<Integer, Partition> partitions = new ConcurrentHashMap<>();
    private RangeMap<Long, Integer> range = TreeRangeMap.create();

    public GraphCache(Graph graph) {
        this.graph = graph;
    }

    public GraphCache() {
    }

    public Partition getPartition(Integer id) {
        return partitions.get(id);
    }

    public Partition addPartition(Integer id, Partition p) {
        return partitions.put(id, p);
    }

    public Partition removePartition(Integer id) {
        return partitions.remove(id);
    }
}
