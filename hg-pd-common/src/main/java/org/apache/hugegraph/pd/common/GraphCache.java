package org.apache.hugegraph.pd.common;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.collections4.CollectionUtils;

import org.apache.hugegraph.pd.grpc.Metapb.Graph;
import org.apache.hugegraph.pd.grpc.Metapb.Partition;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zhangyingjie
 * @date 2023/6/6
 **/
@Slf4j
@Data
public class GraphCache {

    private Graph graph;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private AtomicBoolean writing = new AtomicBoolean(false);
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Map<Integer, AtomicBoolean> state = new ConcurrentHashMap<>();
    private Map<Integer, Partition> partitions = new ConcurrentHashMap<>();
    private volatile RangeMap<Long, Integer> range = TreeRangeMap.create();

    public GraphCache(Graph graph) {
        this.graph = graph;
    }

    public void init(List<Partition> ps) {
        Map<Integer, Partition> gps = new ConcurrentHashMap<>(ps.size(), 1);
        if (!CollectionUtils.isEmpty(ps)) {
            WriteLock lock = getLock().writeLock();
            try {
                lock.lock();
                for (Partition p : ps) {
                    gps.put(p.getId(), p);
                    range.put(Range.closedOpen(p.getStartKey(), p.getEndKey()), p.getId());
                }
            } catch (Exception e) {
                log.warn("init graph with error:", e);
            } finally {
                lock.unlock();
            }
        }
        setPartitions(gps);

    }

    public Partition getPartition(Integer id) {
        return partitions.get(id);
    }

    public Partition addPartition(Integer id, Partition p) {
        return partitions.put(id, p);
    }

    public Partition removePartition(Integer id) {
        Partition p = partitions.get(id);
        if (p != null) {
            RangeMap<Long, Integer> range = getRange();
            if (Objects.equals(p.getId(), range.get(p.getStartKey())) &&
                Objects.equals(p.getId(), range.get(p.getEndKey() - 1))) {
                WriteLock lock = getLock().writeLock();
                lock.lock();
                try {
                    range.remove(range.getEntry(p.getStartKey()).getKey());
                } catch (Exception e) {
                    log.warn("remove partition with error:", e);
                } finally {
                    lock.unlock();
                }
            }
        }
        return partitions.remove(id);
    }

    public void removePartitions() {
        getState().clear();
        RangeMap<Long, Integer> range = getRange();
        WriteLock lock = getLock().writeLock();
        try {
            lock.lock();
            if (range != null) {
                range.clear();
            }
        } catch (Exception e) {
            log.warn("remove partition with error:", e);
        } finally {
            lock.unlock();
        }
        getPartitions().clear();
        getInitialized().set(false);
    }


    /*
     * 需要外部加写锁
     * */
    public void reset() {
        partitions.clear();
        try {
            range.clear();
        } catch (Exception e) {

        }
    }

    public boolean updatePartition(Partition partition) {
        int partId = partition.getId();
        Partition p = getPartition(partId);
        if (p != null && p.equals(partition)) {
            return false;
        }
        WriteLock lock = getLock().writeLock();
        try {
            lock.lock();
            RangeMap<Long, Integer> range = getRange();
            addPartition(partId, partition);
            try {
                if (p != null) {
                    // old [1-3) 被 [2-3)覆盖了。当 [1-3) 变成[1-2) 不应该删除原先的[1-3)
                    // 当确认老的 start, end 都是自己的时候，才可以删除老的. (即还没覆盖）
                    if (Objects.equals(partId, range.get(partition.getStartKey())) &&
                        Objects.equals(partId, range.get(partition.getEndKey() - 1))) {
                        range.remove(range.getEntry(partition.getStartKey()).getKey());
                    }
                }
                range.put(Range.closedOpen(partition.getStartKey(), partition.getEndKey()), partId);
            } catch (Exception e) {
                log.warn("update partition with error:", e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
        return true;
    }
}
