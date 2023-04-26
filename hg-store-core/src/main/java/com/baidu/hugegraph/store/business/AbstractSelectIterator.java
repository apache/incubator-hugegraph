package com.baidu.hugegraph.store.business;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.backend.serializer.AbstractSerializer;
import com.baidu.hugegraph.backend.serializer.BinarySerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.iterator.CIter;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.util.Bytes;

import lombok.extern.slf4j.Slf4j;

/**
 * @author zhangyingjie
 * @date 2022/10/26
 **/
@Slf4j
public abstract class AbstractSelectIterator implements ScanIterator {
    protected ScanIterator iterator;
    protected AbstractSerializer serializer;

    public AbstractSelectIterator() {
        this.serializer = new BinarySerializer();
    }

    public boolean belongToMe(BackendEntry entry,
                              BackendEntry.BackendColumn column) {
        return Bytes.prefixWith(column.name, entry.id().asBytes());
    }

    public HugeElement parseEntry(BackendEntry entry, boolean isVertex) {
        try {
            if (isVertex) {
                return this.serializer.readVertex(null, entry);
            } else {
                CIter<Edge> itr =
                        this.serializer.readEdges(null, entry, true, false);

                // Iterator<HugeEdge> itr = this.serializer.readEdges(
                //         null, entry, true, false).iterator();
                HugeElement el = null;
                if (itr.hasNext()) {
                    el = (HugeElement) itr.next();
                }
                return el;
            }
        } catch (Exception e) {
            log.error("Failed to parse entry: {}", entry, e);
            throw e;
        }
    }
}
