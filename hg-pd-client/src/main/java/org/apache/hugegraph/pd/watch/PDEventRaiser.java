package org.apache.hugegraph.pd.watch;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hugegraph.pd.client.listener.PDEventListener;
import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.grpc.watch.WatchResponse;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com on 2023/12/7
 */
@Slf4j
public class PDEventRaiser {
    private final List<PDEventListener> listeners = new CopyOnWriteArrayList<>();
    private final Watcher pdWatch;

    public PDEventRaiser(Watcher pdWatch) {
        this.pdWatch = pdWatch;
        this.pdWatch.watchPartition(this::raisePartitionEvent);
        this.pdWatch.watchGraph(this::raiseGraphEvent);
        this.pdWatch.watchShardGroup(this::raiseShardGroupEvent);
        this.pdWatch.watchNode(this::raiseNodeEvent);
    }

    public void addListener(PDEventListener listener) {
        HgAssert.isArgumentNotNull(listener, "PDEventListener");
        this.listeners.add(listener);
    }

    public void removeListener(PDEventListener listener) {
        HgAssert.isArgumentNotNull(listener, "PDEventListener");
        this.listeners.remove(listener);
    }

    void raisePartitionEvent(PartitionEvent response) {
        listeners.forEach(listener -> listener.onPartitionChanged(response));
    }

    void raiseGraphEvent(WatchResponse response) {
        listeners.forEach(listener -> listener.onGraphChanged(response));
    }

    void raiseShardGroupEvent(WatchResponse response) {
        listeners.forEach(listener -> listener.onShardGroupChanged(response));
    }

    void raiseNodeEvent(NodeEvent response) {
        log.info("PDClient receive store event {} {}",
                 response.getEventType(), Long.toHexString(response.getNodeId()));
        listeners.forEach(listener -> listener.onStoreChanged(response));
    }

}
