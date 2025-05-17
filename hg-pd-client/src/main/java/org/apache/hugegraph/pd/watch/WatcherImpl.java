package org.apache.hugegraph.pd.watch;

import java.io.Closeable;
import java.util.function.Function;

import org.apache.hugegraph.pd.client.PulseClient;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseType;
import org.apache.hugegraph.pd.grpc.watch.WatchResponse;
import org.apache.hugegraph.pd.pulse.DefaultPulseListener;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com on 2023/12/7
 */
@Slf4j
public class WatcherImpl implements Watcher {

    private final PulseClient pulse;
    private final Function<PulseResponse, PulseResponse> converter = response -> response;

    public WatcherImpl(PulseClient pulse) {
        this.pulse = pulse;
        DefaultPulseListener nodeListener = new DefaultPulseListener<>(WatcherImpl::parseNodeEvent);
        DefaultPulseListener partitionListener = new DefaultPulseListener<>(WatcherImpl::parsePartitionEvent);
        DefaultPulseListener graphListener = new DefaultPulseListener<>(WatcherImpl::parseGraphEvent);
        DefaultPulseListener shardGroupListener =
                new DefaultPulseListener<>(WatcherImpl::parseShardGroupEvent);
        DefaultPulseListener instructListener = new DefaultPulseListener<>(converter);
        this.pulse.connect(PulseType.PULSE_TYPE_STORE_NODE_CHANGE, nodeListener);
        this.pulse.connect(PulseType.PULSE_TYPE_PARTITION_CHANGE, partitionListener);
        this.pulse.connect(PulseType.PULSE_TYPE_GRAPH_CHANGE, graphListener);
        this.pulse.connect(PulseType.PULSE_TYPE_SHARD_GROUP_CHANGE, shardGroupListener);
        this.pulse.connect(PulseType.PULSE_TYPE_PD_INSTRUCTION, instructListener);
    }

    private static NodeEvent parseNodeEvent(PulseResponse response) {
        return NodeEvent.of(response.getNodeResponse());
    }

    private static PartitionEvent parsePartitionEvent(PulseResponse response) {
        return PartitionEvent.of(response.getPartitionResponse());
    }

    private static WatchResponse parseGraphEvent(PulseResponse response) {
        return PDWatchPulseConverter.toWatchGraphResponse(response);
    }

    private static WatchResponse parseShardGroupEvent(PulseResponse response) {
        return PDWatchPulseConverter.toWatchShardGroupResponse(response);
    }

    @Deprecated
    public String getCurrentHost() {
        return this.pulse.getLeaderAddress();
    }

    @Deprecated
    public boolean checkChannel() {
        return true;
    }

    public Closeable watch(PulseType type, WatchListener listener) {
        DefaultPulseListener<PulseResponse> pulseListener =
                (DefaultPulseListener<PulseResponse>) pulse.getListener(type);
        pulseListener.addWatchListener(listener);
        return () -> pulseListener.removeWatchListener(listener);
    }

    public Closeable watchPartition(WatchListener<PartitionEvent> listener) {
        return watch(PulseType.PULSE_TYPE_PARTITION_CHANGE, listener);
    }

    public Closeable watchNode(WatchListener<NodeEvent> listener) {
        return watch(PulseType.PULSE_TYPE_STORE_NODE_CHANGE, listener);
    }

    public Closeable watchGraph(WatchListener<WatchResponse> listener) {
        return watch(PulseType.PULSE_TYPE_GRAPH_CHANGE, listener);
    }

    public Closeable watchShardGroup(WatchListener<WatchResponse> listener) {
        return watch(PulseType.PULSE_TYPE_SHARD_GROUP_CHANGE, listener);
    }

    public Closeable watchPdPeers(WatchListener<PulseResponse> listener) {
        return watch(PulseType.PULSE_TYPE_PD_INSTRUCTION, listener);
    }

}
