package org.apache.hugegraph.pd.client;

import com.baidu.hugegraph.pd.grpc.watch.HgPdWatchGrpc;
import com.baidu.hugegraph.pd.grpc.watch.WatchCreateRequest;
import com.baidu.hugegraph.pd.grpc.watch.WatchNodeResponse;
import com.baidu.hugegraph.pd.grpc.watch.WatchPartitionResponse;
import com.baidu.hugegraph.pd.grpc.watch.WatchRequest;
import com.baidu.hugegraph.pd.grpc.watch.WatchResponse;
import com.baidu.hugegraph.pd.grpc.watch.WatchType;
import com.baidu.hugegraph.pd.watch.NodeEvent;
import com.baidu.hugegraph.pd.watch.PartitionEvent;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.function.Supplier;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/4
 */
final class PDWatchImpl implements PDWatch {

    private final HgPdWatchGrpc.HgPdWatchStub stub;

    // TODO: support several servers.
    PDWatchImpl(String pdServerAddress) {
        this.stub = HgPdWatchGrpc.newStub(getChannel(pdServerAddress));
    }

    private ManagedChannel getChannel(String target) {
        return ManagedChannelBuilder.forTarget(target).usePlaintext().build();
    }

    /**
     * Get Partition change watcher.
     *
     * @param listener
     * @return
     */
    @Override
    public Watcher watchPartition(Listener<PartitionEvent> listener) {
        return new PartitionWatcher(listener);
    }

    /**
     * Get Store-Node change watcher.
     *
     * @param listener
     * @return
     */
    @Override
    public Watcher watchNode(Listener<NodeEvent> listener) {
        return new NodeWatcher(listener);
    }

    @Override
    public Watcher watchGraph(Listener<WatchResponse> listener) {
        return new GraphWatcher(listener);
    }

    @Override
    public Watcher watchShardGroup(Listener<WatchResponse> listener) {
        return new ShardGroupWatcher(listener);
    }

    private class GraphWatcher extends AbstractWatcher<WatchResponse> {

        private GraphWatcher(Listener listener) {
            super(listener,
                  () -> WatchCreateRequest
                          .newBuilder()
                          .setWatchType(WatchType.WATCH_TYPE_GRAPH_CHANGE)
                          .build()
                 );
        }

        @Override
        public void onNext(WatchResponse watchResponse) {
            this.listener.onNext(watchResponse);
        }
    }

    private class ShardGroupWatcher extends AbstractWatcher<WatchResponse> {

        private ShardGroupWatcher(Listener listener) {
            super(listener,
                    () -> WatchCreateRequest
                            .newBuilder()
                            .setWatchType(WatchType.WATCH_TYPE_SHARD_GROUP_CHANGE)
                            .build()
            );
        }

        @Override
        public void onNext(WatchResponse watchResponse) {
            this.listener.onNext(watchResponse);
        }
    }

    private class PartitionWatcher extends AbstractWatcher<PartitionEvent> {

        private PartitionWatcher(Listener listener) {
            super(listener,
                    () -> WatchCreateRequest
                            .newBuilder()
                            .setWatchType(WatchType.WATCH_TYPE_PARTITION_CHANGE)
                            .build()
            );
        }

        @Override
        public void onNext(WatchResponse watchResponse) {
            WatchPartitionResponse res = watchResponse.getPartitionResponse();
            PartitionEvent event = new PartitionEvent(res.getGraph(), res.getPartitionId(),
                    PartitionEvent.ChangeType.grpcTypeOf(res.getChangeType()));
            this.listener.onNext(event);
        }
    }

    private class NodeWatcher extends AbstractWatcher<NodeEvent> {
        private NodeWatcher(Listener listener) {
            super(listener,
                    () -> WatchCreateRequest
                            .newBuilder()
                            .setWatchType(WatchType.WATCH_TYPE_STORE_NODE_CHANGE)
                            .build()
            );
        }

        @Override
        public void onNext(WatchResponse watchResponse) {
            WatchNodeResponse res = watchResponse.getNodeResponse();
            NodeEvent event = new NodeEvent(res.getGraph(), res.getNodeId(),
                    NodeEvent.EventType.grpcTypeOf(res.getNodeEventType()));
            this.listener.onNext(event);
        }
    }

    private abstract class AbstractWatcher<T> implements Watcher, StreamObserver<WatchResponse> {
        Listener<T> listener;
        StreamObserver<WatchRequest> reqStream;
        Supplier<WatchCreateRequest> requestSupplier;

        private AbstractWatcher(Listener<T> listener, Supplier<WatchCreateRequest> requestSupplier) {
            this.listener = listener;
            this.requestSupplier = requestSupplier;
            this.init();
        }

        void init() {
            this.reqStream = PDWatchImpl.this.stub.watch(this);
            this.reqStream.onNext(WatchRequest.newBuilder().setCreateRequest(
                    this.requestSupplier.get()
            ).build());
        }

        @Override
        public void close() {
            this.reqStream.onCompleted();
        }

        @Override
        public abstract void onNext(WatchResponse watchResponse);

        @Override
        public void onError(Throwable throwable) {
            this.listener.onError(throwable);
        }

        @Override
        public void onCompleted() {
            this.listener.onCompleted();
        }
    }

}
