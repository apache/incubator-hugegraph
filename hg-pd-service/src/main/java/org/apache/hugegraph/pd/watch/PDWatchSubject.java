package org.apache.hugegraph.pd.watch;

import static com.baidu.hugegraph.pd.common.HgAssert.isArgumentNotNull;

import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.pd.grpc.watch.NodeEventType;
import com.baidu.hugegraph.pd.grpc.watch.WatchChangeType;
import com.baidu.hugegraph.pd.grpc.watch.WatchCreateRequest;
import com.baidu.hugegraph.pd.grpc.watch.WatchRequest;
import com.baidu.hugegraph.pd.grpc.watch.WatchResponse;
import com.baidu.hugegraph.pd.grpc.watch.WatchType;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/4
 */
@Slf4j
@ThreadSafe
public class PDWatchSubject implements StreamObserver<WatchRequest> {
    public final static Map<String, AbstractWatchSubject> subjectHolder = new ConcurrentHashMap<>();
    private final static byte[] lock = new byte[0];

    private final StreamObserver<WatchResponse> responseObserver;
    private AbstractWatchSubject subject;
    private Long watcherId;

    static {
        subjectHolder.put(WatchType.WATCH_TYPE_PARTITION_CHANGE.name(), new PartitionChangeSubject());
        subjectHolder.put(WatchType.WATCH_TYPE_STORE_NODE_CHANGE.name(), new NodeChangeSubject());
        subjectHolder.put(WatchType.WATCH_TYPE_GRAPH_CHANGE.name(), new NodeChangeSubject());
        subjectHolder.put(WatchType.WATCH_TYPE_SHARD_GROUP_CHANGE.name(), new ShardGroupChangeSubject());
    }

    public static StreamObserver<WatchRequest> addObserver(StreamObserver<WatchResponse> responseObserver) {
        isArgumentNotNull(responseObserver, "responseObserver");
        return new PDWatchSubject(responseObserver);
    }

    /**
     * Notify partition change
     * @param changeType change type
     * @param graph name of graph
     * @param partitionId id of partition
     */
    public static void notifyPartitionChange(ChangeType changeType, String graph, int partitionId) {
        ((PartitionChangeSubject) subjectHolder.get(WatchType.WATCH_TYPE_PARTITION_CHANGE.name()))
                .notifyWatcher(changeType.getGrpcType(), graph, partitionId);

    }

    public static void notifyShardGroupChange(ChangeType changeType, int groupId, Metapb.ShardGroup group) {
        ((ShardGroupChangeSubject) subjectHolder.get(WatchType.WATCH_TYPE_SHARD_GROUP_CHANGE.name()))
                .notifyWatcher(changeType.getGrpcType(), groupId, group);
    }

    /**
     * Notify store-node change
     * @param changeType change type
     * @param graph name of graph
     * @param nodeId id of partition
     */
    public static void notifyNodeChange(NodeEventType changeType, String graph, long nodeId) {
        ((NodeChangeSubject) subjectHolder.get(WatchType.WATCH_TYPE_STORE_NODE_CHANGE.name()))
                .notifyWatcher(changeType, graph, nodeId);
    }

    public static void notifyChange(WatchType type,
                                    WatchResponse.Builder builder) {
        subjectHolder.get(type.name()).notifyWatcher(builder);
    }

    public static void notifyError(String message){
        subjectHolder.forEach((k, v)->{
            v.notifyError(message);
        });
    }
    
    private PDWatchSubject(StreamObserver<WatchResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    private static Long createWatcherId() {
        synchronized (lock) {
            Thread.yield();
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                log.error("Failed to sleep", e);
            }

            return System.currentTimeMillis();
        }

    }

    private void cancelWatcher() {

        if (this.subject == null) {
            this.responseObserver.onError(new Exception("Invoke cancel-watch before create-watch."));
            return;
        }

        this.subject.removeObserver(this.watcherId, this.responseObserver);
    }


    private WatchType getWatchType(WatchCreateRequest request) {
        WatchType watchType = request.getWatchType();

        if (watchType.equals(WatchType.WATCH_TYPE_UNKNOWN)) {
            this.responseObserver.onError(new Exception("unknown watch type."));
            return null;
        }

        return watchType;
    }

    private AbstractWatchSubject getSubject(WatchType watchType) {
        AbstractWatchSubject subject = subjectHolder.get(watchType.name());

        if (subject == null) {
            responseObserver.onError(new Exception("Unsupported watch-type: " + watchType.name()));
            return null;
        }

        return subject;
    }


    private void addWatcher(WatchCreateRequest request) {
        if (this.subject != null) {
            return;
        }
        WatchType watchType = getWatchType(request);
        if (watchType == null) return;

        this.subject = getSubject(watchType);
        this.watcherId = createWatcherId();

        this.subject.addObserver(this.watcherId, this.responseObserver);
    }

    @Override
    public void onNext(WatchRequest watchRequest) {

        if (watchRequest.hasCreateRequest()) {
            this.addWatcher(watchRequest.getCreateRequest());
            return;
        }

        if (watchRequest.hasCancelRequest()) {
            this.cancelWatcher();
        }

    }

    @Override
    public void onError(Throwable throwable) {
        this.cancelWatcher();
    }

    @Override
    public void onCompleted() {
        this.cancelWatcher();
    }

    public enum ChangeType {
        ADD(WatchChangeType.WATCH_CHANGE_TYPE_ADD),
        ALTER(WatchChangeType.WATCH_CHANGE_TYPE_ALTER),
        DEL(WatchChangeType.WATCH_CHANGE_TYPE_DEL),

        USER_DEFINED (WatchChangeType.WATCH_CHANGE_TYPE_SPECIAL1);

        private final WatchChangeType grpcType;

        ChangeType(WatchChangeType grpcType) {
            this.grpcType = grpcType;
        }

        public WatchChangeType getGrpcType() {
            return this.grpcType;
        }
    }

}
