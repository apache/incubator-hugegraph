package org.apache.hugegraph.pd.pulse;

import static org.apache.hugegraph.pd.common.HgAssert.isArgumentNotNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import org.apache.hugegraph.pd.grpc.pulse.PdInstructionResponse;
import org.apache.hugegraph.pd.grpc.pulse.PdInstructionType;
import org.apache.hugegraph.pd.grpc.pulse.PulseAckRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseGraphResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseNodeResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseNoticeRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulsePartitionResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseShardGroupResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseType;
import org.apache.hugegraph.pd.grpc.pulse.StoreNodeEventType;
import org.apache.hugegraph.pd.notice.NoticeBroadcaster;
import org.apache.hugegraph.pd.util.IdUtil;
import com.google.protobuf.GeneratedMessageV3;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Collections;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/8
 * @version 2.0.0 added the watch suite on 2023/11/06
 */
@Slf4j
@ThreadSafe
public class PDPulseSubjects {
    private static final Map<String, AbstractObserverSubject> PULSE_SUBJECT_HOLDER = new ConcurrentHashMap<>();
    private static final Map<String, AbstractObserverSubject> NOTICE_SUBJECT_HOLDER = new ConcurrentHashMap<>();
    private static final Map<String, PulseType> PULSE_TYPE_HOLDER = new ConcurrentHashMap<>();

    static {
        RetryingHub.broadcasterProvider = PDPulseSubjects::createBroadcaster;
        BroadcasterFactory.subjectProvider = PDPulseSubjects::getSubjectViaNotice;
        RetryingSwitch.subjectProvider = PDPulseSubjects::getSubjectViaNotice;

        putPulseSubject(new PartitionHeartbeatSubject(), PartitionHeartbeatResponse.class);
        putPulseSubject(new PdInstructionSubject(), PdInstructionResponse.class);

        putPulseSubject(new PartitionChangeSubject(), PulsePartitionResponse.class);
        putPulseSubject(new StoreNodeChangeSubject(), PulseNodeResponse.class);
        putPulseSubject(new GraphChangeSubject(), PulseGraphResponse.class);
        putPulseSubject(new ShardGroupChangeSubject(), PulseShardGroupResponse.class);
    }

    static synchronized void putPulseSubject
            (AbstractObserverSubject subject, Class<? extends GeneratedMessageV3> noticeClass) {

        HgAssert.isArgumentNotNull(subject, "subject");
        HgAssert.isArgumentNotNull(noticeClass, "noticeClass");

        PulseType pulseType = subject.getPulseType();
        if (PULSE_SUBJECT_HOLDER.containsKey(pulseType.name())) {
            log.warn("Pulse type [ {} ] has been registered, will be replaced with subject: [ {} ]"
                    , pulseType.name(), subject.getClass().getTypeName());
        }

        log.info("Registering pulse type [ {} ] with subject: [ {} ]", pulseType, subject.getClass().getSimpleName());
        PULSE_SUBJECT_HOLDER.put(pulseType.name(), subject);
        NOTICE_SUBJECT_HOLDER.put(noticeClass.getTypeName(), subject);
        PULSE_TYPE_HOLDER.put(noticeClass.getTypeName(), pulseType);
    }

    public static void setDurableQueueProvider(PulseDurableProvider durableQueueProvider) {
        HgAssert.isArgumentNotNull(durableQueueProvider, "durableQueueProvider");
        RetryingHub.durableQueueProvider = durableQueueProvider;
        BroadcasterFactory.durableQueueProvider = durableQueueProvider;
        RetryingSwitch.pulseDurableProvider = durableQueueProvider;
    }

    /**
     * Add a responseObserver of client
     */
    public static StreamObserver<PulseRequest> addObserver(StreamObserver<PulseResponse> responseObserver) {
        isArgumentNotNull(responseObserver, "responseObserver");
        return SubjectIndividualObserver.of(responseObserver, getSubjectProvider())
                .setAckConsumer(getAckConsumer());
    }

    /**
     * Broadcasting notice to all PD clients.
     */
    public static void notifyClient(PartitionHeartbeatResponse.Builder responseBuilder) {
        HgAssert.isArgumentNotNull(responseBuilder, "responseBuilder");
        RetryingHub.addQueue(createBroadcaster(responseBuilder.build()).notifying());
    }

    /**
     * Dispatching notice to the specific PD client
     *
     * @return false if failed to dispatch notice
     */
    public static boolean notifyClient(PartitionHeartbeatResponse.Builder responseBuilder, long storeId) {
        return notifyClient(responseBuilder, Collections.singleton(storeId));
    }

    /**
     * Dispatching notice to the specific PD client
     *
     * @return false if failed to dispatch notice
     */
    public static boolean notifyClient(PartitionHeartbeatResponse.Builder responseBuilder, Collection<Long> storeIds) {
        HgAssert.isArgumentNotNull(responseBuilder, "responseBuilder");
        return RetryingSwitch.addNotice(responseBuilder.build(), storeIds);
    }

    /**
     * @param response
     * @see PDPulseSubjects::notifyPeerChange
     */
    @Deprecated
    public static void notifyClient(PdInstructionResponse response) {
        RetryingHub.addQueue(createBroadcaster(response).notifying());
    }

    public static void notifyError(int code, String message) {
        PULSE_SUBJECT_HOLDER.forEach((k, v) -> {
            v.notifyError(code, message);
        });
    }

    /**
     * Adding a notice listener for notices from the pd-client.
     *
     * @param listener
     */
    public static void listenPartitionHeartbeat(PulseListener<PartitionHeartbeatRequest> listener) {
        PULSE_SUBJECT_HOLDER.get(PulseType.PULSE_TYPE_PARTITION_HEARTBEAT.name()).addListener(createListenerId(), listener);
    }

    /**
     * Setting an interceptor for partition-heartbeat listener.
     * The interceptor will be invoked when an exception raised by the listener.
     * Continuing to invoke the next listener if interceptor returns 0.
     *
     * @param interceptor
     */
    public static void setPartitionErrInterceptor(BiFunction<PulseNoticeRequest, Exception, Integer> interceptor) {
        PULSE_SUBJECT_HOLDER.get(PulseType.PULSE_TYPE_PARTITION_HEARTBEAT.name()).setListenerErrInterceptor(interceptor);
    }

    /**************************************************************************
     *  The following methods are for internal use only.                      *
     * ***********************************************************************/
    private static BiConsumer<AbstractObserverSubject, PulseAckRequest> getAckConsumer() {
        return (subject, ack) -> {
            log.info("Receiving an ack of subject[{}], ack: {noticeId={},observerId={}}"
                    , subject.getPulseType(), ack.getNoticeId(), ack.getObserverId());
            if (!RetryingHub.removeNotice(ack.getNoticeId())) {
                RetryingSwitch.ackNotice(ack.getNoticeId(), ack.getObserverId());
            }
        };
    }

    private static AbstractObserverSubject getSubjectViaNotice(Class<? extends GeneratedMessageV3> noticeClass) {
        return PDPulseSubjects.NOTICE_SUBJECT_HOLDER.get(noticeClass.getTypeName());
    }

    private static NoticeBroadcaster createBroadcaster(GeneratedMessageV3 notice) {
        return BroadcasterFactory.create(notice);
    }

    private static Function<PulseType, AbstractObserverSubject> getSubjectProvider() {
        return (pulseType) -> PULSE_SUBJECT_HOLDER.get(pulseType.name());
    }

    private static AbstractObserverSubject getSubject(PulseType pulseType) {
        AbstractObserverSubject subject = PULSE_SUBJECT_HOLDER.get(pulseType.name());
        if (subject == null) {
            throw new IllegalArgumentException("Can't find the subject of pulseType: " + pulseType.name());
        }
        return subject;
    }

    private static Long createListenerId() {
        return IdUtil.createMillisId();
    }

    /**************************************************************************
     *  The following methods are for the implementation of watch             *
     *************************************************************************/

    public static void notifyPartitionChange(ChangeType changeType, String graph, int partitionId) {
        getSubject(PulseType.PULSE_TYPE_PARTITION_CHANGE).notifyClient(
                PulseNotices.createPartitionChange(changeType, graph, partitionId)
        );
    }

    public static void notifyShardGroupChange(ChangeType changeType, int groupId, Metapb.ShardGroup group) {
        getSubject(PulseType.PULSE_TYPE_SHARD_GROUP_CHANGE).notifyClient(
                PulseNotices.createShardGroupChange(changeType, groupId, group)
        );
    }

    public static void notifyNodeChange(StoreNodeEventType changeType, String graph, long nodeId) {
        getSubject(PulseType.PULSE_TYPE_STORE_NODE_CHANGE).notifyClient(
                PulseNotices.createNodeChange(changeType, graph, nodeId)
        );
    }

    public static void notifyGraphChange(Metapb.Graph graph) {
        getSubject(PulseType.PULSE_TYPE_GRAPH_CHANGE).notifyClient(
                PulseNotices.createGraphChange(graph)
        );
    }

    public static void notifyPeerChange(List<String> peers) {
        PdInstructionResponse.Builder builder = PdInstructionResponse.newBuilder();
        builder.setInstructionType(PdInstructionType.CHANGE_PEERS).addAllPeers(peers);
        notify(PulseType.PULSE_TYPE_PD_INSTRUCTION, builder.build());
    }

    public static void notify(PulseType type, GeneratedMessageV3 message){
        getSubject(type).notifyClient(message);
    }

}
