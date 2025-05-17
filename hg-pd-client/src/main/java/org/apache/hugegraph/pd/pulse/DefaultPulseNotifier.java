package org.apache.hugegraph.pd.pulse;

import org.apache.hugegraph.pd.client.impl.StreamDelegatorSender;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseAckRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseCreateRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseNoticeRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseType;

public class DefaultPulseNotifier<T> implements PulseNotifier<T> {

    private final PulseType pulseType;
    private final StreamDelegatorSender sender;

    private long observerId;

    public DefaultPulseNotifier(PulseType pulseType, StreamDelegatorSender sender, long observerId) {
        this.pulseType = pulseType;
        this.sender = sender;
        this.observerId = observerId;
        this.sender.onReconnected(o -> start());
    }

    public void start() {
        send(PulseRequest.newBuilder()
                .setCreateRequest(PulseCreateRequest.newBuilder()
                        .setPulseType(this.pulseType)
                        .setObserverId(this.observerId)
                ));
    }

    public void ack(long noticeId, long observerId) {
        send(PulseRequest.newBuilder()
                         .setAckRequest(
                                 PulseAckRequest.newBuilder().setNoticeId(noticeId)
                                                .setObserverId(observerId)));
    }

    public void send(PulseRequest.Builder builder) {
        this.sender.send(builder.build());
    }

    @Override
    public void close() {
        this.sender.close();
    }

    public void notifyServer(T request) {
        this.sender.send(this.getRequest(request));
    }

    public void crash(String error) {
        this.sender.error(error);
    }

    private <T> PulseRequest getRequest(T requestBuilder) {
        PulseNoticeRequest.Builder builder = PulseNoticeRequest.newBuilder();
        if (requestBuilder instanceof PartitionHeartbeatRequest.Builder) {
            builder.setPartitionHeartbeatRequest((PartitionHeartbeatRequest.Builder) requestBuilder);
        } else {
            throw new IllegalStateException("Unregistered request type: " + requestBuilder.getClass());
        }
        return PulseRequest.newBuilder().setNoticeRequest(builder).build();
    }
}
