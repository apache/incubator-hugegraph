package org.apache.hugegraph.pd.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.baidu.hugegraph.pd.grpc.pulse.HgPdPulseGrpc;
import com.baidu.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import com.baidu.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import com.baidu.hugegraph.pd.grpc.pulse.PulseAckRequest;
import com.baidu.hugegraph.pd.grpc.pulse.PulseCreateRequest;
import com.baidu.hugegraph.pd.grpc.pulse.PulseNoticeRequest;
import com.baidu.hugegraph.pd.grpc.pulse.PulseRequest;
import com.baidu.hugegraph.pd.grpc.pulse.PulseResponse;
import com.baidu.hugegraph.pd.grpc.pulse.PulseType;
import com.baidu.hugegraph.pd.pulse.PartitionNotice;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/9
 */
@Slf4j
final class PDPulseImpl implements PDPulse {

    private final HgPdPulseGrpc.HgPdPulseStub stub;

    private ExecutorService threadPool ;

    // TODO: support several servers.
    public PDPulseImpl(String pdServerAddress) {
        this.stub = HgPdPulseGrpc.newStub(getChannel(pdServerAddress));
        var namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("ack-notice-pool-%d").build();
        threadPool = Executors.newSingleThreadExecutor(namedThreadFactory);
    }

    private ManagedChannel getChannel(String target) {
        return ManagedChannelBuilder.forTarget(target).usePlaintext().build();
    }

    @Override
    public Notifier<PartitionHeartbeatRequest.Builder> connectPartition(Listener<PartitionHeartbeatResponse> listener) {
        return new PartitionHeartbeat(listener);
    }

    /*** PartitionHeartbeat's implement  ***/
    private class PartitionHeartbeat extends
                                     AbstractConnector<PartitionHeartbeatRequest.Builder, PartitionHeartbeatResponse> {
        private long observerId = -1;

        PartitionHeartbeat(Listener listener) {
            super(listener, PulseType.PULSE_TYPE_PARTITION_HEARTBEAT);
        }

        private void setObserverId(long observerId) {
            if (this.observerId == -1) {
                this.observerId = observerId;
            }
        }

        @Override
        public void notifyServer(PartitionHeartbeatRequest.Builder requestBuilder) {
            this.reqStream.onNext(PulseRequest.newBuilder()
                                              .setNoticeRequest(
                                                      PulseNoticeRequest.newBuilder()
                                                                        .setPartitionHeartbeatRequest(
                                                                                requestBuilder.build()
                                                                        ).build()
                                              ).build()
            );
        }

        @Override
        public void onNext(PulseResponse pulseResponse) {
            this.setObserverId(pulseResponse.getObserverId());
            long noticeId = pulseResponse.getNoticeId();
            PartitionHeartbeatResponse res = pulseResponse.getPartitionHeartbeatResponse();
            this.listener.onNext(res);
            this.listener.onNotice(new PartitionNotice(noticeId,
                                                       e -> super.ackNotice(e, observerId), res));
        }

    }

    private abstract class AbstractConnector<N, L> implements Notifier<N>, StreamObserver<PulseResponse> {
        Listener<L> listener;
        StreamObserver<PulseRequest> reqStream;
        PulseType pulseType;
        PulseRequest.Builder reqBuilder = PulseRequest.newBuilder();
        PulseAckRequest.Builder ackBuilder = PulseAckRequest.newBuilder();

        private AbstractConnector(Listener<L> listener, PulseType pulseType) {
            this.listener = listener;
            this.pulseType = pulseType;
            this.init();
        }

        void init() {
            PulseCreateRequest.Builder builder = PulseCreateRequest.newBuilder()
                    .setPulseType(this.pulseType);

            this.reqStream = PDPulseImpl.this.stub.pulse(this);
            this.reqStream.onNext(reqBuilder.clear().setCreateRequest(builder).build());
        }

        /*** notifier ***/
        @Override
        public void close() {
            this.reqStream.onCompleted();
        }

        @Override
        public abstract void notifyServer(N t);

        @Override
        public void crash(String error) {
            this.reqStream.onError(new Throwable(error));
        }

        /*** listener  ***/
        @Override
        public abstract void onNext(PulseResponse pulseResponse);

        @Override
        public void onError(Throwable throwable) {
            this.listener.onError(throwable);
        }

        @Override
        public void onCompleted() {
            this.listener.onCompleted();
        }

        protected void ackNotice(long noticeId, long observerId) {
            threadPool.execute(() -> {
                // log.info("send ack: {}, ts: {}", noticeId, System.currentTimeMillis());
                this.reqStream.onNext(reqBuilder.clear()
                        .setAckRequest(
                                this.ackBuilder.clear().setNoticeId(noticeId)
                                        .setObserverId(observerId).build()
                        ).build()
                );
            });
        }
    }

}
