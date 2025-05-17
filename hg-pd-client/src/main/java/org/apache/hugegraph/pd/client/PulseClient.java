package org.apache.hugegraph.pd.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.hugegraph.pd.client.impl.StreamDelegator;
import org.apache.hugegraph.pd.client.impl.StreamDelegatorSender;
import org.apache.hugegraph.pd.client.support.PDExecutors;
import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.grpc.pulse.HgPdPulseGrpc;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseType;
import org.apache.hugegraph.pd.pulse.DefaultPulseNotifier;
import org.apache.hugegraph.pd.pulse.Pulse;
import org.apache.hugegraph.pd.pulse.PulseListener;
import org.apache.hugegraph.pd.pulse.PulseNotifier;
import org.apache.hugegraph.pd.pulse.PulseResponseNotice;
import org.apache.hugegraph.pd.pulse.PulseServerNotice;
import com.google.protobuf.GeneratedMessageV3;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com on 2023/11/20
 * @version 3.0.1 removed the `noticeParserMap` on 2024/01/08
 */
@Slf4j
public class PulseClient extends BaseClient implements Pulse {

    private final byte[] lock = new byte[0];
    private final Map<PulseType, PulseListener<PulseResponse>> listeners = new ConcurrentHashMap<>();
    private final Map<PulseType, DefaultPulseNotifier<?>> notifiers = new ConcurrentHashMap<>();
    private final Map<PulseType, StreamDelegator<PulseRequest, PulseResponse>> delegators =
            new ConcurrentHashMap<>();
    private final ExecutorService threadPool = PDExecutors.newQueuingPool("pulse-ack", 1);

    @Getter
    @Setter
    private long observerId;

    public PulseClient(PDConfig config) {
        super(config, HgPdPulseGrpc::newStub, HgPdPulseGrpc::newBlockingStub);
    }

    public PulseNotifier<PartitionHeartbeatRequest.Builder> connect(PulseListener<PulseResponse> listener) {
        return connect(PulseType.PULSE_TYPE_PARTITION_HEARTBEAT, listener);
    }

    public <T extends GeneratedMessageV3.Builder> PulseNotifier<T> connect(PulseType pulseType,
                                                                           PulseListener<PulseResponse> listener) {
        HgAssert.isArgumentNotNull(listener, "listener");
        this.listeners.put(pulseType, listener);
        DefaultPulseNotifier notifier = this.notifiers.get(pulseType);
        if (notifier == null) {
            synchronized (this.lock) {
                notifier = this.notifiers.computeIfAbsent(pulseType,
                        k -> new DefaultPulseNotifier(pulseType, newStreaming(pulseType), this.observerId)
                );
                notifier.start();
            }
        }
        return notifier;
    }

    public boolean resetStub(String host, PulseNotifier notifier) {
        return true;
    }

    private StreamDelegatorSender newStreaming(PulseType pulseType) {
        StreamDelegator delegator = delegators.computeIfAbsent(pulseType,
                                                               k -> new StreamDelegator(pulseType.name(),
                                                                                        getLeaderInvoker(),
                                                                                        HgPdPulseGrpc.getPulseMethod()));
        return delegator.link(response -> handleOnNext(pulseType, (PulseResponse) response));
    }

    public PulseListener<PulseResponse> getListener(PulseType pulseType) {
        return this.listeners.get(pulseType);
    }

    private PulseServerNotice<PulseResponse> toPulseResponseNotice(PulseResponse pulseResponse) {
        return new PulseResponseNotice(pulseResponse.getNoticeId(),
                                       e -> ackNotice(
                                               pulseResponse.getPulseType(),
                                               pulseResponse.getNoticeId(),
                                               pulseResponse.getObserverId()),
                                       pulseResponse);
    }

    private void handleOnNext(PulseType pulseType, PulseResponse response) {
        PulseServerNotice<PulseResponse> notice = toPulseResponseNotice(response);
        PulseListener<PulseResponse> listener = getListener(pulseType);
        if (listener != null) {
            try {
                listener.onNext(response);
                listener.onNotice(notice);
            } catch (Throwable e) {
                log.error("Listener failed to handle notice: \n{}, caused by: ", response, e);
            }
        }
    }

    private void ackNotice(PulseType pulseType, long noticeId, long observerId) {
        DefaultPulseNotifier<?> sender = this.notifiers.get(pulseType);
        if (sender == null) {
            log.error("Sender is null, pulse type: {}", pulseType);
            throw new IllegalStateException("Sender is null, pulse type: " + pulseType);
        }
        sendAck(sender, noticeId, observerId);
    }

    private void sendAck(DefaultPulseNotifier<?> sender, long noticeId, long observerId) {
        this.threadPool.execute(() -> {
            log.info("Sending ack, notice id: {}, observer id: {}, ts: {}", noticeId, observerId,
                     System.currentTimeMillis());
            sender.ack(noticeId, observerId);
        });
    }

    public void onLeaderChanged(String leader) {
        this.delegators.entrySet().parallelStream().forEach(e -> {
            try {
                e.getValue().reconnect();
            } catch (Exception ex) {
                log.warn("reconnect to leader with error:", ex);
            }
        });
    }
}
