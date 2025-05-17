package org.apache.hugegraph.pd.pulse;

import org.apache.hugegraph.pd.grpc.pulse.PulseNoticeRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseType;
import org.apache.hugegraph.pd.util.IdUtil;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.hugegraph.pd.common.HgAssert.isArgumentNotNull;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/9
 */
@ThreadSafe
@Slf4j
abstract class AbstractObserverSubject {
    /* sending notices to the clients */
    private final ConcurrentHashMap<Long, StreamObserver<PulseResponse>> observerHolder = new ConcurrentHashMap<>(1024);
    /* notices from the clients */
    private final ConcurrentHashMap<Long, PulseListener> listenerHolder = new ConcurrentHashMap<>(1024);
    private BiFunction<PulseNoticeRequest, Exception, Integer> listenerErrInterceptor = defaultErrInterceptor();

    private final byte[] lock = new byte[0];
    private final PulseType pulseType;

    private static BiFunction<PulseNoticeRequest, Exception, Integer> defaultErrInterceptor() {
        return (req, err) -> {
            log.error("Failed to handle client's notice[{}], error: {}", req, err);
            return 0;
        };
    }

    public PulseType getPulseType() {
        return pulseType;
    }

    public void setListenerErrInterceptor(BiFunction<PulseNoticeRequest, Exception, Integer> listenerErrInterceptor) {
        isArgumentNotNull(listenerErrInterceptor, "listenerErrInterceptor");
        this.listenerErrInterceptor = listenerErrInterceptor;
    }

    protected AbstractObserverSubject(PulseType pulseType) {
        this.pulseType = pulseType;
    }

    /**
     * Adding an observer from the remote client.
     *
     * @param observerId
     * @param responseObserver
     */
    void addObserver(Long observerId, StreamObserver<PulseResponse> responseObserver) {
        synchronized (this.observerHolder) {

            if (this.observerHolder.containsKey(observerId)) {
                responseObserver.onError(
                        new Exception("The observer [" + observerId + "] of " + this.pulseType.name()
                                + " subject has been existing."));
                return;
            }

            this.observerHolder.put(observerId, responseObserver);
            log.info("Added an observer to subject [ {} ], observer-id: [ {} ], total observers: [ {} ]."
                    , this.pulseType, observerId, this.observerHolder.size());
        }

    }

    /**
     * Removing an observer by id
     *
     * @param observerId
     * @param responseObserver
     */
    void removeObserver(Long observerId, StreamObserver<PulseResponse> responseObserver) {
        synchronized (this.observerHolder) {
            log.info("Removing an observer of subject [ {} ], observer-id: [ {} ].", this.pulseType, observerId);
            this.observerHolder.remove(observerId);
        }

        responseObserver.onCompleted();
    }

    abstract String toNoticeString(PulseResponse res);

    /**
     * @param c
     * @return notice ID
     */
    protected long send2Clients(Consumer<PulseResponse.Builder> c, String originId) {
        synchronized (lock) {
            if (c == null) {
                log.error(this.pulseType.name() + "'s notice was abandoned, caused by: notifyObserver(null)");
                return -1;
            }

            if (originId == null) {
                originId = "nil";
            }
            PulseResponse.Builder resBuilder = PulseResponse.newBuilder();

            try {
                c.accept(resBuilder);
                resBuilder.setPulseType(this.pulseType);
                resBuilder.setOriginId(originId);
            } catch (Throwable t) {
                log.error(this.pulseType.name() + "'s notice was abandoned, caused by:", t);
                return -1;
            }

            long noticeId = IdUtil.createMillisId();
            Iterator<Map.Entry<Long, StreamObserver<PulseResponse>>> iter = observerHolder.entrySet().iterator();

            log.info("Broadcasting a notice to clients, subject: [ {} ], notice id: [ {} ], origin id: [ {} ]" +
                            ", observer count: [ {} ]", this.pulseType.name(), noticeId, originId,
                    observerHolder.size());

            while (iter.hasNext()) {
                Map.Entry<Long, StreamObserver<PulseResponse>> entry = iter.next();
                Long observerId = entry.getKey();
                PulseResponse res = resBuilder.setObserverId(observerId).setNoticeId(noticeId).build();

                try {
                    entry.getValue().onNext(res);
                } catch (Throwable e) {
                    log.error("Failed to send a notice to observer [ {} ] of subject [ {} ], caused by:",
                            observerId, this.pulseType.name(), e);

                    // TODO: ? try multi-times?
                    // iter.remove();
                    // log.error("Removed an observer [ {} ] of subject [ {} ]"
                    //        + ", because of once failure of sending.", entry.getKey(), this.pulseType.name(),e);
                }

            }

            return noticeId;
        }

    }

    protected long send2Clients(Consumer<PulseResponse.Builder> c, long noticeId, Collection<Long> observerIds) {
        if (observerIds == null || observerIds.isEmpty()) {
            return noticeId;
        }

        synchronized (lock) {
            if (c == null) {
                log.error(this.pulseType.name() + "'s notice was abandoned, caused by: notifyObserver(null)");
                return -1;
            }

            PulseResponse.Builder resBuilder = PulseResponse.newBuilder();

            try {
                c.accept(resBuilder);
                resBuilder.setPulseType(this.pulseType)
                        .setOriginId(String.valueOf(noticeId))
                        .setNoticeId(noticeId);
            } catch (Throwable t) {
                log.error(this.pulseType.name() + "'s notice was abandoned, caused by:", t);
                return -1;
            }

            log.info("Dispatching a notice to clients, subject: [ {} ], notice id: [ {} ], observer ids: {} "
                    // , content:\n{}"
                    , this.pulseType.name(), noticeId, observerIds);

            PulseResponse resPrototype = resBuilder.build();

            observerIds.parallelStream().forEach(observerId -> {
                StreamObserver<PulseResponse> observer = this.observerHolder.get(observerId);

                if (observer == null) {
                    log.warn("Failed to send a notice, because observer [ {} ] does not exist.",
                            observerId);
                    return;
                }

                try {
                    observer.onNext(PulseResponse.newBuilder(resPrototype).setObserverId(observerId).build());
                } catch (Throwable e) {
                    log.error("Failed to send a notice to observer [ {} ] of subject [ {} ], caused by:",
                            observerId, this.pulseType.name(), e);
                }

/*                log.info("Sent a notice, subject: [ {} ], notice id: [ {} ], observer id: [ {} ] "
                        // , content:\n{}"
                        , this.pulseType.name(), noticeId, observerId);*/
            });

            return noticeId;
        }

    }

    public long notifyClient(GeneratedMessageV3 response) {
        return this.notifyClient(response, null);
    }

    abstract long notifyClient(GeneratedMessageV3 response, String originId);

    abstract long notifyClient(GeneratedMessageV3 response, long noticeId, Collection<Long> observerIds);

    protected void notifyError(int code, String message) {
        synchronized (lock) {
            Iterator<Map.Entry<Long, StreamObserver<PulseResponse>>> iter = observerHolder.entrySet().iterator();
            PulseResponse.Builder resBuilder = PulseResponse.newBuilder();

            while (iter.hasNext()) {
                Map.Entry<Long, StreamObserver<PulseResponse>> entry = iter.next();
                Long observerId = entry.getKey();
                PulseResponse res = resBuilder.setObserverId(observerId).build();
                try {
                    entry.getValue().onError(Status.fromCodeValue(code).withDescription(message).asRuntimeException());
                } catch (Throwable e) {
                    log.warn("Failed to send {} 's notice[{}] to observer[{}], error:{}",
                            this.pulseType.name(), toNoticeString(res), observerId, e.getMessage());
                }
            }

        }
    }

    /**
     * Add a listener from local server
     *
     * @param listenerId
     * @param listener
     */
    void addListener(Long listenerId, PulseListener<?> listener) {
        synchronized (this.listenerHolder) {
            if (this.listenerHolder.containsKey(listenerId)) {
                listener.onError(
                        new Exception("The listener-id[" + listenerId + "] of " + this.pulseType.name()
                                + " subject has been existing."));
                return;
            }

            log.info("Adding a " + this.pulseType + "'s listener, listener-id is [" + listenerId + "].");
            this.listenerHolder.put(listenerId, listener);

        }
    }

    /**
     * Remove a listener by id
     *
     * @param listenerId
     * @param listener
     */
    void removeListener(Long listenerId, PulseListener<?> listener) {
        synchronized (this.listenerHolder) {
            log.info("Removing a " + this.pulseType + "'s listener, listener-id is [" + listenerId + "].");
            this.observerHolder.remove(listenerId);
        }

        listener.onCompleted();
    }

    abstract <T> Function<PulseNoticeRequest, T> getNoticeHandler();

    void handleClientNotice(PulseNoticeRequest noticeRequest) {
        Iterator<Map.Entry<Long, PulseListener>> iter = listenerHolder.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry<Long, PulseListener> entry = iter.next();
            Long listenerId = entry.getKey();
            try {
                entry.getValue().onNext(getNoticeHandler().apply(noticeRequest));
            } catch (Exception e) {
                int flag = 0;
                try {
                    flag = this.listenerErrInterceptor.apply(noticeRequest, e);
                } catch (Exception e1) {
                    log.error("Failed to invoke error interceptor with notice[{}], listenerId: {}, error: {}"
                            , noticeRequest, listenerId, e1);
                }
                if (flag != 0) {
                    break;
                }
            }
        }
    }

}
