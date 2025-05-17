package org.apache.hugegraph.pd.client.impl;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.common.PDRuntimeException;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamDelegatorSender<ReqT, RespT> implements Closeable {

    private final AtomicBoolean isClosed = new AtomicBoolean(true);
    private AtomicReference<StreamObserver<ReqT>> observer = new AtomicReference<>();
    private Consumer<Void> reconnectedConsumer;
    private StreamDelegator<ReqT, RespT> delegator;

    public StreamDelegatorSender(StreamDelegator<ReqT, RespT> delegator) {
        this.delegator = delegator;
    }

    protected StreamDelegatorSender setReqStream(StreamDelegator<ReqT, RespT> delegator,
                                                 StreamObserver<ReqT> reqStream) {
        this.delegator = delegator;
        this.observer.set(reqStream);
        this.isClosed.set(false);
        return this;
    }

    protected StreamDelegatorSender updateReqStream(StreamObserver<ReqT> reqStream) {
        complete();
        this.observer.set(reqStream);
        this.isClosed.set(false);
        reconnect(null);
        return this;
    }

    private void reconnect(Void e) {
        if (this.reconnectedConsumer != null) {
            try {
                this.reconnectedConsumer.accept(e);
            } catch (Exception ex) {
                log.error("Failed to invoke [ reconnectedConsumer ], caused by: ", ex);
            }
        } else {
            log.info("Received a reconnection complete event.");
        }
    }

    public void onReconnected(Consumer<Void> reconnectedConsumer) {
        HgAssert.isArgumentNotNull(reconnectedConsumer, "connectedConsumer");
        this.reconnectedConsumer = reconnectedConsumer;
    }

    public void send(ReqT t) {
        HgAssert.isArgumentNotNull(t, "request");
        try {
            this.observer.get().onNext(t);
        } catch (Throwable e) {
            log.error("Failed to send to server, caused by: ", e);
            this.delegator.reconnect();
            throw new PDRuntimeException(-1, e);
        }
    }

    public void error(String error) {
        if (!this.isClosed.compareAndSet(false, true)) {
            log.warn("Aborted sending the error due the closure of the connection.");
            return;
        }
        this.delegator.resetState();
        Throwable t = new Throwable(error);
        log.error("Sender failed to invoke [onError], caused by: ", t);
        this.observer.get().onError(t);
    }

    public void close() {
        this.delegator.resetState();
        if (!this.isClosed.compareAndSet(false, true)) {
            return;
        }
        complete();
    }

    protected void complete() {
        try {
            StreamObserver<ReqT> observer = this.observer.get();
            if (observer != null) {
                observer.onCompleted();
            }
        } catch (Throwable e) {
            log.error("Sender failed to invoke [onCompleted], caused by: ", e);
        }
    }
}
