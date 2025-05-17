package org.apache.hugegraph.pd.client.impl;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.client.rpc.Invoker;
import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.common.PDRuntimeException;
import org.apache.hugegraph.pd.grpc.common.ErrorType;

import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class StreamDelegator<ReqT, RespT> {

    private final Invoker invoker;
    private final MethodDescriptor<ReqT, RespT> methodDesc;
    private final AtomicReference<ReqT> requestHolder = new AtomicReference<>();
    private final AtomicReference<StreamDelegatorState> state =
            new AtomicReference<>(StreamDelegatorState.IDLE);
    private final AtomicReference<StreamDelegatorSender> senderHolder = new AtomicReference<>();
    private final AtomicReference<Future> actionHolder = new AtomicReference<>();
    private final AtomicBoolean connecting = new AtomicBoolean();
    private final AtomicBoolean autoReconnect = new AtomicBoolean(true);
    private final AtomicReference<Throwable> lastError = new AtomicReference<>();
    @Getter
    private final String name;
    private Consumer<RespT> dataHandler = this::defaultHandler;
    private Consumer<Throwable> errorHandler = this::defaultHandler;
    private Consumer<Void> completeHandler = this::defaultHandler;

    public StreamDelegator(String delegatorName, Invoker invoker, MethodDescriptor<ReqT, RespT> methodDesc) {
        HgAssert.isArgumentValid(delegatorName, "delegator name");
        HgAssert.isArgumentNotNull(invoker, "stub invoker");
        HgAssert.isArgumentNotNull(methodDesc, "methodDesc");
        this.name = delegatorName;
        this.invoker = invoker;
        this.methodDesc = methodDesc;
    }

    public void close() {
        StreamDelegatorSender sender = this.senderHolder.get();
        if (sender != null) {
            sender.close();
        }
    }

    private <T> void defaultHandler(T t) {
        if (t instanceof Throwable) {
            error("Default handler received an error:", t);
            this.lastError.set((Throwable) t);
        } else if (t != null) {
            info("Default handler received a stream data: {}", t);
        }
    }

    public void listen(ReqT request, Consumer<RespT> dataHandler) {
        if (!this.state.compareAndSet(StreamDelegatorState.IDLE, StreamDelegatorState.LISTENING)) {
            info("It's not in the idle StreamDelegatorState, skip listening.", new Object[0]);
            throw new IllegalStateException(
                    "It's not in the idle StreamDelegatorState and not via the 'listening' method. ");
        }
        if (!this.requestHolder.compareAndSet(null, request)) {
            info("Already connected, skip listening.", new Object[0]);
            throw new IllegalStateException("Already connected, UNKNOWN StreamDelegatorState!");
        }
        this.dataHandler = dataHandler;
        try {
            listen2Server(request, new StreamDelegatorReceiver<>(this));
        } catch (RuntimeException e) {
            this.requestHolder.set(null);
            this.state.set(StreamDelegatorState.IDLE);
            throw e;
        }
    }

    public StreamDelegatorSender link(Consumer<RespT> dataHandler) {
        HgAssert.isArgumentNotNull(dataHandler, "data handler");
        if (!this.state.compareAndSet(StreamDelegatorState.IDLE, StreamDelegatorState.LINKING)) {
            info("It's not in the idle StreamDelegatorState, skip linking.", new Object[0]);
            throw new IllegalStateException(
                    "It's not in the idle StreamDelegatorState and not via the 'linking' method.");
        }
        if (this.senderHolder.get() != null) {
            info("Already connected, skip linking.", new Object[0]);
            return this.senderHolder.get();
        }
        if (!this.senderHolder.compareAndSet(null, new StreamDelegatorSender<>(this))) {
            info("Already connected, skip linking.", new Object[0]);
            return this.senderHolder.get();
        }
        this.dataHandler = dataHandler;
        try {
            this.senderHolder.get().setReqStream(this, link2Server(new StreamDelegatorReceiver<>(this)));
        } catch (Exception e) {
            this.senderHolder.set(null);
            this.state.set(StreamDelegatorState.IDLE);
            throw e;
        }
        return this.senderHolder.get();
    }

    private StreamObserver<ReqT> link2Server(StreamObserver<RespT> receiver) {
        try {
            return this.invoker.streamingCall(this.methodDesc, receiver);
        } catch (Exception e) {
            error("Failed to establish a link to the server, method type: {}, caused by: ", methodDesc, e);
            throw new PDRuntimeException(ErrorType.ERROR_VALUE, e);
        }
    }

    private void listen2Server(ReqT request, StreamObserver<RespT> receiver) {
        try {
            this.invoker.serverStreamingCall(this.methodDesc, request, receiver);
        } catch (Exception e) {
            error("Failed to set up a listening connection to the server, method type: {}, caused by: ",
                  methodDesc, e);
            throw new PDRuntimeException(ErrorType.ERROR_VALUE, e);
        }
    }

    public void reconnect() {
        reconnect(null);
    }

    public void reconnect(Throwable t) {
        if (this.connecting.compareAndSet(false, true)) {
            if (t != null) {
                log.warn("Received an error and trying to reconnect: ", t);
            }
            try {
                AtomicBoolean connected = new AtomicBoolean(false);
                int count = 0;
                while (!connected.get()) {
                    try {
                        count++;
                        StreamDelegatorSender sender = this.senderHolder.get();
                        ReqT request = this.requestHolder.get();
                        if (sender == null && request == null) {
                            info("The sender and request are both null, skip reconnecting.");
                            return;
                        }
                        if (sender != null) {
                            info("The [{}]th attempt to [linking]...", count);
                            sender.updateReqStream(link2Server(new StreamDelegatorReceiver<>(this)));
                        } else {
                            info("The [{}]th attempt to [listening]...", count);
                            listen2Server(request, new StreamDelegatorReceiver<>(this));
                        }
                        connected.set(true);
                        break;
                    } catch (Exception e) {
                        try {
                            error("Failed to reconnect, waiting [{}] seconds for the next attempt.", 3);
                            connected.set(false);
                            Thread.sleep(3000L);
                        } catch (InterruptedException ex) {
                            error("Failed to sleep thread and cancel the reconnecting process.", e);
                        }
                    }
                }
                if (connected.get()) {
                    info("Reconnect server successfully!");
                } else {
                    error("Reconnect server failed!");
                }
            } catch (Exception e) {
                warn("Failed to reconnect:", e);
            } finally {
                this.connecting.set(false);
            }
        }
    }

    protected void onNext(RespT res) {
        this.dataHandler.accept(res);
    }

    protected void onError(Throwable t) {
        if (this.autoReconnect.get()) {
            this.invoker.reconnect();
        } else {
            log.warn(this.name + " received an error and trying to reconnect: ", t);
        }
    }

    protected void onCompleted() {
        this.completeHandler.accept(null);
    }

    protected void resetState() {
        this.senderHolder.set(null);
        this.requestHolder.set(null);
        this.state.set(StreamDelegatorState.IDLE);
    }

    protected void info(String msg, Object... args) {
        log.info("[" + this.name + "] " + msg, args);
    }

    protected void error(String msg, Object... args) {
        log.error("[" + this.name + "] " + msg, args);
    }

    protected void warn(String msg, Object... args) {
        log.warn("[" + this.name + "] " + msg, args);
    }
}
