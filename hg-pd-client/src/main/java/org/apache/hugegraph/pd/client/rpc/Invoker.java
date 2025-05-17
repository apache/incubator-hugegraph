package org.apache.hugegraph.pd.client.rpc;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.common.PDException;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;

public abstract class Invoker {
    protected ConnectionManager cm;
    protected Function<Channel, AbstractStub> asCreator;
    protected Function<Channel, AbstractBlockingStub> bsCreator;
    protected AbstractStub<?> asyncStub;
    protected AbstractBlockingStub<?> blockingStub;
    protected PDConfig config;

    public Invoker(ConnectionManager cm, Function<Channel, AbstractStub> asCreator,
                   Function<Channel, AbstractBlockingStub> bsCreator) {
        this.cm = cm;
        this.config = this.cm.getConfig();
        this.asCreator = asCreator;
        this.bsCreator = bsCreator;
    }

    public abstract <ReqT, RespT> RespT blockingCall(MethodDescriptor<ReqT, RespT> paramMethodDescriptor,
                                                     ReqT paramReqT) throws PDException;

    public abstract <ReqT, RespT> RespT blockingCall(MethodDescriptor<ReqT, RespT> paramMethodDescriptor,
                                                     ReqT paramReqT, long paramLong) throws PDException;

    public <ReqT, RespT> RespT blockingCall(MethodDescriptor<ReqT, RespT> method, ReqT req,
                                            Predicate<RespT> predicate) throws PDException {
        HgAssert.isArgumentNotNull(predicate, "The predicate can't be null");
        RespT respT = blockingCall(method, req);
        if (predicate.test(respT)) {
            return respT;
        }
        return null;
    }

    public abstract <ReqT, RespT> void serverStreamingCall(
            MethodDescriptor<ReqT, RespT> methodDescriptor, ReqT paramReqT,
            StreamObserver<RespT> paramStreamObserver) throws PDException;

    public abstract <ReqT, RespT> StreamObserver<ReqT> streamingCall(
            MethodDescriptor<ReqT, RespT> paramMethodDescriptor,
            StreamObserver<RespT> paramStreamObserver) throws PDException;

    protected CallOptions getBlockingCallOptions() {
        return getBlockingCallOptions(this.cm.getDefaultDeadline());
    }

    protected CallOptions getBlockingCallOptions(long duration) {
        if (this.blockingStub == null) {
            this.blockingStub = this.cm.createBlockingStub(this.bsCreator);
        }
        return this.blockingStub.getCallOptions()
                                .withDeadlineAfter(duration, TimeUnit.MILLISECONDS);
    }

    protected CallOptions getStreamingCallOptions() {
        if (this.asyncStub == null) {
            this.asyncStub = this.cm.createAsyncStub(this.asCreator);
        }
        return this.asyncStub.getCallOptions();
    }

    protected Channel getChannel() {
        return this.cm.getLeaderChannel();
    }

    public void reconnect() {
        this.cm.reconnect();
    }
}
