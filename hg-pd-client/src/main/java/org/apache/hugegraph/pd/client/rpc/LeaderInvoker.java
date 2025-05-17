package org.apache.hugegraph.pd.client.rpc;

import java.util.function.Function;

import org.apache.hugegraph.pd.client.interceptor.Authentication;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PDRuntimeException;
import org.apache.hugegraph.pd.grpc.common.ErrorType;

import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LeaderInvoker extends Invoker {

    private static final int MAX_RETRY = 10;
    private Authentication auth = new Authentication(this.config.getUserName(), this.config.getAuthority());

    public LeaderInvoker(ConnectionManager cm, Function<Channel, AbstractStub> asCreator,
                         Function<Channel, AbstractBlockingStub> bsCreator) {
        super(cm, asCreator, bsCreator);
    }

    public <ReqT, RespT> RespT blockingCall(MethodDescriptor<ReqT, RespT> method, ReqT req) throws
                                                                                            PDException {
        long deadline = this.cm.getDefaultDeadline();
        return blockingCall(method, req, deadline);
    }

    public <ReqT, RespT> RespT blockingCall(MethodDescriptor<ReqT, RespT> method, ReqT req,
                                            long timeout) throws PDException {
        return recurCall(channel -> {
            ClientCall<ReqT, RespT> call =
                    this.auth.interceptCall(method, getBlockingCallOptions(timeout), channel);
            return ClientCalls.blockingUnaryCall(call, req);
        }, method.getFullMethodName(), req, 0);
    }

    public <ReqT, RespT> void serverStreamingCall(MethodDescriptor<ReqT, RespT> methodDescriptor, ReqT request,
                                                  StreamObserver<RespT> observer) throws PDException {
        recurCall(channel -> {
            ClientCall<ReqT, RespT> call =
                    this.auth.interceptCall(methodDescriptor, getStreamingCallOptions(), channel);
            ClientCalls.asyncServerStreamingCall(call, request, observer);
            return Boolean.valueOf(true);
        }, methodDescriptor.getFullMethodName(), request, 0);
    }

    public <ReqT, RespT> StreamObserver<ReqT> streamingCall(MethodDescriptor<ReqT, RespT> method,
                                                            StreamObserver<RespT> observer) throws
                                                                                            PDException {
        return recurCall(channel -> {
            ClientCall<ReqT, RespT> call =
                    this.auth.interceptCall(method, getStreamingCallOptions(), channel);
            return ClientCalls.asyncBidiStreamingCall(call, observer);
        }, method.getFullMethodName(), observer, 0);
    }

    private <Q, S> S recurCall(Function<Channel, S> call, String methodName, Q req, int retry) throws
                                                                                               PDException {
        S t;
        Channel channel = getChannel();
        try {
            t = call.apply(channel);
        } catch (StatusRuntimeException | PDRuntimeException e) {
            if (e instanceof StatusRuntimeException){
                if (((StatusRuntimeException) e).getStatus().getCode().equals(Status.Code.UNAUTHENTICATED)){
                    throw new PDException(ErrorType.PD_UNAUTHENTICATED, e);
                }
            }
            if (retry == MAX_RETRY) {
                String s = req.toString();
                log.error("Failed to call [{}] in [{}] times, req: {}, caused by:", methodName, MAX_RETRY, s,
                          e);
                throw new PDException(ErrorType.PD_UNAVAILABLE, e);
            }
            if (retry > 1 && Channels.canNotWork(e)) {
                Status status = ((StatusRuntimeException) e).getStatus();
                if (status.getCode() == Status.Code.UNAVAILABLE) {
                    cm.reconnect(true);
                } else {
                    cm.reconnect();
                }
            }
            try {
                log.info("Retrying to call [{}] after [{}] seconds.", methodName, retry);
                synchronized (e) {
                    e.wait(retry * 1000L + 100L);
                }
            } catch (Exception ex) {
                log.error("Failed to sleep, caused by:", ex);
                throw new PDException(ErrorType.ERROR, ex);
            }
            return recurCall(call, methodName, req, retry + 1);
        } catch (Throwable e) {
            log.error("Failed to call [{}] without retrying, req: {}, caused by:", methodName,
                      req.toString(), e);
            throw new PDException(ErrorType.ERROR, e);
        }
        return t;
    }
}
