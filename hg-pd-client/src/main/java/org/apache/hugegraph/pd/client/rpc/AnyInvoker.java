package org.apache.hugegraph.pd.client.rpc;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.hugegraph.pd.client.interceptor.Authentication;
import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.common.PDException;

import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com on 2023/12/21
 */
@Slf4j
public class AnyInvoker extends Invoker {

    public AnyInvoker(ConnectionManager cm, Function<Channel, AbstractStub> asyncStubCreator,
                      Function<Channel, AbstractBlockingStub> blockingStubCreator) {
        super(cm, asyncStubCreator, blockingStubCreator);
    }

    private <ReqT, RespT> ClientCall<ReqT, RespT> newBlockingClientCall(MethodDescriptor<ReqT, RespT> method,
                                                                        Channel channel) {
        Authentication auth = new Authentication(config.getUserName(), config.getAuthority());
        return auth.interceptCall(method, getBlockingCallOptions(), channel);
    }

    public <ReqT, RespT> RespT blockingCall(MethodDescriptor<ReqT, RespT> method, ReqT req) throws
                                                                                            PDException {
        return blockingCall(method, req, resp -> true);
    }

    public <ReqT, RespT> RespT blockingCall(MethodDescriptor<ReqT, RespT> method, ReqT req,
                                            long timeout) throws PDException {
        HgAssert.isArgumentNotNull(method, "method");
        HgAssert.isArgumentNotNull(req, "request");
        HgAssert.isTrue((timeout >= 0L), "timeout must be positive");
        return parallelCall(
                c -> ClientCalls.blockingUnaryCall(c, method, getBlockingCallOptions(timeout), req),
                resp -> true);
    }

    public <ReqT, RespT> RespT blockingCall(MethodDescriptor<ReqT, RespT> method, ReqT req,
                                            Predicate<RespT> predicate) throws PDException {
        HgAssert.isArgumentNotNull(predicate, "Predicate");
        return parallelCall(c -> ClientCalls.blockingUnaryCall(c, method, getBlockingCallOptions(), req),
                            predicate);
    }

    public <ReqT, RespT> void serverStreamingCall(MethodDescriptor<ReqT, RespT> methodDescriptor, ReqT request,
                                                  StreamObserver<RespT> responseObserver) throws PDException {
        throw new UnsupportedOperationException("Not support server streaming call");
    }

    public <ReqT, RespT> StreamObserver<ReqT> streamingCall(MethodDescriptor<ReqT, RespT> method,
                                                            StreamObserver<RespT> responseObserver) throws
                                                                                                    PDException {
        throw new UnsupportedOperationException("Not support streaming call");
    }

    private <T> T parallelCall(Function<Channel, T> mapper, Predicate<T> predicate) throws PDException {
        return this.cm.getParallelChannelStream()
                      .map(errorShutdown(mapper))
                      .filter(Objects::nonNull)
                      .filter(predicate)
                      .findAny()
                      .orElse(null);
    }

    private <T> Function<Channel, T> errorShutdown(Function<Channel, T> mapper) throws PDException {
        return channel -> {
            try {
                return mapper.apply(channel);
            } catch (Exception exception) {
                return null;
            }
        };
    }
}
