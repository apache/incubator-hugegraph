package org.apache.hugegraph.pd.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.common.ErrorType;
import org.apache.hugegraph.pd.grpc.common.Errors;
import org.apache.hugegraph.pd.grpc.common.ResponseHeader;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.raft.RaftStateListener;
import org.apache.hugegraph.pd.service.interceptor.RedirectInterceptor;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;

/**
 * @author zhangyingjie
 * @date 2022/6/21
 **/
public interface ServiceGrpc extends RaftStateListener {

    ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap();
    ManagedChannel channel = null;
    Logger log = LoggerFactory.getLogger(ServiceGrpc.class);

    default ResponseHeader getResponseHeader(PDException e) {
        Errors.Builder builder = Errors.newBuilder().setTypeValue(e.getErrorCode());
        if (!StringUtils.isEmpty(e.getMessage())) {
            builder.setMessage(e.getMessage());
        }
        Errors error = builder.build();
        ResponseHeader header = ResponseHeader.newBuilder().setError(error).build();
        return header;
    }

    default ResponseHeader getResponseHeader() {
        Errors error = Errors.newBuilder().setType(ErrorType.OK).build();
        ResponseHeader header = ResponseHeader.newBuilder().setError(error).build();
        return header;
    }

    default ResponseHeader getResponseHeader(int errorCode, String errorMsg) {
        Errors.Builder builder = Errors.newBuilder().setTypeValue(errorCode);
        if (!StringUtils.isEmpty(errorMsg)) {
            builder.setMessage(errorMsg);
        }
        Errors error = builder.build();
        ResponseHeader header = ResponseHeader.newBuilder().setError(error).build();
        return header;
    }

    default boolean isLeader() {
        return RaftEngine.getInstance().isLeader();
    }

    default <ReqT, RespT> void redirectToLeader(ManagedChannel channel, MethodDescriptor<ReqT, RespT> method,
                                                ReqT req, StreamObserver<RespT> observer) {
        try {
            String address = RaftEngine.getInstance().getLeaderGrpcAddress();
            if ((channel = channels.get(address)) == null || channel.isTerminated() || channel.isShutdown()) {
                synchronized (this) {
                    if ((channel = channels.get(address)) == null || channel.isTerminated() ||
                        channel.isShutdown()) {
                        while (channel != null && channel.isShutdown() && !channel.isTerminated()) {
                            channel.awaitTermination(50, TimeUnit.MILLISECONDS);
                        }
                        ManagedChannel c = ManagedChannelBuilder.forTarget(address)
                                                                .maxInboundMessageSize(Integer.MAX_VALUE)
                                                                .usePlaintext().usePlaintext().build();
                        channels.put(address, c);
                        channel = c;
                    }
                }
            }
            ClientCall<ReqT, RespT> call = new RedirectInterceptor().interceptCall(method,
                                                                                   CallOptions.DEFAULT,
                                                                                   channel);
            io.grpc.stub.ClientCalls.asyncUnaryCall(call, req, observer);
        } catch (Exception e) {
            log.error("Failed to redirect to leader", e);
        }
    }

    default <ReqT, RespT> void redirectToLeader(MethodDescriptor<ReqT, RespT> method, ReqT req,
                                                StreamObserver<RespT> observer) {
        redirectToLeader(channel, method, req, observer);
    }


    @Override
    default void onRaftLeaderChanged() {
    }
}
