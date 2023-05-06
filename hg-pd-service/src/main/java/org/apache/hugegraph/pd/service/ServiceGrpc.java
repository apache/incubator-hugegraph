package org.apache.hugegraph.pd.service;

import org.apache.hugegraph.pd.watch.PDWatchSubject;

import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.pulse.PDPulseSubject;

import com.baidu.hugegraph.pd.raft.RaftEngine;
import com.baidu.hugegraph.pd.raft.RaftStateListener;
import com.baidu.hugegraph.pd.watch.PDWatchSubject;

import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;

/**
 * @author zhangyingjie
 * @date 2022/6/21
 **/
public interface ServiceGrpc extends RaftStateListener {

    default Pdpb.ResponseHeader getResponseHeader(PDException e) {
        Pdpb.Error error = Pdpb.Error.newBuilder().setTypeValue(e.getErrorCode()).setMessage(e.getMessage()).build();
        Pdpb.ResponseHeader header = Pdpb.ResponseHeader.newBuilder().setError(error).build();
        return header;
    }

    default Pdpb.ResponseHeader getResponseHeader() {
        Pdpb.Error error = Pdpb.Error.newBuilder().setType(Pdpb.ErrorType.OK).build();
        Pdpb.ResponseHeader header = Pdpb.ResponseHeader.newBuilder().setError(error).build();
        return header;
    }

    default boolean isLeader() {
        return RaftEngine.getInstance().isLeader();
    }

    ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap();

    default <ReqT, RespT> void redirectToLeader(ManagedChannel channel, MethodDescriptor<ReqT, RespT> method,
                                                ReqT req, io.grpc.stub.StreamObserver<RespT> observer) {
        try {
            String address = RaftEngine.getInstance().getLeaderGrpcAddress();
            if ((channel = channels.get(address)) == null) {
                synchronized (this) {
                    if ((channel = channels.get(address)) == null) {
                        ManagedChannel c = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
                        channels.put(address, c);
                        channel = c;
                    }
                }
            }
            io.grpc.stub.ClientCalls.asyncUnaryCall(channel.newCall(method, CallOptions.DEFAULT),
                                                    req, observer);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    default void onRaftLeaderChanged() {
        synchronized (this){
            if (!isLeader()) {
                try {
                    String message = "lose leader";
                    PDPulseSubject.notifyError(message);
                    PDWatchSubject.notifyError(message);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }
}
