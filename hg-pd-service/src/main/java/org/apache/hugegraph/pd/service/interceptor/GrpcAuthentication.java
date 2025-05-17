package org.apache.hugegraph.pd.service.interceptor;

import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.service.ServiceGrpc;
import org.springframework.stereotype.Service;

import org.apache.hugegraph.pd.common.Consts;
import org.apache.hugegraph.pd.raft.RaftEngine;

import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * @author zhangyingjie
 * @date 2023/4/23
 **/
@Service
public class GrpcAuthentication extends Authentication implements ServerInterceptor, ServiceGrpc {

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {
        try {
            String authority = headers.get(Consts.CREDENTIAL_KEY);
            String token = headers.get(Consts.TOKEN_KEY);
            Function<String, Listener<ReqT>> tokenCall = t -> {
                ServerCall<ReqT, RespT> sc = new SimpleForwardingServerCall(call) {
                    @Override
                    public void sendHeaders(Metadata headers) {
                        if (!StringUtils.isEmpty(t)) {
                            headers.put(Consts.TOKEN_KEY, t);
                        }
                        if (!isLeader()) {
                            String grpcAddress = null;
                            try {
                                grpcAddress = RaftEngine.getInstance().getLeaderGrpcAddress(true);
                            } catch (Exception e) {
                            }
                            if (!StringUtils.isEmpty(grpcAddress)) {
                                headers.put(Consts.LEADER_KEY, grpcAddress);
                            }
                        }
                        super.sendHeaders(headers);
                    }
                };
                return next.startCall(sc, headers);
            };
            return authenticate(authority, token, tokenCall);
        } catch (Exception e) {
            call.close(Status.UNAUTHENTICATED.withDescription(e.getMessage()), headers);
            return next.startCall(call, headers);
        }
    }
}
