package org.apache.hugegraph.pd.client.interceptor;

import org.apache.commons.lang3.StringUtils;

import org.apache.hugegraph.pd.client.rpc.ConnectionManagers;
import org.apache.hugegraph.pd.common.Cache;
import org.apache.hugegraph.pd.common.Consts;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zhangyingjie
 * @date 2023/4/25
 **/
@Slf4j
public class Authentication implements ClientInterceptor {

    private static Cache<String> cache = new Cache();
    private static long ttl = 3600L;
    private String authority;
    private String name;

    public Authentication(String userName, String authority) {
        assert !StringUtils.isEmpty(userName);
        this.name = userName;
        this.authority = authority;
    }

    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions, Channel next) {
        return new SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            public void sendMessage(ReqT message) {
                super.sendMessage(message);
            }

            public void start(Listener<RespT> listener, Metadata headers) {
                if (StringUtils.isEmpty(authority) ||
                    StringUtils.isEmpty(name)) {
                    throw new RuntimeException("invalid user name or password,access denied");
                }
                headers.put(Consts.CREDENTIAL_KEY, authority);
                String token = cache.get(name);
                if (token != null) {
                    headers.put(Consts.TOKEN_KEY, cache.get(name));
                }
                SimpleForwardingClientCallListener<RespT> callListener =
                        new SimpleForwardingClientCallListener<RespT>(listener) {
                            public void onHeaders(Metadata headers) {
                                super.onHeaders(headers);
                                String t = headers.get(Consts.TOKEN_KEY);
                                if (!StringUtils.isEmpty(t)) {
                                    cache.put(name, t, ttl);
                                }
                                String leader = headers.get(Consts.LEADER_KEY);
                                if (!StringUtils.isEmpty(leader)) {
                                    ConnectionManagers.getInstance().reset(leader);
                                }
                            }
                        };
                super.start(callListener, headers);
            }
        };
    }
}
