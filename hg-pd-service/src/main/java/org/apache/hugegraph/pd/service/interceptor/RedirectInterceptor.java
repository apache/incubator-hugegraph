package org.apache.hugegraph.pd.service.interceptor;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Base64;

import org.apache.hugegraph.pd.common.Consts;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

/**
 * @author zhangyingjie
 * @date 2024/1/17
 **/
public class RedirectInterceptor implements ClientInterceptor {

    private static String auth = "store:$2a$04$9ZGBULe2vc73DMj7r/iBKeQB1SagtUXPrDbMmNswRkTwlWQURE/Jy";
    private static String authority = new String(Base64.getEncoder().encode(auth.getBytes(UTF_8)));

    public RedirectInterceptor() {
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions, Channel next) {

        return new SimpleForwardingClientCall<>(
                next.newCall(method, callOptions)) {

            @Override
            public void sendMessage(ReqT message) {
                super.sendMessage(message);
            }

            @Override
            public void start(ClientCall.Listener<RespT> listener, Metadata headers) {
                headers.put(Consts.CREDENTIAL_KEY, authority);
                SimpleForwardingClientCallListener<RespT> callListener =
                        new SimpleForwardingClientCallListener<>(listener) {
                            @Override
                            public void onMessage(RespT message) {
                                super.onMessage(message);
                            }

                            @Override
                            public void onHeaders(Metadata headers) {
                                super.onHeaders(headers);
                            }

                            @Override
                            public void onClose(Status status, Metadata trailers) {
                                super.onClose(status, trailers);
                            }
                        };
                super.start(callListener, headers);
            }
        };
    }
}
