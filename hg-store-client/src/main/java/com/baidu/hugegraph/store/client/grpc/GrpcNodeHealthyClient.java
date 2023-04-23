package com.baidu.hugegraph.store.client.grpc;

import com.baidu.hugegraph.store.grpc.HealthyGrpc;
import com.baidu.hugegraph.store.grpc.HealthyOuterClass;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
@ThreadSafe
public final class GrpcNodeHealthyClient {

    private final static Map<String, ManagedChannel> CHANNEL_MAP = new ConcurrentHashMap<>();
    private final static Map<String, HealthyGrpc.HealthyBlockingStub> STUB_MAP = new ConcurrentHashMap<>();

    // TODO: Forbid constructing out of the package.
    public GrpcNodeHealthyClient() {

    }

    private ManagedChannel getChannel(String target) {
        ManagedChannel channel = CHANNEL_MAP.get(target);
        if (channel == null) {
            channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
            CHANNEL_MAP.put(target, channel);
        }
        return channel;
    }

    private HealthyGrpc.HealthyBlockingStub getStub(String target) {
        HealthyGrpc.HealthyBlockingStub stub = STUB_MAP.get(target);
        if (stub == null) {
            stub = HealthyGrpc.newBlockingStub(getChannel(target));
            STUB_MAP.put(target, stub);
        }
        return stub;
    }


/*    boolean isHealthy(GrpcStoreNodeImpl node) {
        String target = node.getAddress();

        HealthyOuterClass.StringReply response = getStub(target).isOk(Empty.newBuilder().build());
        String res = response.getMessage();

        if ("ok".equals(res)) {
            return true;
        } else {
            System.out.printf("gRPC-res-msg: %s%n", res);
            return false;
        }
    }*/

    public boolean isHealthy() {
        String target = "localhost:9080";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        HealthyGrpc.HealthyBlockingStub stub = HealthyGrpc.newBlockingStub(channel);
        HealthyOuterClass.StringReply response = stub.isOk(Empty.newBuilder().build());

        String res = response.getMessage();
        System.out.printf("gRPC response message:%s%n", res);

        if ("ok".equals(res)) {
            return true;
        } else {

            return false;
        }
    }
}
