/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.pd.client;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.PDGrpc.PDBlockingStub;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.Pdpb.GetMembersRequest;
import org.apache.hugegraph.pd.grpc.Pdpb.GetMembersResponse;

import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractClient implements Closeable {

    private static final ConcurrentHashMap<String, ManagedChannel> chs = new ConcurrentHashMap<>();
    public static Pdpb.ResponseHeader okHeader = Pdpb.ResponseHeader.newBuilder().setError(
            Pdpb.Error.newBuilder().setType(Pdpb.ErrorType.OK)).build();
    protected final Pdpb.RequestHeader header;
    protected final AbstractClientStubProxy stubProxy;
    protected final PDConfig config;
    protected ManagedChannel channel = null;
    protected volatile ConcurrentMap<String, AbstractBlockingStub> stubs = null;

    protected AbstractClient(PDConfig config) {
        String[] hosts = config.getServerHost().split(",");
        this.stubProxy = new AbstractClientStubProxy(hosts);
        this.header = Pdpb.RequestHeader.getDefaultInstance();
        this.config = config;
    }

    public static Pdpb.ResponseHeader newErrorHeader(int errorCode, String errorMsg) {
        Pdpb.ResponseHeader header = Pdpb.ResponseHeader.newBuilder().setError(
                Pdpb.Error.newBuilder().setTypeValue(errorCode).setMessage(errorMsg)).build();
        return header;
    }

    protected static void handleErrors(Pdpb.ResponseHeader header) throws PDException {
        if (header.hasError() && header.getError().getType() != Pdpb.ErrorType.OK) {
            throw new PDException(header.getError().getTypeValue(),
                                  String.format("PD request error, error code = %d, msg = %s",
                                                header.getError().getTypeValue(),
                                                header.getError().getMessage()));
        }
    }

    protected AbstractBlockingStub getBlockingStub() throws PDException {
        if (stubProxy.getBlockingStub() == null) {
            synchronized (this) {
                if (stubProxy.getBlockingStub() == null) {
                    String host = resetStub();
                    if (host.isEmpty()) {
                        throw new PDException(Pdpb.ErrorType.PD_UNREACHABLE_VALUE,
                                              "PD unreachable, pd.peers=" +
                                              config.getServerHost());
                    }
                }
            }
        }
        return (AbstractBlockingStub) stubProxy.getBlockingStub()
                                               .withDeadlineAfter(config.getGrpcTimeOut(),
                                                                  TimeUnit.MILLISECONDS);
    }

    protected AbstractStub getStub() throws PDException {
        if (stubProxy.getStub() == null) {
            synchronized (this) {
                if (stubProxy.getStub() == null) {
                    String host = resetStub();
                    if (host.isEmpty()) {
                        throw new PDException(Pdpb.ErrorType.PD_UNREACHABLE_VALUE,
                                              "PD unreachable, pd.peers=" +
                                              config.getServerHost());
                    }
                }
            }
        }
        return stubProxy.getStub();
    }

    protected abstract AbstractStub createStub();

    protected abstract AbstractBlockingStub createBlockingStub();

    private String resetStub() {
        String leaderHost = "";
        for (int i = 0; i < stubProxy.getHostCount(); i++) {
            String host = stubProxy.nextHost();
            channel = ManagedChannelBuilder.forTarget(host).usePlaintext().build();
            PDBlockingStub blockingStub = PDGrpc.newBlockingStub(channel)
                                                .withDeadlineAfter(config.getGrpcTimeOut(),
                                                                   TimeUnit.MILLISECONDS);
            try {
                GetMembersRequest request = Pdpb.GetMembersRequest.newBuilder()
                                                                  .setHeader(header).build();
                GetMembersResponse members = blockingStub.getMembers(request);
                Metapb.Member leader = members.getLeader();
                leaderHost = leader.getGrpcUrl();
                close();
                channel = ManagedChannelBuilder.forTarget(leaderHost).usePlaintext().build();
                stubProxy.setBlockingStub(createBlockingStub());
                stubProxy.setStub(createStub());
                log.info("PDClient connect to host = {} success", leaderHost);
                break;
            } catch (Exception e) {
                log.error("PDClient connect to {} exception {}, {}", host, e.getMessage(),
                          e.getCause() != null ? e.getCause().getMessage() : "");
            }
        }
        return leaderHost;
    }

    protected <ReqT, RespT, StubT extends AbstractBlockingStub<StubT>> RespT blockingUnaryCall(
            MethodDescriptor<ReqT, RespT> method, ReqT req) throws PDException {
        return blockingUnaryCall(method, req, 5);
    }

    protected <ReqT, RespT, StubT extends AbstractBlockingStub<StubT>> RespT blockingUnaryCall(
            MethodDescriptor<ReqT, RespT> method, ReqT req, int retry) throws PDException {
        AbstractBlockingStub stub = getBlockingStub();
        try {
            RespT resp =
                    ClientCalls.blockingUnaryCall(stub.getChannel(), method, stub.getCallOptions(),
                                                  req);
            return resp;
        } catch (Exception e) {
            log.error(method.getFullMethodName() + " exception, {}", e.getMessage());
            if (e instanceof StatusRuntimeException) {
                if (retry < stubProxy.getHostCount()) {
                    synchronized (this) {
                        stubProxy.setBlockingStub(null);
                    }
                    return blockingUnaryCall(method, req, ++retry);
                }
            }
        }
        return null;
    }

    // this.stubs = new ConcurrentHashMap<String,AbstractBlockingStub>(hosts.length);
    private AbstractBlockingStub getConcurrentBlockingStub(String address) {
        AbstractBlockingStub stub = stubs.get(address);
        if (stub != null) {
            return stub;
        }
        Channel ch = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
        PDBlockingStub blockingStub =
                PDGrpc.newBlockingStub(ch).withDeadlineAfter(config.getGrpcTimeOut(),
                                                             TimeUnit.MILLISECONDS);
        stubs.put(address, blockingStub);
        return blockingStub;

    }

    protected <ReqT, RespT> KVPair<Boolean, RespT> concurrentBlockingUnaryCall(
            MethodDescriptor<ReqT, RespT> method, ReqT req, Predicate<RespT> predicate) {
        LinkedList<String> hostList = this.stubProxy.getHostList();
        if (this.stubs == null) {
            synchronized (this) {
                if (this.stubs == null) {
                    this.stubs = new ConcurrentHashMap<>(hostList.size());
                }
            }
        }
        Stream<RespT> respTStream = hostList.parallelStream().map((address) -> {
            AbstractBlockingStub stub = getConcurrentBlockingStub(address);
            RespT resp = ClientCalls.blockingUnaryCall(stub.getChannel(),
                                                       method, stub.getCallOptions(), req);
            return resp;
        });
        KVPair<Boolean, RespT> pair;
        AtomicReference<RespT> response = new AtomicReference<>();
        boolean result = respTStream.anyMatch((r) -> {
            response.set(r);
            return predicate.test(r);
        });
        if (result) {
            pair = new KVPair<>(true, null);
        } else {
            pair = new KVPair<>(false, response.get());
        }
        return pair;
    }

    protected <ReqT, RespT> void streamingCall(MethodDescriptor<ReqT, RespT> method, ReqT request,
                                               StreamObserver<RespT> responseObserver,
                                               int retry) throws PDException {
        AbstractStub stub = getStub();
        try {
            ClientCall<ReqT, RespT> call = stub.getChannel().newCall(method, stub.getCallOptions());
            ClientCalls.asyncServerStreamingCall(call, request, responseObserver);
        } catch (Exception e) {
            if (e instanceof StatusRuntimeException) {
                if (retry < stubProxy.getHostCount()) {
                    synchronized (this) {
                        stubProxy.setStub(null);
                    }
                    streamingCall(method, request, responseObserver, ++retry);
                    return;
                }
            }
            log.error("rpc call with exception, {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        closeChannel(channel);
        if (stubs != null) {
            for (AbstractBlockingStub stub : stubs.values()) {
                closeChannel((ManagedChannel) stub.getChannel());
            }
        }

    }

    private void closeChannel(ManagedChannel channel) {
        try {
            while (channel != null &&
                   !channel.shutdownNow().awaitTermination(100, TimeUnit.MILLISECONDS)) {
                continue;
            }
        } catch (Exception e) {
            log.info("Close channel with error : ", e);
        }
    }
}
