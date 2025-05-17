<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
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

import static org.apache.hugegraph.pd.watch.NodeEvent.EventType.NODE_PD_LEADER_CHANGE;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Metapb.ShardGroup;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.Pdpb.CachePartitionResponse;
import org.apache.hugegraph.pd.grpc.Pdpb.CacheResponse;
import org.apache.hugegraph.pd.grpc.Pdpb.GetGraphRequest;
import org.apache.hugegraph.pd.grpc.Pdpb.GetPartitionByCodeRequest;
import org.apache.hugegraph.pd.grpc.Pdpb.GetPartitionRequest;
import org.apache.hugegraph.pd.grpc.Pdpb.GetPartitionResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchResponse;
import org.apache.hugegraph.pd.watch.NodeEvent;
import org.apache.hugegraph.pd.watch.PartitionEvent;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractBlockingStub;
import lombok.extern.slf4j.Slf4j;

/**
 * PD client implementation class
 */
@Slf4j
public class PDClient {

    private final PDConfig config;
    private final Pdpb.RequestHeader header;
    private final ClientCache cache;
    private final StubProxy stubProxy;
    private final List<PDEventListener> eventListeners;
    private PDWatch.Watcher partitionWatcher;
    private PDWatch.Watcher storeWatcher;
    private PDWatch.Watcher graphWatcher;
    private PDWatch.Watcher shardGroupWatcher;
    private PDWatch pdWatch;

    private PDClient(PDConfig config) {
        this.config = config;
        this.header = Pdpb.RequestHeader.getDefaultInstance();
        this.stubProxy = new StubProxy(config.getServerHost().split(","));
        this.eventListeners = new CopyOnWriteArrayList<>();
        this.cache = new ClientCache(this);
    }

    /**
     * Create a PD client object and initialize the stub
     *
     * @param config
     * @return
     */
    public static PDClient create(PDConfig config) {
        return new PDClient(config);
    }

    private synchronized void newBlockingStub() throws PDException {
        if (stubProxy.get() != null) {
            return;
        }

        String host = newLeaderStub();
        if (host.isEmpty()) {
            throw new PDException(Pdpb.ErrorType.PD_UNREACHABLE_VALUE,
                                  "PD unreachable, pd.peers=" + config.getServerHost());
        }

        log.info("PDClient enable cache, init PDWatch object");
        connectPdWatch(host);
    }

    public void connectPdWatch(String leader) {

        if (pdWatch != null && Objects.equals(pdWatch.getCurrentHost(), leader) &&
            pdWatch.checkChannel()) {
            return;
        }

        log.info("PDWatch client connect host:{}", leader);
        pdWatch = new PDWatchImpl(leader);

        partitionWatcher = pdWatch.watchPartition(new PDWatch.Listener<>() {
            @Override
            public void onNext(PartitionEvent response) {
                // log.info("PDClient receive partition event {}-{} {}",
                //        response.getGraph(), response.getPartitionId(), response.getChangeType());
                invalidPartitionCache(response.getGraph(), response.getPartitionId());

                if (response.getChangeType() == PartitionEvent.ChangeType.DEL) {
                    cache.removeAll(response.getGraph());
                }

                eventListeners.forEach(listener -> {
                    listener.onPartitionChanged(response);
                });
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("watchPartition exception {}", throwable.getMessage());
                closeStub(false);
            }
        });

        storeWatcher = pdWatch.watchNode(new PDWatch.Listener<>() {
            @Override
            public void onNext(NodeEvent response) {
                log.info("PDClient receive store event {} {}",
                         response.getEventType(), Long.toHexString(response.getNodeId()));

                if (response.getEventType() == NODE_PD_LEADER_CHANGE) {
                    // pd raft change
                    var leaderIp = response.getGraph();
                    log.info("watchNode: pd leader changed to {}, current watch:{}",
                             leaderIp, pdWatch.getCurrentHost());
                    closeStub(!Objects.equals(pdWatch.getCurrentHost(), leaderIp));
                    connectPdWatch(leaderIp);
                }

                invalidStoreCache(response.getNodeId());
                eventListeners.forEach(listener -> {
                    listener.onStoreChanged(response);
                });
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("watchNode exception {}", throwable.getMessage());
                closeStub(false);
            }

        });

        graphWatcher = pdWatch.watchGraph(new PDWatch.Listener<>() {
            @Override
            public void onNext(WatchResponse response) {
                eventListeners.forEach(listener -> {
                    listener.onGraphChanged(response);
                });
            }

            @Override
            public void onError(Throwable throwable) {
                log.warn("graphWatcher exception {}", throwable.getMessage());
            }
        });

        shardGroupWatcher = pdWatch.watchShardGroup(new PDWatch.Listener<>() {
            @Override
            public void onNext(WatchResponse response) {
                var shardResponse = response.getShardGroupResponse();
                // log.info("PDClient receive shard group event: raft {}-{}", shardResponse
                // .getShardGroupId(),
                //        shardResponse.getType());
                if (config.isEnableCache()) {
                    switch (shardResponse.getType()) {
                        case WATCH_CHANGE_TYPE_DEL:
                            cache.deleteShardGroup(shardResponse.getShardGroupId());
                            break;
                        case WATCH_CHANGE_TYPE_ALTER:
                            cache.updateShardGroup(
                                    response.getShardGroupResponse().getShardGroup());
                            break;
                        default:
                            break;
                    }
                }
                eventListeners.forEach(listener -> listener.onShardGroupChanged(response));
            }

            @Override
            public void onError(Throwable throwable) {
                log.warn("shardGroupWatcher exception {}", throwable.getMessage());
            }
        });

    }

    private synchronized void closeStub(boolean closeWatcher) {
        stubProxy.set(null);
        cache.reset();

        if (closeWatcher) {
            if (partitionWatcher != null) {
                partitionWatcher.close();
                partitionWatcher = null;
            }
            if (storeWatcher != null) {
                storeWatcher.close();
                storeWatcher = null;
            }
            if (graphWatcher != null) {
                graphWatcher.close();
                graphWatcher = null;
            }

            if (shardGroupWatcher != null) {
                shardGroupWatcher.close();
                shardGroupWatcher = null;
            }

            pdWatch = null;
        }
    }

    private PDGrpc.PDBlockingStub getStub() throws PDException {
        if (stubProxy.get() == null) {
            newBlockingStub();
        }
        return stubProxy.get().withDeadlineAfter(config.getGrpcTimeOut(), TimeUnit.MILLISECONDS);
    }

    private PDGrpc.PDBlockingStub newStub() throws PDException {
        if (stubProxy.get() == null) {
            newBlockingStub();
        }
        return PDGrpc.newBlockingStub(stubProxy.get().getChannel())
                     .withDeadlineAfter(config.getGrpcTimeOut(),
                                        TimeUnit.MILLISECONDS);
    }

    private String newLeaderStub() {
        String leaderHost = "";
        for (int i = 0; i < stubProxy.getHostCount(); i++) {
            String host = stubProxy.nextHost();
            ManagedChannel channel = Channels.getChannel(host);

            PDGrpc.PDBlockingStub stub = PDGrpc.newBlockingStub(channel)
                                               .withDeadlineAfter(config.getGrpcTimeOut(),
                                                                  TimeUnit.MILLISECONDS);
            try {
                var leaderIp = getLeaderIp(stub);
                if (!leaderIp.equalsIgnoreCase(host)) {
                    leaderHost = leaderIp;
                    stubProxy.set(PDGrpc.newBlockingStub(channel)
                                        .withDeadlineAfter(config.getGrpcTimeOut(),
                                                           TimeUnit.MILLISECONDS));
                } else {
                    stubProxy.set(stub);
                    leaderHost = host;
                }
                stubProxy.setLeader(leaderIp);

                log.info("PDClient connect to host = {} success", leaderHost);
                break;
            } catch (Exception e) {
                log.error("PDClient connect to {} exception {}, {}", host, e.getMessage(),
                          e.getCause() != null ? e.getCause().getMessage() : "");
            }
        }
        return leaderHost;
    }

    public String getLeaderIp() {

        return getLeaderIp(stubProxy.get());
    }

    private String getLeaderIp(PDGrpc.PDBlockingStub stub) {
        if (stub == null) {
            try {
                getStub();
                return stubProxy.getLeader();
            } catch (PDException e) {
                throw new RuntimeException(e);
            }
        }

        Pdpb.GetMembersRequest request = Pdpb.GetMembersRequest.newBuilder()
                                                               .setHeader(header)
                                                               .build();
        Metapb.Member leader = stub.getMembers(request).getLeader();
        return leader.getGrpcUrl();
    }

    /**
     * Store registration, the store ID will be returned, and the initial registration will
     * return a new ID
========
package org.apache.hugegraph.pd.client.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.pd.client.ClientCache;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.client.rpc.ConnectionManager;
import org.apache.hugegraph.pd.client.rpc.Invoker;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.grpc.ClusterOp;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.StoreGroup;
import org.apache.hugegraph.pd.grpc.common.ErrorType;
import org.apache.hugegraph.pd.grpc.common.RequestHeader;
import org.apache.hugegraph.pd.grpc.common.ResponseHeader;
import com.google.protobuf.ByteString;

import io.grpc.MethodDescriptor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com on 2023/12/8
 */
@Slf4j
public class PDApi {
    private final PDConfig config;
    private final ConnectionManager cm;
    private final ClientCache cache;
    private final RequestHeader header = RequestHeader.getDefaultInstance();
    private final Invoker invoker;
    private PDClient client;

    public PDApi(PDClient client, ClientCache cache) {
        this.client = client;
        this.config = client.getConfig();
        this.cm = client.getCm();
        this.cache = cache;
        this.invoker = client.getLeaderInvoker();
    }

    private <ReqT, RespT> RespT blockingUnaryCall(
            MethodDescriptor<ReqT, RespT> method, ReqT req) throws PDException {
        return invoker.blockingCall(method, req);
    }

    private void handleResponseError(ResponseHeader header) throws PDException {
        var errorType = header.getError().getType();
        if (header.hasError() && errorType != ErrorType.OK) {
            throw new PDException(header.getError().getTypeValue(),
                    String.format("PD request error, error code = %d, msg = %s",
                            header.getError().getTypeValue(),
                            header.getError().getMessage()));
        }
    }

    /**
     * Store注册，返回storeID，初次注册会返回新ID
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
     *
     * @param store
     * @return
     */
    public long registerStore(Metapb.Store store) throws PDException {
        Pdpb.RegisterStoreRequest request = Pdpb.RegisterStoreRequest.newBuilder()
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
                                                                     .setHeader(header)
                                                                     .setStore(store).build();

========
                .setHeader(header)
                .setStore(store).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        Pdpb.RegisterStoreResponse response =
                blockingUnaryCall(PDGrpc.getRegisterStoreMethod(), request);
        handleResponseError(response.getHeader());
        return response.getStoreId();
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
    /**
     * Returns the Store object based on the store ID
     *
     * @param storeId
     * @return
     * @throws PDException
     */
    public Metapb.Store getStore(long storeId) throws PDException {
        Metapb.Store store = cache.getStoreById(storeId);
        if (store == null) {
            Pdpb.GetStoreRequest request = Pdpb.GetStoreRequest.newBuilder()
                                                               .setHeader(header)
                                                               .setStoreId(storeId).build();
            Pdpb.GetStoreResponse response = getStub().getStore(request);
            handleResponseError(response.getHeader());
            store = response.getStore();
            if (config.isEnableCache()) {
                cache.addStore(storeId, store);
            }
        }
        return store;
    }

    /**
     * Update the store information, including online and offline
     *
     * @param store
     * @return
     */
    public Metapb.Store updateStore(Metapb.Store store) throws PDException {
        Pdpb.SetStoreRequest request = Pdpb.SetStoreRequest.newBuilder()
                                                           .setHeader(header)
                                                           .setStore(store).build();

        Pdpb.SetStoreResponse response = getStub().setStore(request);
        handleResponseError(response.getHeader());
        store = response.getStore();
        if (config.isEnableCache()) {
            cache.addStore(store.getId(), store);
        }
        return store;
    }

    /**
     * Return to the active store
     *
     * @param graphName
     * @return
     */
    public List<Metapb.Store> getActiveStores(String graphName) throws PDException {
        List<Metapb.Store> stores = new ArrayList<>();
        KVPair<Metapb.Partition, Metapb.Shard> ptShard = this.getPartitionByCode(graphName, 0);
        while (ptShard != null) {
            stores.add(this.getStore(ptShard.getValue().getStoreId()));
            if (ptShard.getKey().getEndKey() < PartitionUtils.MAX_VALUE) {
                ptShard = this.getPartitionByCode(graphName, ptShard.getKey().getEndKey());
            } else {
                ptShard = null;
            }
        }
        return stores;
    }

    public List<Metapb.Store> getActiveStores() throws PDException {
        Pdpb.GetAllStoresRequest request = Pdpb.GetAllStoresRequest.newBuilder()
                                                                   .setHeader(header)
                                                                   .setGraphName("")
                                                                   .setExcludeOfflineStores(true)
                                                                   .build();
        Pdpb.GetAllStoresResponse response = getStub().getAllStores(request);
        handleResponseError(response.getHeader());
        return response.getStoresList();

    }

    /**
     * Return to the active store
     *
     * @param graphName
     * @return
     */
    public List<Metapb.Store> getAllStores(String graphName) throws PDException {
        Pdpb.GetAllStoresRequest request = Pdpb.GetAllStoresRequest.newBuilder()
                                                                   .setHeader(header)
                                                                   .setGraphName(graphName)
                                                                   .setExcludeOfflineStores(false)
                                                                   .build();
        Pdpb.GetAllStoresResponse response = getStub().getAllStores(request);
        handleResponseError(response.getHeader());
        return response.getStoresList();

    }

    /**
     * Store heartbeat, call regularly, stay online
     *
     * @param stats
     * @throws PDException
     */
    public Metapb.ClusterStats storeHeartbeat(Metapb.StoreStats stats) throws PDException {
        Pdpb.StoreHeartbeatRequest request = Pdpb.StoreHeartbeatRequest.newBuilder()
                                                                       .setHeader(header)
                                                                       .setStats(stats).build();
        Pdpb.StoreHeartbeatResponse response = getStub().storeHeartbeat(request);
        handleResponseError(response.getHeader());
        return response.getClusterStats();
    }

    private KVPair<Metapb.Partition, Metapb.Shard> getKvPair(String graphName, byte[] key,
                                                             KVPair<Metapb.Partition,
                                                                     Metapb.Shard> partShard) throws
                                                                                              PDException {
        if (partShard == null) {
            GetPartitionRequest request = GetPartitionRequest.newBuilder()
                                                             .setHeader(header)
                                                             .setGraphName(graphName)
                                                             .setKey(ByteString.copyFrom(key))
                                                             .build();
            GetPartitionResponse response =
========
    public KVPair<Metapb.Partition, Metapb.Shard> getKvPair(String graphName, byte[] key, KVPair<Metapb.Partition,
            Metapb.Shard> partShard) throws PDException {
        if (partShard == null) {
            Pdpb.GetPartitionRequest request = Pdpb.GetPartitionRequest.newBuilder()
                    .setHeader(header)
                    .setGraphName(graphName)
                    .setKey(ByteString.copyFrom(key))
                    .build();
            Pdpb.GetPartitionResponse response =
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
                    blockingUnaryCall(PDGrpc.getGetPartitionMethod(), request);
            handleResponseError(response.getHeader());
            partShard = new KVPair<>(response.getPartition(), response.getLeader());
            cache.update(graphName, partShard.getKey().getId(), partShard.getKey());
        }
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
        return partShard;
    }

    /**
     * Query the partition to which the key belongs
     *
     * @param graphName
     * @param key
     * @return
     * @throws PDException
     */
    public KVPair<Metapb.Partition, Metapb.Shard> getPartition(String graphName, byte[] key) throws
                                                                                             PDException {

        KVPair<Metapb.Partition, Metapb.Shard> partShard =
                this.getPartitionByCode(graphName, PartitionUtils.calcHashcode(key));
        partShard = getKvPair(graphName, key, partShard);
        return partShard;
    }

    public KVPair<Metapb.Partition, Metapb.Shard> getPartition(String graphName, byte[] key,
                                                               int code) throws
                                                                         PDException {
        KVPair<Metapb.Partition, Metapb.Shard> partShard =
                cache.getPartitionByCode(graphName, code);
        partShard = getKvPair(graphName, key, partShard);
========

>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        return partShard;
    }

    /**
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
     * Query the partition information based on the hashcode
========
     * 根据hashcode查询所属分区信息
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
     *
     * @param graphName
     * @param hashCode
     * @return
     * @throws PDException
     */
    public KVPair<Metapb.Partition, Metapb.Shard> getPartitionByCode(String graphName,
                                                                     long hashCode)
            throws PDException {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
        KVPair<Metapb.Partition, Metapb.Shard> partShard =
                cache.getPartitionByCode(graphName, hashCode);
        if (partShard == null) {
            GetPartitionByCodeRequest request = GetPartitionByCodeRequest.newBuilder()
                                                                         .setHeader(header)
                                                                         .setGraphName(graphName)
                                                                         .setCode(hashCode).build();
            GetPartitionResponse response =
========
        //  先查cache，cache没有命中，在调用PD
        KVPair<Metapb.Partition, Metapb.Shard> partShard = cache.getPartitionByCode(graphName, hashCode);
        if (partShard == null) {
            Pdpb.GetPartitionByCodeRequest request = Pdpb.GetPartitionByCodeRequest.newBuilder()
                    .setHeader(header)
                    .setGraphName(graphName)
                    .setCode(hashCode).build();
            Pdpb.GetPartitionResponse response =
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
                    blockingUnaryCall(PDGrpc.getGetPartitionByCodeMethod(), request);
            handleResponseError(response.getHeader());
            partShard = new KVPair<>(response.getPartition(), response.getLeader());
            cache.update(graphName, partShard.getKey().getId(), partShard.getKey());
            cache.updateShardGroup(getShardGroup(partShard.getKey().getId()));
        }

        if (partShard.getValue() == null) {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
            ShardGroup shardGroup = getShardGroup(partShard.getKey().getId());
========
            Metapb.ShardGroup shardGroup = getShardGroup(partShard.getKey().getId());
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
            if (shardGroup != null) {
                for (var shard : shardGroup.getShardsList()) {
                    if (shard.getRole() == Metapb.ShardRole.Leader) {
                        partShard.setValue(shard);
                    }
                }
            } else {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
                log.error("getPartitionByCode: get shard group failed, {}",
                          partShard.getKey().getId());
========
                log.error("getPartitionByCode: get shard group failed, {}", partShard.getKey().getId());
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
            }
        }
        return partShard;
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
    /**
     * Obtain the hash value of the key
     */
    public int keyToCode(String graphName, byte[] key) {
        return PartitionUtils.calcHashcode(key);
    }

    /**
     * Returns partition information based on the partition ID and RPC request
========
    /**
     * 根据分区id返回分区信息, RPC请求
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
     *
     * @param graphName
     * @param partId
     * @return
     * @throws PDException
     */
    public KVPair<Metapb.Partition, Metapb.Shard> getPartitionById(String graphName,
                                                                   int partId) throws PDException {
        KVPair<Metapb.Partition, Metapb.Shard> partShard =
                cache.getPartitionById(graphName, partId);
        if (partShard == null) {
            Pdpb.GetPartitionByIDRequest request = Pdpb.GetPartitionByIDRequest.newBuilder()
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
                                                                               .setHeader(header)
                                                                               .setGraphName(
                                                                                       graphName)
                                                                               .setPartitionId(
                                                                                       partId)
                                                                               .build();
            GetPartitionResponse response =
========
                    .setHeader(header)
                    .setGraphName(graphName)
                    .setPartitionId(partId)
                    .build();
            Pdpb.GetPartitionResponse response =
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
                    blockingUnaryCall(PDGrpc.getGetPartitionByIDMethod(), request);
            handleResponseError(response.getHeader());
            partShard = new KVPair<>(response.getPartition(), response.getLeader());
            if (config.isEnableCache()) {
                cache.update(graphName, partShard.getKey().getId(), partShard.getKey());
                cache.updateShardGroup(getShardGroup(partShard.getKey().getId()));
            }
        }
        if (partShard.getValue() == null) {
            var shardGroup = getShardGroup(partShard.getKey().getId());
            if (shardGroup != null) {
                for (var shard : shardGroup.getShardsList()) {
                    if (shard.getRole() == Metapb.ShardRole.Leader) {
                        partShard.setValue(shard);
                    }
                }
            } else {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
                log.error("getPartitionById: get shard group failed, {}",
                          partShard.getKey().getId());
========
                log.error("getPartitionById: get shard group failed, {}", partShard.getKey().getId());
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
            }
        }
        return partShard;
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
    public ShardGroup getShardGroup(int partId) throws PDException {
        ShardGroup group = cache.getShardGroup(partId);
        if (group == null) {
            Pdpb.GetShardGroupRequest request = Pdpb.GetShardGroupRequest.newBuilder()
                                                                         .setHeader(header)
                                                                         .setGroupId(partId)
                                                                         .build();
            Pdpb.GetShardGroupResponse response =
                    blockingUnaryCall(PDGrpc.getGetShardGroupMethod(), request);
            handleResponseError(response.getHeader());
            group = response.getShardGroup();
            if (config.isEnableCache()) {
                cache.updateShardGroup(group);
            }
        }
        return group;
========

    public Metapb.ShardGroup getShardGroupDirect(int partId) throws PDException {
        Pdpb.GetShardGroupRequest request = Pdpb.GetShardGroupRequest.newBuilder()
                .setHeader(header)
                .setGroupId(partId)
                .build();
        Pdpb.GetShardGroupResponse response = blockingUnaryCall(PDGrpc.getGetShardGroupMethod(), request);
        handleResponseError(response.getHeader());
        return response.getShardGroup();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
    }

    public void updateShardGroup(ShardGroup shardGroup) throws PDException {
        Pdpb.UpdateShardGroupRequest request = Pdpb.UpdateShardGroupRequest.newBuilder()
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
                                                                           .setHeader(header)
                                                                           .setShardGroup(
                                                                                   shardGroup)
                                                                           .build();
========
                .setHeader(header)
                .setShardGroup(shardGroup)
                .build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        Pdpb.UpdateShardGroupResponse response =
                blockingUnaryCall(PDGrpc.getUpdateShardGroupMethod(), request);
        handleResponseError(response.getHeader());

        if (config.isEnableCache()) {
            cache.updateShardGroup(shardGroup);
        }
    }

    /**
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
     * Returns information about all partitions spanned by the start and end keys
     *
     * @param graphName
     * @param startKey
     * @param endKey
     * @return
     * @throws PDException
     */
    public List<KVPair<Metapb.Partition, Metapb.Shard>> scanPartitions(String graphName,
                                                                       byte[] startKey,
                                                                       byte[] endKey) throws
                                                                                      PDException {
        List<KVPair<Metapb.Partition, Metapb.Shard>> partitions = new ArrayList<>();
        KVPair<Metapb.Partition, Metapb.Shard> startPartShard = getPartition(graphName, startKey);
        KVPair<Metapb.Partition, Metapb.Shard> endPartShard = getPartition(graphName, endKey);
        if (startPartShard == null || endPartShard == null) {
            return null;
        }

        partitions.add(startPartShard);
        while (startPartShard.getKey().getEndKey() < endPartShard.getKey().getEndKey()
               && startPartShard.getKey().getEndKey() < PartitionUtils.MAX_VALUE) {
            startPartShard = getPartitionByCode(graphName, startPartShard.getKey().getEndKey());
            partitions.add(startPartShard);
        }
        return partitions;
    }

    /**
     * Query partition information based on conditions
========
     * 根据条件查询分区信息
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
     *
     * @return
     * @throws PDException
     */
    public List<Metapb.Partition> getPartitionsByStore(long storeId) throws PDException {

        Metapb.PartitionQuery query = Metapb.PartitionQuery.newBuilder()
                                                           .setStoreId(storeId)
                                                           .build();
        Pdpb.QueryPartitionsRequest request = Pdpb.QueryPartitionsRequest.newBuilder()
                                                                         .setQuery(query).build();
        Pdpb.QueryPartitionsResponse response =
                blockingUnaryCall(PDGrpc.getQueryPartitionsMethod(), request);

        handleResponseError(response.getHeader());
        return response.getPartitionsList();
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
    public List<Metapb.Partition> queryPartitions(long storeId, int partitionId) throws
                                                                                 PDException {
========
    /**
     * 查找指定store上的指定partitionId
     *
     * @return
     * @throws PDException
     */
    public List<Metapb.Partition> queryPartitions(long storeId, int partitionId) throws PDException {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java

        Metapb.PartitionQuery query = Metapb.PartitionQuery.newBuilder()
                                                           .setStoreId(storeId)
                                                           .setPartitionId(partitionId)
                                                           .build();
        Pdpb.QueryPartitionsRequest request = Pdpb.QueryPartitionsRequest.newBuilder()
                                                                         .setQuery(query).build();
        Pdpb.QueryPartitionsResponse response =
                blockingUnaryCall(PDGrpc.getQueryPartitionsMethod(), request);

        handleResponseError(response.getHeader());
        return response.getPartitionsList();
    }

    public List<Metapb.Partition> getPartitions(long storeId, String graphName) throws PDException {

        Metapb.PartitionQuery query = Metapb.PartitionQuery.newBuilder()
                .setStoreId(storeId)
                .setGraphName(graphName).build();
        Pdpb.QueryPartitionsRequest request = Pdpb.QueryPartitionsRequest.newBuilder()
                .setQuery(query).build();
        Pdpb.QueryPartitionsResponse response =
                blockingUnaryCall(PDGrpc.getQueryPartitionsMethod(), request);

        handleResponseError(response.getHeader());
        return response.getPartitionsList();

    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
    public Metapb.Graph setGraph(Metapb.Graph graph) throws PDException {
        Pdpb.SetGraphRequest request = Pdpb.SetGraphRequest.newBuilder()
                                                           .setGraph(graph)
                                                           .build();
        Pdpb.SetGraphResponse response =
========
    public Metapb.Graph createGraph(Metapb.Graph graph) throws PDException {
        Pdpb.CreateGraphRequest request = Pdpb.CreateGraphRequest.newBuilder()
                .setGraph(graph)
                .build();
        Pdpb.CreateGraphResponse response = blockingUnaryCall(PDGrpc.getCreateGraphMethod(), request);
        handleResponseError(response.getHeader());
        return response.getGraph();
    }

    public Metapb.Graph setGraph(Metapb.Graph graph) throws PDException {
        Pdpb.CreateGraphRequest request = Pdpb.CreateGraphRequest.newBuilder()
                .setGraph(graph)
                .build();
        Pdpb.CreateGraphResponse response =
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
                blockingUnaryCall(PDGrpc.getSetGraphMethod(), request);

        handleResponseError(response.getHeader());
        return response.getGraph();
    }

    public Metapb.Graph getGraph(String graphName) throws PDException {
        GetGraphRequest request = GetGraphRequest.newBuilder()
                                                 .setGraphName(graphName)
                                                 .build();
        Pdpb.GetGraphResponse response =
                blockingUnaryCall(PDGrpc.getGetGraphMethod(), request);

        handleResponseError(response.getHeader());
        return response.getGraph();
    }

    public Metapb.Graph getGraphWithOutException(String graphName) throws
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
                                                                   PDException {
        GetGraphRequest request = GetGraphRequest.newBuilder()
                                                 .setGraphName(
                                                         graphName)
                                                 .build();
========
            PDException {
        Pdpb.GetGraphRequest request = Pdpb.GetGraphRequest.newBuilder()
                .setGraphName(
                        graphName)
                .build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        Pdpb.GetGraphResponse response = blockingUnaryCall(
                PDGrpc.getGetGraphMethod(), request);
        return response.getGraph();
    }

    public Metapb.Graph delGraph(String graphName) throws PDException {
        Pdpb.DelGraphRequest request = Pdpb.DelGraphRequest.newBuilder()
                                                           .setGraphName(graphName)
                                                           .build();
        Pdpb.DelGraphResponse response =
                blockingUnaryCall(PDGrpc.getDelGraphMethod(), request);

        handleResponseError(response.getHeader());
        return response.getGraph();
    }

    public List<Metapb.Partition> updatePartition(List<Metapb.Partition> partitions) throws
                                                                                     PDException {

        Pdpb.UpdatePartitionRequest request = Pdpb.UpdatePartitionRequest.newBuilder()
                                                                         .addAllPartition(
                                                                                 partitions)
                                                                         .build();
        Pdpb.UpdatePartitionResponse response =
                blockingUnaryCall(PDGrpc.getUpdatePartitionMethod(), request);
        handleResponseError(response.getHeader());
        invalidPartitionCache();

        return response.getPartitionList();
    }

    public Metapb.Partition delPartition(String graphName, int partitionId) throws PDException {

        Pdpb.DelPartitionRequest request = Pdpb.DelPartitionRequest.newBuilder()
                                                                   .setGraphName(graphName)
                                                                   .setPartitionId(partitionId)
                                                                   .build();
        Pdpb.DelPartitionResponse response =
                blockingUnaryCall(PDGrpc.getDelPartitionMethod(), request);

        handleResponseError(response.getHeader());
        invalidPartitionCache(graphName, partitionId);
        return response.getPartition();
    }
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java

    /**
     * Delete the partitioned cache
     */
    public void invalidPartitionCache(String graphName, int partitionId) {
        if (null != cache.getPartitionById(graphName, partitionId)) {
            cache.removePartition(graphName, partitionId);
        }
    }

    /**
     * Delete the partitioned cache
     */
    public void invalidPartitionCache() {
        cache.removePartitions();
    }

    /**
     * Delete the partitioned cache
     */
    public void invalidStoreCache(long storeId) {
        cache.removeStore(storeId);
    }

    /**
     * Update the cache
     */
    public void updatePartitionLeader(String graphName, int partId, long leaderStoreId) {
        KVPair<Metapb.Partition, Metapb.Shard> partShard = null;
        try {
            partShard = this.getPartitionById(graphName, partId);

            if (partShard != null && partShard.getValue().getStoreId() != leaderStoreId) {
                var shardGroup = this.getShardGroup(partId);
                Metapb.Shard shard = null;
                List<Metapb.Shard> shards = new ArrayList<>();

                for (Metapb.Shard s : shardGroup.getShardsList()) {
                    if (s.getStoreId() == leaderStoreId) {
                        shard = s;
                        shards.add(Metapb.Shard.newBuilder(s)
                                               .setStoreId(s.getStoreId())
                                               .setRole(Metapb.ShardRole.Leader).build());
                    } else {
                        shards.add(Metapb.Shard.newBuilder(s)
                                               .setStoreId(s.getStoreId())
                                               .setRole(Metapb.ShardRole.Follower).build());
                    }
                }

                if (config.isEnableCache()) {
                    if (shard == null) {
                        cache.removePartition(graphName, partId);
                    }
                }
            }
        } catch (PDException e) {
            log.error("getPartitionException: {}", e.getMessage());
        }
    }

    /**
     * Update the cache
     *
     * @param partition
     */
    public void updatePartitionCache(Metapb.Partition partition, Metapb.Shard leader) {
        if (config.isEnableCache()) {
            cache.update(partition.getGraphName(), partition.getId(), partition);
            cache.updateLeader(partition.getId(), leader);
        }
    }
========
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java

    public Pdpb.GetIdResponse getIdByKey(String key, int delta) throws PDException {
        Pdpb.GetIdRequest request = Pdpb.GetIdRequest.newBuilder()
                                                     .setHeader(header)
                                                     .setKey(key)
                                                     .setDelta(delta)
                                                     .build();
        Pdpb.GetIdResponse response = blockingUnaryCall(PDGrpc.getGetIdMethod(), request);
        handleResponseError(response.getHeader());
        return response;
    }

    public Pdpb.ResetIdResponse resetIdByKey(String key) throws PDException {
        Pdpb.ResetIdRequest request = Pdpb.ResetIdRequest.newBuilder()
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
                                                         .setHeader(header)
                                                         .setKey(key)
                                                         .build();
========
                .setHeader(header)
                .setKey(key)
                .build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        Pdpb.ResetIdResponse response = blockingUnaryCall(PDGrpc.getResetIdMethod(), request);
        handleResponseError(response.getHeader());
        return response;
    }

    public Metapb.Member getLeader() throws PDException {
        Pdpb.GetMembersRequest request = Pdpb.GetMembersRequest.newBuilder()
                                                               .setHeader(header)
                                                               .build();
        Pdpb.GetMembersResponse response = blockingUnaryCall(PDGrpc.getGetMembersMethod(), request);
        handleResponseError(response.getHeader());
        return response.getLeader();
    }

    public Pdpb.GetMembersResponse getMembers() throws PDException {
        Pdpb.GetMembersRequest request = Pdpb.GetMembersRequest.newBuilder()
                .setHeader(header)
                .build();
        Pdpb.GetMembersResponse response = blockingUnaryCall(PDGrpc.getGetMembersMethod(), request);
        handleResponseError(response.getHeader());
        return response;
    }

    public Metapb.ClusterStats getClusterStats(long storeId) throws PDException {
        Pdpb.GetClusterStatsRequest request = Pdpb.GetClusterStatsRequest.newBuilder()
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
                                                                         .setHeader(header)
                                                                         .build();
        Pdpb.GetClusterStatsResponse response =
                blockingUnaryCall(PDGrpc.getGetClusterStatsMethod(), request);
========
                .setHeader(header)
                .setStoreId(storeId)
                .build();
        Pdpb.GetClusterStatsResponse response = blockingUnaryCall(PDGrpc.getGetClusterStatsMethod(), request);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        handleResponseError(response.getHeader());
        return response.getCluster();
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
    private <ReqT, RespT, StubT extends AbstractBlockingStub<StubT>> RespT
    blockingUnaryCall(MethodDescriptor<ReqT, RespT> method, ReqT req) throws PDException {
        return blockingUnaryCall(method, req, 1);
    }

    private <ReqT, RespT, StubT extends AbstractBlockingStub<StubT>> RespT
    blockingUnaryCall(MethodDescriptor<ReqT, RespT> method, ReqT req, int retry) throws
                                                                                 PDException {
        io.grpc.stub.AbstractBlockingStub<StubT> stub = (AbstractBlockingStub<StubT>) getStub();
        try {
            RespT resp = io.grpc.stub.ClientCalls.blockingUnaryCall(stub.getChannel(), method,
                                                                    stub.getCallOptions(), req);
            return resp;
        } catch (Exception e) {
            log.error(method.getFullMethodName() + " exception, {}", e.getMessage());
            if (e instanceof StatusRuntimeException) {
                if (retry < stubProxy.getHostCount()) {
                    closeStub(true);
                    return blockingUnaryCall(method, req, ++retry);
                }
========
    public Metapb.ClusterStats getClusterStats(int storeGroupId) throws PDException {
        Pdpb.GetClusterStatsRequest request = Pdpb.GetClusterStatsRequest.newBuilder()
                .setHeader(header)
                .setStoreGroup(storeGroupId)
                .build();
        Pdpb.GetClusterStatsResponse response = blockingUnaryCall(PDGrpc.getGetClusterStatsMethod(), request);
        handleResponseError(response.getHeader());
        return response.getCluster();
    }

    public void changePeerList(String peerList) throws PDException {
        ClusterOp.ChangePeerListRequest request = ClusterOp.ChangePeerListRequest.newBuilder()
                .setPeerList(peerList)
                .setHeader(header).build();
        ClusterOp.ChangePeerListResponse response =
                blockingUnaryCall(PDGrpc.getChangePeerListMethod(), request);
        handleResponseError(response.getHeader());
    }

    public void reportTask(MetaTask.Task task) throws PDException {
        ClusterOp.ReportTaskRequest request = ClusterOp.ReportTaskRequest.newBuilder()
                .setHeader(header)
                .setTask(task).build();
        ClusterOp.ReportTaskResponse response = blockingUnaryCall(PDGrpc.getReportTaskMethod(), request);
        handleResponseError(response.getHeader());
    }

    public void deleteShardGroup(int groupId) throws PDException {
        Pdpb.DeleteShardGroupRequest request = Pdpb.DeleteShardGroupRequest
                .newBuilder()
                .setHeader(header)
                .setGroupId(groupId)
                .build();
        Pdpb.DeleteShardGroupResponse response =
                blockingUnaryCall(PDGrpc.getDeleteShardGroupMethod(), request);

        handleResponseError(response.getHeader());
    }

    public Metapb.ShardGroup getShardGroup(int partId) throws PDException {
        Metapb.ShardGroup group = cache.getShardGroup(partId);
        if (group == null) {
            group = getShardGroupDirect(partId);
            if (config.isEnableCache()) {
                cache.updateShardGroup(group);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
            }
        }
        return group;
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
    private void handleResponseError(Pdpb.ResponseHeader header) throws
                                                                 PDException {
        var errorType = header.getError().getType();
        if (header.hasError() && errorType != Pdpb.ErrorType.OK) {

            throw new PDException(header.getError().getTypeValue(),
                                  String.format(
                                          "PD request error, error code = %d, msg = %s",
                                          header.getError().getTypeValue(),
                                          header.getError().getMessage()));
========
    public void invalidPartitionCache() {
        //  检查是否存在缓存
        cache.removePartitions();
    }

    /**
     * 删除分区缓存
     */
    public void invalidPartitionCache(String graphName, int partitionId) {
        //  检查是否存在缓存
        if (null != cache.getPartitionById(graphName, partitionId)) {
            cache.removePartition(graphName, partitionId);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        }

    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
    public void addEventListener(PDEventListener listener) {
        eventListeners.add(listener);
    }

    public PDWatch getWatchClient() {
        return new PDWatchImpl(stubProxy.getHost());
    }

    /**
     * Returns the store status information
     */
    public List<Metapb.Store> getStoreStatus(boolean offlineExcluded) throws PDException {
        Pdpb.GetAllStoresRequest request = Pdpb.GetAllStoresRequest.newBuilder()
                                                                   .setHeader(header)
                                                                   .setExcludeOfflineStores(
                                                                           offlineExcluded)
                                                                   .build();
        Pdpb.GetAllStoresResponse response = getStub().getStoreStatus(request);
========
    /**
     * 根据storeId返回Store对象
     *
     * @param storeId
     * @return
     * @throws PDException
     */
    public Metapb.Store getStore(long storeId) throws PDException {
        Metapb.Store store = cache.getStoreById(storeId);
        if (store == null) {
            Pdpb.GetStoreRequest request = Pdpb.GetStoreRequest.newBuilder()
                    .setHeader(header)
                    .setStoreId(storeId).build();
            // Pdpb.GetStoreResponse response = getStub().getStore(request);
            Pdpb.GetStoreResponse response = blockingUnaryCall(PDGrpc.getGetStoreMethod(), request);
            handleResponseError(response.getHeader());
            store = response.getStore();
            if (config.isEnableCache()) {
                cache.addStore(storeId, store);
            }
        }
        return store;
    }

    /**
     * 更新Store信息，包括上下线等
     *
     * @param store
     * @return
     */
    public Metapb.Store updateStore(Metapb.Store store) throws PDException {
        Pdpb.SetStoreRequest request = Pdpb.SetStoreRequest.newBuilder()
                .setHeader(header)
                .setStore(store).build();

        // Pdpb.SetStoreResponse response = getStub().setStore(request);
        Pdpb.SetStoreResponse response = blockingUnaryCall(PDGrpc.getSetStoreMethod(), request);
        handleResponseError(response.getHeader());
        store = response.getStore();
        if (config.isEnableCache()) {
            cache.addStore(store.getId(), store);
        }
        return store;
    }

    public List<Metapb.Store> getActiveStores() throws PDException {
        Pdpb.GetAllStoresRequest request = Pdpb.GetAllStoresRequest.newBuilder()
                .setHeader(header)
                .setGraphName("")
                .setExcludeOfflineStores(true)
                .build();
        // Pdpb.GetAllStoresResponse response = getStub().getAllStores(request);
        Pdpb.GetAllStoresResponse response = blockingUnaryCall(PDGrpc.getGetAllStoresMethod(), request);
        handleResponseError(response.getHeader());
        return response.getStoresList();

    }

    /**
     * 返回活跃的Store
     *
     * @param graphName
     * @return
     */
    public List<Metapb.Store> getActiveStores(String graphName) throws PDException {
        Set<Metapb.Store> stores = new HashSet<>();
        KVPair<Metapb.Partition, Metapb.Shard> ptShard = this.getPartitionByCode(graphName, 0);
        while (ptShard != null) {
            stores.add(this.getStore(ptShard.getValue().getStoreId()));
            if (ptShard.getKey().getEndKey() < PartitionUtils.MAX_VALUE) {
                ptShard = this.getPartitionByCode(graphName, ptShard.getKey().getEndKey());
            } else {
                ptShard = null;
            }
        }
        return new ArrayList<>(stores);
    }

    /**
     * 返回活跃的Store
     *
     * @param graphName
     * @return
     */
    public List<Metapb.Store> getAllStores(String graphName) throws PDException {
        Pdpb.GetAllStoresRequest request = Pdpb.GetAllStoresRequest.newBuilder()
                .setHeader(header)
                .setGraphName(graphName)
                .setExcludeOfflineStores(false)
                .build();
        // Pdpb.GetAllStoresResponse response = getStub().getAllStores(request);
        Pdpb.GetAllStoresResponse response = blockingUnaryCall(PDGrpc.getGetAllStoresMethod(), request);
        handleResponseError(response.getHeader());
        return response.getStoresList();

    }

    /**
     * Store心跳，定期调用，保持在线状态
     *
     * @param stats
     * @throws PDException
     */
    public Metapb.ClusterStats storeHeartbeat(Metapb.StoreStats stats) throws PDException {
        Pdpb.StoreHeartbeatRequest request = Pdpb.StoreHeartbeatRequest.newBuilder()
                .setHeader(header)
                .setStats(stats).build();
        // Pdpb.StoreHeartbeatResponse response = getStub().storeHeartbeat(request);
        Pdpb.StoreHeartbeatResponse response = blockingUnaryCall(PDGrpc.getStoreHeartbeatMethod(), request);
        handleResponseError(response.getHeader());
        return response.getClusterStats();
    }

    /**
     * 返回Store状态信息
     */
    public List<Metapb.Store> getStoreStatus(boolean offlineExcluded) throws PDException {
        Pdpb.GetAllStoresRequest request = Pdpb.GetAllStoresRequest.newBuilder()
                .setHeader(header)
                .setExcludeOfflineStores(offlineExcluded)
                .build();
        // Pdpb.GetAllStoresResponse response = getStub().getStoreStatus(request);
        Pdpb.GetAllStoresResponse response = blockingUnaryCall(PDGrpc.getGetStoreStatusMethod(), request);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        handleResponseError(response.getHeader());
        List<Metapb.Store> stores = response.getStoresList();
        return stores;
    }

    public void setGraphSpace(String graphSpaceName, long storageLimit) throws PDException {
        Metapb.GraphSpace graphSpace = Metapb.GraphSpace.newBuilder().setName(graphSpaceName)
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
                                                        .setStorageLimit(storageLimit)
                                                        .setTimestamp(System.currentTimeMillis())
                                                        .build();
========
                .setStorageLimit(storageLimit)
                .setTimestamp(System.currentTimeMillis()).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        Pdpb.SetGraphSpaceRequest request = Pdpb.SetGraphSpaceRequest.newBuilder()
                .setHeader(header)
                .setGraphSpace(graphSpace)
                .build();
        // Pdpb.SetGraphSpaceResponse response = getStub().setGraphSpace(request);
        Pdpb.SetGraphSpaceResponse response = blockingUnaryCall(PDGrpc.getSetGraphSpaceMethod(), request);
        handleResponseError(response.getHeader());
    }

    public List<Metapb.GraphSpace> getGraphSpace(String graphSpaceName) throws
            PDException {
        Pdpb.GetGraphSpaceRequest.Builder builder = Pdpb.GetGraphSpaceRequest.newBuilder();
        Pdpb.GetGraphSpaceRequest request;
        builder.setHeader(header);
        if (graphSpaceName != null && graphSpaceName.length() > 0) {
            builder.setGraphSpaceName(graphSpaceName);
        }
        request = builder.build();
        // Pdpb.GetGraphSpaceResponse response = getStub().getGraphSpace(request);
        Pdpb.GetGraphSpaceResponse response = blockingUnaryCall(PDGrpc.getGetGraphSpaceMethod(), request);
        List<Metapb.GraphSpace> graphSpaceList = response.getGraphSpaceList();
        handleResponseError(response.getHeader());
        return graphSpaceList;
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
    public void setPDConfig(int partitionCount, String peerList, int shardCount,
                            long version) throws PDException {
        Metapb.PDConfig pdConfig = Metapb.PDConfig.newBuilder().setPartitionCount(partitionCount)
                                                  .setPeersList(peerList).setShardCount(shardCount)
                                                  .setVersion(version)
                                                  .setTimestamp(System.currentTimeMillis())
                                                  .build();
        Pdpb.SetPDConfigRequest request = Pdpb.SetPDConfigRequest.newBuilder()
                                                                 .setHeader(header)
                                                                 .setPdConfig(pdConfig)
                                                                 .build();
        Pdpb.SetPDConfigResponse response = getStub().setPDConfig(request);
========
    public void setPDConfig(int partitionCount, String peerList, int shardCount, long version) throws
            PDException {
        Metapb.PDConfig pdConfig = Metapb.PDConfig.newBuilder()
                .setPeersList(peerList)
                .setShardCount(shardCount)
                .setVersion(version)
                .setTimestamp(System.currentTimeMillis())
                .build();
        Pdpb.SetPDConfigRequest request = Pdpb.SetPDConfigRequest.newBuilder()
                .setHeader(header)
                .setPdConfig(pdConfig)
                .build();
        // Pdpb.SetPDConfigResponse response = getStub().setPDConfig(request);
        Pdpb.SetPDConfigResponse response = blockingUnaryCall(PDGrpc.getSetPDConfigMethod(), request);
        handleResponseError(response.getHeader());
    }

    public void setPDConfig(Metapb.PDConfig pdConfig) throws PDException {
        Pdpb.SetPDConfigRequest request = Pdpb.SetPDConfigRequest.newBuilder()
                .setHeader(header)
                .setPdConfig(pdConfig)
                .build();
        // Pdpb.SetPDConfigResponse response = getStub().setPDConfig(request);
        Pdpb.SetPDConfigResponse response = blockingUnaryCall(PDGrpc.getSetPDConfigMethod(), request);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        handleResponseError(response.getHeader());
    }

    public Metapb.PDConfig getPDConfig() throws PDException {
        Pdpb.GetPDConfigRequest request = Pdpb.GetPDConfigRequest.newBuilder()
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
                                                                 .setHeader(header)
                                                                 .build();
        Pdpb.GetPDConfigResponse response = getStub().getPDConfig(request);
========
                .setHeader(header)
                .build();
        // Pdpb.GetPDConfigResponse response = getStub().getPDConfig(request);
        Pdpb.GetPDConfigResponse response = blockingUnaryCall(PDGrpc.getGetPDConfigMethod(), request);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        handleResponseError(response.getHeader());
        return response.getPdConfig();
    }

    public void setPDConfig(Metapb.PDConfig pdConfig) throws PDException {
        Pdpb.SetPDConfigRequest request = Pdpb.SetPDConfigRequest.newBuilder()
                                                                 .setHeader(header)
                                                                 .setPdConfig(pdConfig)
                                                                 .build();
        Pdpb.SetPDConfigResponse response = getStub().setPDConfig(request);
        handleResponseError(response.getHeader());
    }

    public Metapb.PDConfig getPDConfig(long version) throws PDException {
        Pdpb.GetPDConfigRequest request = Pdpb.GetPDConfigRequest.newBuilder().setHeader(
                header).setVersion(version).build();
        // Pdpb.GetPDConfigResponse response = getStub().getPDConfig(request);
        Pdpb.GetPDConfigResponse response = blockingUnaryCall(PDGrpc.getGetPDConfigMethod(), request);
        handleResponseError(response.getHeader());
        return response.getPdConfig();
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
    public void changePeerList(String peerList) throws PDException {
        Pdpb.ChangePeerListRequest request = Pdpb.ChangePeerListRequest.newBuilder()
                                                                       .setPeerList(peerList)
                                                                       .setHeader(header).build();
        Pdpb.getChangePeerListResponse response =
                blockingUnaryCall(PDGrpc.getChangePeerListMethod(), request);
        handleResponseError(response.getHeader());
    }

    /**
     * Working mode
     * Auto: If the number of partitions on each store reaches the maximum value, you need to
     * specify the store group id. The store group id is 0, which is the default partition
     * splitData(ClusterOp.OperationMode mode, int storeGroupId, List<ClusterOp.SplitDataParam>
     * params)
     * mode = Auto storeGroupId, params
     *
     * @throws PDException
     */
    public void splitData() throws PDException {
        Pdpb.SplitDataRequest request = Pdpb.SplitDataRequest.newBuilder()
                                                             .setHeader(header)
                                                             .setMode(Pdpb.OperationMode.Auto)
                                                             .build();
        Pdpb.SplitDataResponse response = getStub().splitData(request);
        handleResponseError(response.getHeader());
    }

    /**
     * Working mode
     * Auto: If the number of partitions on each store reaches the maximum value, you need to
     * specify the store group id. The store group id is 0, which is the default partition
     * Expert: Expert Mode, Specifier is required splitParams, limit SplitDataParam in the same
     * store group
     *
     * @param mode
     * @param params
     * @throws PDException
     */
    public void splitData(Pdpb.OperationMode mode, List<Pdpb.SplitDataParam> params) throws
                                                                                     PDException {
        Pdpb.SplitDataRequest request = Pdpb.SplitDataRequest.newBuilder()
                                                             .setHeader(header)
                                                             .setMode(mode)
                                                             .addAllParam(params).build();
        Pdpb.SplitDataResponse response = getStub().splitData(request);
========
    public void splitData(ClusterOp.OperationMode mode, int storeGroupId, List<ClusterOp.SplitDataParam> params)
            throws PDException {
        ClusterOp.SplitDataRequest request = ClusterOp.SplitDataRequest.newBuilder()
                .setHeader(header)
                .setMode(mode)
                .setStoreGroupId(storeGroupId)
                .addAllParam(params).build();
        ;
        ClusterOp.SplitDataResponse response = blockingUnaryCall(PDGrpc.getSplitDataMethod(), request);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        handleResponseError(response.getHeader());
    }


    public void splitGraphData(String graphName, int toCount) throws PDException {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
        Pdpb.SplitGraphDataRequest request = Pdpb.SplitGraphDataRequest.newBuilder()
                                                                       .setHeader(header)
                                                                       .setGraphName(graphName)
                                                                       .setToCount(toCount)
                                                                       .build();
        Pdpb.SplitDataResponse response = getStub().splitGraphData(request);
========
        ClusterOp.SplitGraphDataRequest request = ClusterOp.SplitGraphDataRequest.newBuilder()
                .setHeader(header)
                .setGraphName(graphName)
                .setToCount(toCount)
                .build();
        // Pdpb.SplitDataResponse response = getStub().splitGraphData(request);
        ClusterOp.SplitDataResponse response = blockingUnaryCall(PDGrpc.getSplitGraphDataMethod(), request);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        handleResponseError(response.getHeader());
    }

    /**
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
     * To automatically transfer to the same number of partitions on each Store, it is
     * recommended to use balancePartition(int storeGroupId) to specify the storeGroupId
     *
     * @throws PDException
     */
    public void balancePartition() throws PDException {
        Pdpb.MovePartitionRequest request = Pdpb.MovePartitionRequest.newBuilder()
                                                                     .setHeader(header)
                                                                     .setMode(
                                                                             Pdpb.OperationMode.Auto)
                                                                     .build();
        Pdpb.MovePartitionResponse response = getStub().movePartition(request);
        handleResponseError(response.getHeader());
    }

    /**
     * Migrate partitions in manual mode
     * // Working mode
     * //  Auto: Automatic transfer to the same number of partitions per Store
     * //  Expert: Expert Mode, Specifier is required transferParams
     *
     * @param params Designation transferParams, expert mode, request source store / target store
     *               in the same store group
     * @throws PDException
     */
    public void movePartition(Pdpb.OperationMode mode, List<Pdpb.MovePartitionParam> params) throws
                                                                                             PDException {
        Pdpb.MovePartitionRequest request = Pdpb.MovePartitionRequest.newBuilder()
                                                                     .setHeader(header)
                                                                     .setMode(mode)
                                                                     .addAllParam(params).build();
        Pdpb.MovePartitionResponse response = getStub().movePartition(request);
        handleResponseError(response.getHeader());
    }

    public void reportTask(MetaTask.Task task) throws PDException {
        Pdpb.ReportTaskRequest request = Pdpb.ReportTaskRequest.newBuilder()
                                                               .setHeader(header)
                                                               .setTask(task).build();
        Pdpb.ReportTaskResponse response = blockingUnaryCall(PDGrpc.getReportTaskMethod(), request);
========
     * 平衡分区
     * @param mode auto or expert
     * @param storeGroupId for auto
     * @param params for expert
     * @throws PDException errors occurs
     */
    public void balancePartition(ClusterOp.OperationMode mode, int storeGroupId,
                                 List<ClusterOp.MovePartitionParam> params) throws PDException {
        ClusterOp.MovePartitionRequest request = ClusterOp.MovePartitionRequest.newBuilder()
                .setHeader(header)
                .setMode(mode)
                .setStoreGroupId(storeGroupId)
                .addAllParam(params)
                .build();
        ClusterOp.MovePartitionResponse response = blockingUnaryCall(PDGrpc.getMovePartitionMethod(), request);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        handleResponseError(response.getHeader());
    }

    public Metapb.PartitionStats getPartitionsStats(String graph, int partId) throws PDException {
        Pdpb.GetPartitionStatsRequest request = Pdpb.GetPartitionStatsRequest.newBuilder()
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
                                                                             .setHeader(header)
                                                                             .setGraphName(graph)
                                                                             .setPartitionId(partId)
                                                                             .build();
        Pdpb.GetPartitionStatsResponse response = getStub().getPartitionStats(request);
========
                .setHeader(header)
                .setGraphName(graph)
                .setPartitionId(partId).build();
        //   Pdpb.GetPartitionStatsResponse response = getStub().getPartitionStats(request);
        Pdpb.GetPartitionStatsResponse response = blockingUnaryCall(PDGrpc.getGetPartitionStatsMethod(), request);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        handleResponseError(response.getHeader());
        return response.getPartitionStats();
    }

    /**
     * Balance the number of leaders in different stores
     */
    public void balanceLeaders() throws PDException {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
        Pdpb.BalanceLeadersRequest request = Pdpb.BalanceLeadersRequest.newBuilder()
                                                                       .setHeader(header)
                                                                       .build();
        Pdpb.BalanceLeadersResponse response = getStub().balanceLeaders(request);
========
        ClusterOp.BalanceLeadersRequest request = ClusterOp.BalanceLeadersRequest.newBuilder()
                .setHeader(header)
                .build();
        // Pdpb.BalanceLeadersResponse response = getStub().balanceLeaders(request);
        ClusterOp.BalanceLeadersResponse response = blockingUnaryCall(PDGrpc.getBalanceLeadersMethod(), request);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        handleResponseError(response.getHeader());
    }

    /**
     * Remove the store from the PD
     */
    public Metapb.Store delStore(long storeId) throws PDException {
        Pdpb.DetStoreRequest request = Pdpb.DetStoreRequest.newBuilder()
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
                                                           .setHeader(header)
                                                           .setStoreId(storeId)
                                                           .build();
        Pdpb.DetStoreResponse response = getStub().delStore(request);
========
                .setHeader(header)
                .setStoreId(storeId)
                .build();
        // Pdpb.DetStoreResponse response = getStub().delStore(request);
        Pdpb.DetStoreResponse response = blockingUnaryCall(PDGrpc.getDelStoreMethod(), request);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        handleResponseError(response.getHeader());
        return response.getStore();
    }

    /**
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
     * Compaction on rocksdb as a whole
========
     * 对rocksdb整体进行compaction
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
     *
     * @throws PDException
     */
    public void dbCompaction() throws PDException {
        ClusterOp.DbCompactionRequest request = ClusterOp.DbCompactionRequest
                .newBuilder()
                .setHeader(header)
                .build();
        //  Pdpb.DbCompactionResponse response = getStub().dbCompaction(request);
        ClusterOp.DbCompactionResponse response = blockingUnaryCall(PDGrpc.getDbCompactionMethod(), request);
        handleResponseError(response.getHeader());
    }

    /**
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
     * Compaction on rocksdb specified tables
========
     * 对rocksdb指定表进行compaction
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
     *
     * @param tableName
     * @throws PDException
     */
    public void dbCompaction(String tableName) throws PDException {
        ClusterOp.DbCompactionRequest request = ClusterOp.DbCompactionRequest
                .newBuilder()
                .setHeader(header)
                .setTableName(tableName)
                .build();
        //  Pdpb.DbCompactionResponse response = getStub().dbCompaction(request);
        ClusterOp.DbCompactionResponse response = blockingUnaryCall(PDGrpc.getDbCompactionMethod(), request);
        handleResponseError(response.getHeader());
    }

    /**
     * Merge partitions to reduce the current partition to toCount
     *
     * @param toCount The number of partitions that can be scaled down
     * @throws PDException
     */
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
    public void combineCluster(int toCount) throws PDException {
        Pdpb.CombineClusterRequest request = Pdpb.CombineClusterRequest
========
    public void combineCluster(int shardGroupId, int toCount) throws PDException {
        ClusterOp.CombineClusterRequest request = ClusterOp.CombineClusterRequest
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
                .newBuilder()
                .setHeader(header)
                .setStoreGroupId(shardGroupId)
                .setToCount(toCount)
                .build();
        ClusterOp.CombineClusterResponse response = blockingUnaryCall(PDGrpc.getCombineClusterMethod(), request);
        handleResponseError(response.getHeader());
    }

    /**
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
     * Scaling a single image to toCount is similar to splitting to ensure that the number of
     * partitions in the same store group is the same.
     * If you have special requirements, you can consider migrating to other groups
========
     * 将单图缩容到 toCount个
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
     *
     * @param graphName graph name
     * @param toCount   target count
     * @throws PDException
     */
    public void combineGraph(String graphName, int toCount) throws PDException {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
        Pdpb.CombineGraphRequest request = Pdpb.CombineGraphRequest
========
        ClusterOp.CombineGraphRequest request = ClusterOp.CombineGraphRequest
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
                .newBuilder()
                .setHeader(header)
                .setGraphName(graphName)
                .setToCount(toCount)
                .build();
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
        Pdpb.CombineGraphResponse response = getStub().combineGraph(request);
        handleResponseError(response.getHeader());
    }

    public void deleteShardGroup(int groupId) throws PDException {
        Pdpb.DeleteShardGroupRequest request = Pdpb.DeleteShardGroupRequest
                .newBuilder()
                .setHeader(header)
                .setGroupId(groupId)
                .build();
        Pdpb.DeleteShardGroupResponse response =
                blockingUnaryCall(PDGrpc.getDeleteShardGroupMethod(), request);

========
        // Pdpb.CombineGraphResponse response = getStub().combineGraph(request);
        ClusterOp.CombineGraphResponse response = blockingUnaryCall(PDGrpc.getCombineGraphMethod(), request);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        handleResponseError(response.getHeader());
    }

    /**
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
     * Used for the store's shard list rebuild
========
     * 用于 store的 shard list重建
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
     *
     * @param groupId shard group id
     * @param shards  shard list, delete when shards size is 0
     */
    public void updateShardGroupOp(int groupId, List<Metapb.Shard> shards) throws PDException {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
        Pdpb.ChangeShardRequest request = Pdpb.ChangeShardRequest.newBuilder()
                                                                 .setHeader(header)
                                                                 .setGroupId(groupId)
                                                                 .addAllShards(shards)
                                                                 .build();
        Pdpb.ChangeShardResponse response = getStub().updateShardGroupOp(request);
========
        ClusterOp.ChangeShardRequest request = ClusterOp.ChangeShardRequest.newBuilder()
                .setHeader(header)
                .setGroupId(groupId)
                .addAllShards(shards)
                .build();
        // Pdpb.ChangeShardResponse response = getStub().updateShardGroupOp(request);
        ClusterOp.ChangeShardResponse response = blockingUnaryCall(PDGrpc.getUpdateShardGroupOpMethod(), request);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        handleResponseError(response.getHeader());
    }

    /**
     * invoke fireChangeShard command
     *
     * @param groupId shard group id
     * @param shards  shard list
     */
    public void changeShard(int groupId, List<Metapb.Shard> shards) throws PDException {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
        Pdpb.ChangeShardRequest request = Pdpb.ChangeShardRequest.newBuilder()
                                                                 .setHeader(header)
                                                                 .setGroupId(groupId)
                                                                 .addAllShards(shards)
                                                                 .build();
        Pdpb.ChangeShardResponse response = getStub().changeShard(request);
        handleResponseError(response.getHeader());
    }

    public ClientCache getCache() {
        return cache;
    }

    public CacheResponse getClientCache() throws PDException {
        GetGraphRequest request = GetGraphRequest.newBuilder().setHeader(header).build();
        CacheResponse cache = getStub().getCache(request);
        handleResponseError(cache.getHeader());
        return cache;
    }

    public CachePartitionResponse getPartitionCache(String graph) throws PDException {
        GetGraphRequest request =
                GetGraphRequest.newBuilder().setHeader(header).setGraphName(graph).build();
        CachePartitionResponse ps = getStub().getPartitions(request);
========
        ClusterOp.ChangeShardRequest request = ClusterOp.ChangeShardRequest.newBuilder()
                .setHeader(header)
                .setGroupId(groupId)
                .addAllShards(shards)
                .build();
        // Pdpb.ChangeShardResponse response = getStub().changeShard(request);
        ClusterOp.ChangeShardResponse response = blockingUnaryCall(PDGrpc.getChangeShardMethod(), request);
        handleResponseError(response.getHeader());
    }

    public Pdpb.CacheResponse getClientCache() throws PDException {
        Pdpb.GetGraphRequest request = Pdpb.GetGraphRequest.newBuilder().setHeader(header).build();
        // Pdpb.CacheResponse cache = getStub().getCache(request);
        Pdpb.CacheResponse cache = blockingUnaryCall(PDGrpc.getGetCacheMethod(), request);
        handleResponseError(cache.getHeader());
        return cache;
    }

    public Pdpb.CachePartitionResponse getPartitionCache(String graph) throws PDException {
        Pdpb.GetGraphRequest request = Pdpb.GetGraphRequest.newBuilder().setHeader(header).setGraphName(graph).build();
        // Pdpb.CachePartitionResponse ps = getStub().getPartitions(request);
        Pdpb.CachePartitionResponse ps = blockingUnaryCall(PDGrpc.getGetPartitionsMethod(), request);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
        handleResponseError(ps.getHeader());
        return ps;
    }

    public void updatePdRaft(String raftConfig) throws PDException {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/PDClient.java
        Pdpb.UpdatePdRaftRequest request = Pdpb.UpdatePdRaftRequest.newBuilder()
                                                                   .setHeader(header)
                                                                   .setConfig(raftConfig)
                                                                   .build();
        Pdpb.UpdatePdRaftResponse response = getStub().updatePdRaft(request);
        handleResponseError(response.getHeader());
    }

    public interface PDEventListener {

        void onStoreChanged(NodeEvent event);

        void onPartitionChanged(PartitionEvent event);

        void onGraphChanged(WatchResponse event);

        default void onShardGroupChanged(WatchResponse event) {
        }

    }

    static class StubProxy {

        private final LinkedList<String> hostList = new LinkedList<>();
        private volatile PDGrpc.PDBlockingStub stub;
        private String leader;

        public StubProxy(String[] hosts) {
            for (String host : hosts) {
                if (!host.isEmpty()) {
                    hostList.offer(host);
                }
            }
        }

        public String nextHost() {
            String host = hostList.poll();
            hostList.offer(host);
            return host;
        }

        public void set(PDGrpc.PDBlockingStub stub) {
            this.stub = stub;
        }

        public PDGrpc.PDBlockingStub get() {
            return this.stub;
        }

        public String getHost() {
            return hostList.peek();
        }

        public int getHostCount() {
            return hostList.size();
        }

        public String getLeader() {
            return leader;
        }

        public void setLeader(String leader) {
            this.leader = leader;
        }
    }
========
        ClusterOp.UpdatePdRaftRequest request = ClusterOp.UpdatePdRaftRequest.newBuilder()
                .setHeader(header)
                .setConfig(raftConfig)
                .build();
        // Pdpb.UpdatePdRaftResponse response = getStub().updatePdRaft(request);
        ClusterOp.UpdatePdRaftResponse response = blockingUnaryCall(PDGrpc.getUpdatePdRaftMethod(), request);
        handleResponseError(response.getHeader());
    }

    public long submitBuildIndexTask(Metapb.BuildIndexParam param) throws PDException {
        Pdpb.IndexTaskCreateRequest request = Pdpb.IndexTaskCreateRequest.newBuilder()
                .setHeader(header)
                .setParam(param)
                .build();
        //  var response = getStub().submitTask(request);
        var response = blockingUnaryCall(PDGrpc.getSubmitIndexTaskMethod(), request);
        handleResponseError(response.getHeader());
        return response.getTaskId();
    }

    public long submitBackupGraphTask(String sourceGraph, String targetGraph) throws PDException {
        Pdpb.BackupGraphRequest request = Pdpb.BackupGraphRequest.newBuilder()
                .setGraphName(sourceGraph)
                .setTargetGraphName(targetGraph)
                .build();
        //  var response = getStub().submitTask(request);
        var response = blockingUnaryCall(PDGrpc.getSubmitBackupGraphTaskMethod(), request);
        handleResponseError(response.getHeader());
        return response.getTaskId();
    }

    public Pdpb.TaskQueryResponse queryBuildIndexTaskStatus(long taskId) throws PDException {
        Pdpb.TaskQueryRequest request = Pdpb.TaskQueryRequest.newBuilder()
                .setHeader(header)
                .setTaskId(taskId)
                .build();
        // var response = getStub().queryTaskState(request);
        var response = blockingUnaryCall(PDGrpc.getQueryTaskStateMethod(), request);
        handleResponseError(response.getHeader());
        return response;
    }

    public Pdpb.TaskQueryResponse retryTask(long taskId) throws PDException {
        Pdpb.TaskQueryRequest request = Pdpb.TaskQueryRequest.newBuilder()
                .setHeader(header)
                .setTaskId(taskId)
                .build();
        // var response = getStub().retryIndexTask(request);
        var response = blockingUnaryCall(PDGrpc.getRetryTaskMethod(), request);
        handleResponseError(response.getHeader());
        return response;
    }

    public Pdpb.GraphStatsResponse getGraphStats(String graphName) throws PDException {
        Pdpb.GetGraphRequest request =
                Pdpb.GetGraphRequest.newBuilder().setHeader(header).setGraphName(graphName).build();
        // Pdpb.GraphStatsResponse graphStats = getStub().getGraphStats(request);
        Pdpb.GraphStatsResponse graphStats = blockingUnaryCall(PDGrpc.getGetGraphStatsMethod(), request);
        handleResponseError(graphStats.getHeader());
        return graphStats;
    }

    /**
     * 返回startKey和endKey跨越的所有分区信息
     *
     * @param graphName
     * @param startKey
     * @param endKey
     * @return
     * @throws PDException
     */
    public List<KVPair<Metapb.Partition, Metapb.Shard>> scanPartitions(String graphName, byte[] startKey,
                                                                       byte[] endKey) throws PDException {
        List<KVPair<Metapb.Partition, Metapb.Shard>> partitions = new ArrayList<>();
        KVPair<Metapb.Partition, Metapb.Shard> startPartShard = getPartition(graphName, startKey);
        KVPair<Metapb.Partition, Metapb.Shard> endPartShard = getPartition(graphName, endKey);
        if (startPartShard == null || endPartShard == null) {
            return null;
        }
        partitions.add(startPartShard);
        while (startPartShard.getKey().getEndKey() < endPartShard.getKey().getEndKey()
                && startPartShard.getKey().getEndKey() < PartitionUtils.MAX_VALUE /*排除最后一个分区*/) {
            startPartShard = getPartitionByCode(graphName, startPartShard.getKey().getEndKey());
            partitions.add(startPartShard);
        }
        return partitions;
    }

    /**
     * 查询Key所属分区信息
     *
     * @param graphName
     * @param key
     * @return
     * @throws PDException
     */
    public KVPair<Metapb.Partition, Metapb.Shard> getPartition(String graphName, byte[] key) throws PDException {
        //  先查cache，cache没有命中，在调用PD
        KVPair<Metapb.Partition, Metapb.Shard> partShard = cache.getPartitionByKey(graphName, key);
        partShard = getKvPair(graphName, key, partShard);
        return partShard;
    }

    public KVPair<Metapb.Partition, Metapb.Shard> getPartition(String graphName, byte[] key, int code) throws PDException {
        KVPair<Metapb.Partition, Metapb.Shard> partShard = cache.getPartitionByCode(graphName, code);
        partShard = getKvPair(graphName, key, partShard);
        return partShard;
    }

    /**
     * Hugegraph-store调用，更新缓存
     *
     * @param partition
     */
    public void updatePartitionCache(Metapb.Partition partition, Metapb.Shard leader) {
        if (config.isEnableCache()) {
            cache.update(partition.getGraphName(), partition.getId(), partition);
            cache.updateLeader(partition.getId(), leader);
        }
    }

    /**
     * Hugegraph server 调用，Leader发生改变，更新缓存
     */
    public void updatePartitionLeader(String graphName, int partId, long leaderStoreId) {
        KVPair<Metapb.Partition, Metapb.Shard> partShard = null;
        try {
            partShard = this.getPartitionById(graphName, partId);

            if (partShard != null && partShard.getValue().getStoreId() != leaderStoreId) {
                var shardGroup = this.getShardGroup(partId);
                Metapb.Shard shard = null;
                List<Metapb.Shard> shards = new ArrayList<>();

                for (Metapb.Shard s : shardGroup.getShardsList()) {
                    if (s.getStoreId() == leaderStoreId) {
                        shard = s;
                        shards.add(Metapb.Shard.newBuilder(s)
                                .setStoreId(s.getStoreId())
                                .setRole(Metapb.ShardRole.Leader).build());
                    } else {
                        shards.add(Metapb.Shard.newBuilder(s)
                                .setStoreId(s.getStoreId())
                                .setRole(Metapb.ShardRole.Follower).build());
                    }
                }

                if (config.isEnableCache()) {
                    if (shard == null) {
                        //  分区的shard中未找到leader，说明分区发生了迁移
                        cache.removePartition(graphName, partId);
                    } else {
                        cache.updateLeader(partId, shard);
                    }
                }
            }
        } catch (PDException e) {
            log.error("getPartitionException: {}", e.getMessage());
        }
    }

    public Metapb.StoreGroup createStoreGroup(int groupId, String name, int partitionCount) throws PDException {
        StoreGroup.CreateStoreGroupRequest request = StoreGroup.CreateStoreGroupRequest.newBuilder()
                .setHeader(header)
                .setGroupId(groupId)
                .setName(name)
                .setPartitionCount(partitionCount)
                .build();

        StoreGroup.CreateStoreGroupResponse response = blockingUnaryCall(PDGrpc.getCreateStoreGroupMethod(), request);
        handleResponseError(response.getHeader());
        return response.getStoreGroup();
    }

    public Metapb.StoreGroup getStoreGroup(int groupId) throws PDException {
        StoreGroup.GetStoreGroupRequest request = StoreGroup.GetStoreGroupRequest.newBuilder()
                .setHeader(header)
                .setGroupId(groupId)
                .build();
        StoreGroup.GetStoreGroupResponse response = blockingUnaryCall(PDGrpc.getGetStoreGroupMethod(), request);
        handleResponseError(response.getHeader());
        return response.getStoreGroup();
    }

    public List<Metapb.StoreGroup> getAllStoreGroups() throws PDException {
        StoreGroup.GetAllStoreGroupRequest request = StoreGroup.GetAllStoreGroupRequest.newBuilder()
                .setHeader(header).build();
        StoreGroup.GetAllStoreGroupResponse response = blockingUnaryCall(PDGrpc.getGetAllStoreGroupMethod(), request);
        handleResponseError(response.getHeader());
        return response.getStoreGroupsList();
    }

    public Metapb.StoreGroup updateStoreGroup(int groupId, String name) throws PDException {
        StoreGroup.UpdateStoreGroupRequest request = StoreGroup.UpdateStoreGroupRequest.newBuilder().setHeader(header)
                .setGroupId(groupId)
                .setName(name)
                .build();
        StoreGroup.UpdateStoreGroupResponse response = blockingUnaryCall(PDGrpc.getUpdateStoreGroupMethod(), request);
        handleResponseError(response.getHeader());
        return response.getStoreGroup();
    }

    public List<Metapb.Store> getStoresByStoreGroup(int groupId) throws PDException {
        StoreGroup.GetGroupStoresRequest request = StoreGroup.GetGroupStoresRequest.newBuilder()
                .setHeader(header).setStoreGroupId(groupId).build();
        StoreGroup.GetGroupStoresResponse response = blockingUnaryCall(PDGrpc.getGetStoresByStoreGroupMethod(), request);
        handleResponseError(response.getHeader());
        return response.getStoresList();
    }

    public boolean updateStoreGroupRelation(long storeId, int groupId) throws PDException {
        StoreGroup.UpdateStoreGroupRelationRequest request = StoreGroup.UpdateStoreGroupRelationRequest.newBuilder()
                .setHeader(header).setStoreId(storeId)
                .setStoreGroupId(groupId).build();
        var response = blockingUnaryCall(PDGrpc.getUpdateStoreGroupRelationMethod(), request);
        handleResponseError(response.getHeader());
        return response.getSuccess();
    }

>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/impl/PDApi.java
}
