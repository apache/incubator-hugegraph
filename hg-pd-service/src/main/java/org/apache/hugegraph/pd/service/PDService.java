<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
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

package org.apache.hugegraph.pd.service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.IdService;
import org.apache.hugegraph.pd.LogService;
import org.apache.hugegraph.pd.PartitionInstructionListener;
import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.PartitionStatusListener;
import org.apache.hugegraph.pd.ShardGroupStatusListener;
import org.apache.hugegraph.pd.StoreMonitorDataService;
import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.StoreStatusListener;
import org.apache.hugegraph.pd.TaskScheduleService;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.Pdpb.CachePartitionResponse;
import org.apache.hugegraph.pd.grpc.Pdpb.CacheResponse;
import org.apache.hugegraph.pd.grpc.Pdpb.GetGraphRequest;
import org.apache.hugegraph.pd.grpc.Pdpb.PutLicenseRequest;
import org.apache.hugegraph.pd.grpc.Pdpb.PutLicenseResponse;
import org.apache.hugegraph.pd.grpc.pulse.ChangeShard;
import org.apache.hugegraph.pd.grpc.pulse.CleanPartition;
import org.apache.hugegraph.pd.grpc.pulse.DbCompaction;
import org.apache.hugegraph.pd.grpc.pulse.MovePartition;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import org.apache.hugegraph.pd.grpc.pulse.PartitionKeyRange;
import org.apache.hugegraph.pd.grpc.pulse.SplitPartition;
import org.apache.hugegraph.pd.grpc.pulse.TransferLeader;
import org.apache.hugegraph.pd.grpc.watch.NodeEventType;
import org.apache.hugegraph.pd.grpc.watch.WatchGraphResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchResponse;
import org.apache.hugegraph.pd.grpc.watch.WatchType;
import org.apache.hugegraph.pd.pulse.PDPulseSubject;
import org.apache.hugegraph.pd.pulse.PulseListener;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.raft.RaftStateListener;
import org.apache.hugegraph.pd.util.grpc.StreamObserverUtil;
import org.apache.hugegraph.pd.watch.PDWatchSubject;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
========
package org.apache.hugegraph.pd.service;

import static org.apache.hugegraph.pd.grpc.common.ErrorType.GRAPH_NOT_EXISTS;
import static org.apache.hugegraph.pd.grpc.common.ErrorType.PARTITION_NOT_EXISTS;
import static org.apache.hugegraph.pd.grpc.common.ErrorType.STORE_GROUP_NOT_EXISTS;
import static org.apache.hugegraph.pd.grpc.common.ErrorType.TASK_NOT_EXISTS;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.util.CollectionUtils;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.IdService;
import org.apache.hugegraph.pd.LogService;
import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.StoreMonitorDataService;
import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.TaskScheduleService;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.ClusterOp;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Metapb.GraphStats;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.Pdpb.CachePartitionResponse;
import org.apache.hugegraph.pd.grpc.Pdpb.CacheResponse;
import org.apache.hugegraph.pd.grpc.Pdpb.GetGraphRequest;
import org.apache.hugegraph.pd.grpc.Pdpb.GetLeaderGrpcAddressResponse;
import org.apache.hugegraph.pd.grpc.Pdpb.GraphStatsResponse;
import org.apache.hugegraph.pd.grpc.Pdpb.PutLicenseRequest;
import org.apache.hugegraph.pd.grpc.Pdpb.PutLicenseResponse;
import org.apache.hugegraph.pd.grpc.StoreGroup;
import org.apache.hugegraph.pd.grpc.common.ErrorType;
import org.apache.hugegraph.pd.grpc.common.Errors;
import org.apache.hugegraph.pd.grpc.common.NoArg;
import org.apache.hugegraph.pd.grpc.common.ResponseHeader;
import org.apache.hugegraph.pd.grpc.common.VoidResponse;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import org.apache.hugegraph.pd.grpc.pulse.SplitPartition;
import org.apache.hugegraph.pd.grpc.pulse.StoreNodeEventType;
import org.apache.hugegraph.pd.license.LicenseVerifierService;
import org.apache.hugegraph.pd.pulse.PDPulseSubjects;
import org.apache.hugegraph.pd.pulse.impl.PartitionInstructionListenerImpl;
import org.apache.hugegraph.pd.pulse.impl.PartitionStatusListenerImpl;
import org.apache.hugegraph.pd.pulse.impl.PulseListenerImpl;
import org.apache.hugegraph.pd.pulse.impl.ShardGroupStatusListenerImpl;
import org.apache.hugegraph.pd.pulse.impl.StoreStatusListenerImpl;
import org.apache.hugegraph.pd.raft.PeerUtil;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.service.interceptor.GrpcAuthentication;
import org.apache.hugegraph.pd.util.grpc.StreamObserverUtil;

import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

// TODO: uncomment later - remove license verifier service now
@Slf4j
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
@GRpcService
public class PDService extends PDGrpc.PDImplBase implements ServiceGrpc, RaftStateListener {

    static String TASK_ID_KEY = "task_id";
    private final Pdpb.ResponseHeader okHeader = Pdpb.ResponseHeader.newBuilder().setError(
            Pdpb.Error.newBuilder().setType(Pdpb.ErrorType.OK)).build();
    // private ManagedChannel channel;
    private final Map<String, ManagedChannel> channelMap = new ConcurrentHashMap<>();
    @Autowired
    private PDConfig pdConfig;
========
@GRpcService(interceptors = {GrpcAuthentication.class})
@DependsOn("pdPulseService")
public class PDService extends PDGrpc.PDImplBase implements ServiceGrpc {

    public static final String TASK_ID_KEY = "task_id";
    private static final String USER_TASK_ID_KEY = "user_task_key";
    @Autowired
    private PDConfig pdConfig;
    @Getter
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
    private StoreNodeService storeNodeService;
    @Getter
    private PartitionService partitionService;
    @Getter
    private TaskScheduleService taskService;
    @Getter
    private IdService idService;
    @Getter
    private ConfigService configService;
    @Getter
    private LogService logService;
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
    //private LicenseVerifierService licenseVerifierService;
    private StoreMonitorDataService storeMonitorDataService;
    private ManagedChannel channel;

    private Pdpb.ResponseHeader newErrorHeader(int errorCode, String errorMsg) {
        Pdpb.ResponseHeader header = Pdpb.ResponseHeader.newBuilder().setError(
                Pdpb.Error.newBuilder().setTypeValue(errorCode).setMessage(errorMsg)).build();
        return header;
    }

    private Pdpb.ResponseHeader newErrorHeader(PDException e) {
        Pdpb.ResponseHeader header = Pdpb.ResponseHeader.newBuilder().setError(
                                                 Pdpb.Error.newBuilder().setTypeValue(e.getErrorCode()).setMessage(e.getMessage()))
                                                        .build();
        return header;
    }

    public StoreNodeService getStoreNodeService() {
        return storeNodeService;
    }

    public PartitionService getPartitionService() {
        return partitionService;
    }

    public TaskScheduleService getTaskService() {
        return taskService;
    }

    public ConfigService getConfigService() {
        return configService;
    }

    public StoreMonitorDataService getStoreMonitorDataService() {
        return this.storeMonitorDataService;
    }

    public LogService getLogService() {
        return logService;
    }

    //public LicenseVerifierService getLicenseVerifierService() {
    //    return licenseVerifierService;
    //}
========
    @Getter
    private LicenseVerifierService licenseVerifierService;
    @Getter
    private StoreMonitorDataService storeMonitorDataService;
    private ResponseHeader okHeader = getResponseHeader();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java

    /**
     * initialize
     */
    @PostConstruct
    public void init() throws PDException {
        log.info("PDService init……{}", pdConfig);
        configService = new ConfigService(pdConfig);
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java

        RaftEngine.getInstance().addStateListener(this);
        RaftEngine.getInstance().addStateListener(configService);
        RaftEngine.getInstance().init(pdConfig.getRaft());
        //pdConfig = configService.loadConfig(); onLeaderChanged
========
        RaftEngine engine = RaftEngine.getInstance();
        engine.addStateListener(this);
        engine.addStateListener(configService);
        engine.init(pdConfig.getRaft());
        // pdConfig = configService.loadConfig(); onLeaderChanged中加载
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        storeNodeService = new StoreNodeService(pdConfig);
        partitionService = new PartitionService(pdConfig, storeNodeService, configService);
        taskService = new TaskScheduleService(pdConfig, storeNodeService, partitionService, configService);
        idService = new IdService(pdConfig);
        logService = new LogService(pdConfig);
        storeMonitorDataService = new StoreMonitorDataService(pdConfig);
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        //if (licenseVerifierService == null) {
        //    licenseVerifierService = new LicenseVerifierService(pdConfig);
        //}
        RaftEngine.getInstance().addStateListener(partitionService);
        pdConfig.setIdService(idService);

        // Receive a heartbeat message
        PDPulseSubject.listenPartitionHeartbeat(new PulseListener<PartitionHeartbeatRequest>() {
            @Override
            public void onNext(PartitionHeartbeatRequest request) throws Exception {
                partitionService.partitionHeartbeat(request.getStates());
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("Received an error notice from pd-client", throwable);
            }

            @Override
            public void onCompleted() {
                log.info("Received an completed notice from pd-client");
            }
        });

        /**
         // Listen for partition commands and forward them to Store
         */
        partitionService.addInstructionListener(new PartitionInstructionListener() {
            private PartitionHeartbeatResponse.Builder getBuilder(Metapb.Partition partition) throws
                                                                                              PDException {
                return PartitionHeartbeatResponse.newBuilder().setPartition(partition)
                                                 .setId(idService.getId(TASK_ID_KEY, 1));
            }

            @Override
            public void changeShard(Metapb.Partition partition, ChangeShard changeShard) throws
                                                                                         PDException {
                PDPulseSubject.notifyClient(getBuilder(partition).setChangeShard(changeShard));

            }

            @Override
            public void transferLeader(Metapb.Partition partition,
                                       TransferLeader transferLeader) throws
                                                                      PDException {
                PDPulseSubject.notifyClient(
                        getBuilder(partition).setTransferLeader(transferLeader));
            }

            @Override
            public void splitPartition(Metapb.Partition partition,
                                       SplitPartition splitPartition) throws
                                                                      PDException {
                PDPulseSubject.notifyClient(
                        getBuilder(partition).setSplitPartition(splitPartition));

            }

            @Override
            public void dbCompaction(Metapb.Partition partition, DbCompaction dbCompaction) throws
                                                                                            PDException {
                PDPulseSubject.notifyClient(getBuilder(partition).setDbCompaction(dbCompaction));

            }

            @Override
            public void movePartition(Metapb.Partition partition,
                                      MovePartition movePartition) throws PDException {
                PDPulseSubject.notifyClient(getBuilder(partition).setMovePartition(movePartition));
            }

            @Override
            public void cleanPartition(Metapb.Partition partition,
                                       CleanPartition cleanPartition) throws PDException {
                PDPulseSubject.notifyClient(
                        getBuilder(partition).setCleanPartition(cleanPartition));
            }

            @Override
            public void changePartitionKeyRange(Metapb.Partition partition,
                                                PartitionKeyRange partitionKeyRange)
                    throws PDException {
                PDPulseSubject.notifyClient(getBuilder(partition).setKeyRange(partitionKeyRange));
            }
        });

        /**
         // Listen for partition status change messages and forward them to Client
         */
        partitionService.addStatusListener(new PartitionStatusListener() {
            @Override
            public void onPartitionChanged(Metapb.Partition old, Metapb.Partition partition) {
                PDWatchSubject.notifyPartitionChange(PDWatchSubject.ChangeType.ALTER,
                                                     partition.getGraphName(), partition.getId());
            }

            @Override
            public void onPartitionRemoved(Metapb.Partition partition) {
                PDWatchSubject.notifyPartitionChange(PDWatchSubject.ChangeType.DEL,
                                                     partition.getGraphName(),
                                                     partition.getId());

            }
        });

        storeNodeService.addShardGroupStatusListener(new ShardGroupStatusListener() {
            @Override
            public void onShardListChanged(Metapb.ShardGroup shardGroup,
                                           Metapb.ShardGroup newShardGroup) {
                // invoked before change, saved to db and update cache.
                if (newShardGroup == null) {
                    PDWatchSubject.notifyShardGroupChange(PDWatchSubject.ChangeType.DEL,
                                                          shardGroup.getId(),
                                                          shardGroup);
                } else {
                    PDWatchSubject.notifyShardGroupChange(PDWatchSubject.ChangeType.ALTER,
                                                          shardGroup.getId(), newShardGroup);
                }
            }

            @Override
            public void onShardListOp(Metapb.ShardGroup shardGroup) {
                PDWatchSubject.notifyShardGroupChange(PDWatchSubject.ChangeType.USER_DEFINED,
                                                      shardGroup.getId(), shardGroup);
            }
        });

        /**
         // Listen for store status change messages and forward them to Client
         */
        storeNodeService.addStatusListener(new StoreStatusListener() {

            @Override
            public void onStoreStatusChanged(Metapb.Store store,
                                             Metapb.StoreState old,
                                             Metapb.StoreState status) {
                NodeEventType type = NodeEventType.NODE_EVENT_TYPE_UNKNOWN;
                if (status == Metapb.StoreState.Up) {
                    type = NodeEventType.NODE_EVENT_TYPE_NODE_ONLINE;
                } else if (status == Metapb.StoreState.Offline) {
                    type = NodeEventType.NODE_EVENT_TYPE_NODE_OFFLINE;
                }
                PDWatchSubject.notifyNodeChange(type, "", store.getId());
            }

            @Override
            public void onGraphChange(Metapb.Graph graph,
                                      Metapb.GraphState stateOld,
                                      Metapb.GraphState stateNew) {
                WatchGraphResponse wgr = WatchGraphResponse.newBuilder()
                                                           .setGraph(graph)
                                                           .build();
                WatchResponse.Builder wr = WatchResponse.newBuilder()
                                                        .setGraphResponse(wgr);
                PDWatchSubject.notifyChange(WatchType.WATCH_TYPE_GRAPH_CHANGE,
                                            wr);
            }

            @Override
            public void onStoreRaftChanged(Metapb.Store store) {
                PDWatchSubject.notifyNodeChange(NodeEventType.NODE_EVENT_TYPE_NODE_RAFT_CHANGE, "",
                                                store.getId());
            }
        });
========
        if (licenseVerifierService == null) {
            licenseVerifierService = new LicenseVerifierService(pdConfig);
        }
        engine.addStateListener(partitionService);
        pdConfig.setIdService(idService);
        // 接收心跳消息
        PDPulseSubjects.listenPartitionHeartbeat(new PulseListenerImpl(this));
        // 处理心跳Listener异常，返回0，不中断其他Listener。
//        PDPulseSubjects.setPartitionErrInterceptor(
//                (req,e) -> {
//                    if (e instanceof PDException) {
//                        var pde = (PDException) e;
//                        if (pde.getErrorCode() == NOT_LEADER.getNumber()) {
//                            try {
//                                log.info("send change leader command to watch, due to ERROR-100", pde);
//                                PDPulseSubjects.notifyClient(PdInstructionResponse.newBuilder()
//                                        .setInstructionType(PdInstructionType.CHANGE_TO_FOLLOWER)
//                                        .setLeaderIp(engine.getLeaderGrpcAddress())
//                                        .build());
//                            } catch (Exception ex) {
//                                log.error("send notice to observer failed, ", ex);
//                            }
//                            return 1; // Aborting other listeners.
//                        }
//                    } else {
//                        log.error("handleNotice error", e);
//                    }
//                    return 0;
//                }
//        );
        // 监听分区指令，并转发给Store
        partitionService.addInstructionListener(new PartitionInstructionListenerImpl(this));
        // 监听分区状态改变消息，并转发给Client
        partitionService.addStatusListener(new PartitionStatusListenerImpl());
        storeNodeService.addShardGroupStatusListener(new ShardGroupStatusListenerImpl());
        // 监听store状态改变消息，并转发给Client
        storeNodeService.addStatusListener(new StoreStatusListenerImpl());
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        storeNodeService.init(partitionService);
        partitionService.init();
        taskService.init();
    }

    /**
     * <pre>
     * Register a store, and the first registration generates a new store_id, store_id is the unique identifier of the store
     * </pre>
     */
    @Override
    public void registerStore(Pdpb.RegisterStoreRequest request,
                              io.grpc.stub.StreamObserver<Pdpb.RegisterStoreResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getRegisterStoreMethod(), request, observer);
            return;
        }
        Pdpb.RegisterStoreResponse response = null;
        try {
            Metapb.Store store = storeNodeService.register(request.getStore());
            response = Pdpb.RegisterStoreResponse.newBuilder().setHeader(okHeader)
                                                 .setStoreId(store.getId())
                                                 .build();
        } catch (PDException e) {
            response = Pdpb.RegisterStoreResponse.newBuilder().setHeader(getResponseHeader(e)).build();
            log.error("registerStore exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();

    }

    /**
     * Find the store based on store_id
     */
    @Override
    public void getStore(Pdpb.GetStoreRequest request,
                         io.grpc.stub.StreamObserver<Pdpb.GetStoreResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetStoreMethod(), request, observer);
            return;
        }
        Pdpb.GetStoreResponse response = null;
        try {
            Metapb.Store store = storeNodeService.getStore(request.getStoreId());
            response =
                    Pdpb.GetStoreResponse.newBuilder().setHeader(okHeader).setStore(store).build();
        } catch (PDException e) {
            response = Pdpb.GetStoreResponse.newBuilder().setHeader(getResponseHeader(e)).build();
            log.error("{} getStore exception: {}", StreamObserverUtil.getRemoteIP(observer), e);
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * <pre>
     * Modify information such as the status of the store.
     * </pre>
     */
    @Override
    public void setStore(Pdpb.SetStoreRequest request,
                         StreamObserver<Pdpb.SetStoreResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getSetStoreMethod(), request, observer);
            return;
        }
        Pdpb.SetStoreResponse response = null;
        try {
            Metapb.StoreState state = request.getStore().getState();
            Long storeId = request.getStore().getId();
            // In the Pending state, you can go online
            Metapb.Store lastStore = storeNodeService.getStore(request.getStore().getId());
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            if (lastStore == null) {
                // storeId does not exist, an exception is thrown
                throw new PDException(Pdpb.ErrorType.STORE_ID_NOT_EXIST_VALUE,
                                      String.format("Store id %d does not exist!", storeId));
            }
            if (Metapb.StoreState.Up.equals(state)) {
                if (!Metapb.StoreState.Pending.equals(lastStore.getState())) {
                    throw new PDException(Pdpb.ErrorType.UPDATE_STORE_STATE_ERROR_VALUE,
                                          "only stores in Pending state can be set to Up!");
========
            if (lastStore == null){
                // storeId不存在，抛出异常
                throw new PDException(ErrorType.STORE_ID_NOT_EXIST_VALUE,
                        String.format("Store id %d does not exist!", storeId));
            }
            if (Metapb.StoreState.Up.equals(state)){
                if (!Metapb.StoreState.Pending.equals(lastStore.getState())){
                    throw new PDException(ErrorType.UPDATE_STORE_STATE_ERROR_VALUE,
                            "only stores in Pending state can be set to Up!");
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                }
            }
            if (state.equals(Metapb.StoreState.Offline)) {
                Metapb.ClusterStats stats = storeNodeService.getClusterStats(storeId);
                if (stats.getState() != Metapb.ClusterState.Cluster_OK) {
                    ResponseHeader errorHeader = getResponseHeader(-1,
                                                                     "can not offline node "
                                                                     +
                                                                     "when cluster state is not " +
                                                                     "normal ");
                    response = Pdpb.SetStoreResponse.newBuilder().setHeader(errorHeader).build();
                    observer.onNext(response);
                    observer.onCompleted();
                    return;
                }
            }
            logService.insertLog(LogService.NODE_CHANGE, LogService.GRPC, request.getStore());
            // If the check fails, the status will be changed to Pending, and the reason for the
            // error will be returned
            if (state.equals(Metapb.StoreState.Up)) {
                int cores = 0;
                long id = request.getStore().getId();
                List<Metapb.Store> stores = storeNodeService.getStores();
                int nodeCount = 0;
                for (Metapb.Store store : stores) {
                    if (store.getId() == id) {
                        // Get the cores from the previously registered store as a validation
                        // parameter
                        cores = store.getCores();
                    }
                    if (store.getState().equals(Metapb.StoreState.Up)) {
                        nodeCount++;
                    }
                }
                try {
                    //licenseVerifierService.verify(cores, nodeCount);
                } catch (Exception e) {
                    Metapb.Store store = Metapb.Store.newBuilder(request.getStore())
                                                     .setState(Metapb.StoreState.Pending).build();
                    storeNodeService.updateStore(store);
                    throw new PDException(ErrorType.LICENSE_ERROR_VALUE,
                                          "check license with error :"
                                          + e.getMessage()
                                          + ", and changed node state to 'Pending'");
                }
            }
            Metapb.Store store = request.getStore();
            // Before going offline, check whether the number of active machines is greater than
            // the minimum threshold
            if (state.equals(Metapb.StoreState.Tombstone)) {
                List<Metapb.Store> activeStores = storeNodeService.getActiveStores();
                if (lastStore.getState() == Metapb.StoreState.Up
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                    && activeStores.size() - 1 < pdConfig.getMinStoreCount()) {
                    throw new PDException(Pdpb.ErrorType.LESS_ACTIVE_STORE_VALUE,
                                          "The number of active stores is less then " +
                                          pdConfig.getMinStoreCount());
                }
                if (!storeNodeService.checkStoreCanOffline(request.getStore())) {
                    throw new PDException(Pdpb.ErrorType.LESS_ACTIVE_STORE_VALUE,
                                          "check activeStores or online shardsList size");
                }
                if (lastStore.getState() == Metapb.StoreState.Exiting) {
                    // If it is already in the offline state, no further processing will be made
                    throw new PDException(Pdpb.ErrorType.Store_Tombstone_Doing_VALUE,
                                          "Downline is in progress, do not resubmit");
========
                        && activeStores.size() - 1 < pdConfig.getMinStoreCount()) {
                    throw new PDException(ErrorType.LESS_ACTIVE_STORE_VALUE,
                            "The number of active stores is less then " + pdConfig.getMinStoreCount());
                }
                if (!storeNodeService.checkStoreCanOffline(request.getStore())){
                    throw new PDException(ErrorType.LESS_ACTIVE_STORE_VALUE,
                            "check activeStores or online shardsList size");
                }
                if (lastStore.getState() == Metapb.StoreState.Exiting){
                    // 如果已经是下线中的状态，则不作进一步处理
                    throw new PDException(ErrorType.Store_Tombstone_Doing_VALUE,
                            "Downline is in progress, do not resubmit");
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                }
                Map<String, Object> resultMap = taskService.canAllPartitionsMovedOut(lastStore);
                if ((boolean) resultMap.get("flag")) {
                    if (resultMap.get("current_store_is_online") != null
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                        && (boolean) resultMap.get("current_store_is_online")) {
========
                            && (boolean) resultMap.get("current_store_is_online")) {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                        log.info("updateStore removeActiveStores store {}", store.getId());
                        // Set the status of the online store to Offline and wait for the replica
                        // to be migrated
                        store = Metapb.Store.newBuilder(lastStore)
                                            .setState(Metapb.StoreState.Exiting).build();
                        // Perform partition migration operations
                        taskService.movePartitions((Map<Integer, KVPair<Long, Long>>) resultMap.get(
                                "movedPartitions"));
                    } else {
                        // If the store is offline, the replica is not migrated
                        // Change the status to Tombstone
                    }
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                } else {
                    throw new PDException(Pdpb.ErrorType.UPDATE_STORE_STATE_ERROR_VALUE,
                                          "the resources on other stores may be not enough to " +
                                          "store " +
                                          "the partitions of current store!");
========
                }else{
                    throw new PDException(ErrorType.UPDATE_STORE_STATE_ERROR_VALUE,
                            "the resources on other stores may be not enough to store " +
                                    "the partitions of current store!");
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                }
            }
            store = storeNodeService.updateStore(store);
            response =
                    Pdpb.SetStoreResponse.newBuilder().setHeader(okHeader).setStore(store).build();
        } catch (PDException e) {
            response = Pdpb.SetStoreResponse.newBuilder().setHeader(getResponseHeader(e)).build();
            log.error("setStore exception: ", e);
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getAllStores(Pdpb.GetAllStoresRequest request,
                             io.grpc.stub.StreamObserver<Pdpb.GetAllStoresResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetAllStoresMethod(), request, observer);
            return;
        }
        Pdpb.GetAllStoresResponse response = null;
        try {
            List<Metapb.Store> stores = null;
            if (request.getExcludeOfflineStores()) {
                if (! request.getGraphName().isEmpty()) {
                    var graph = partitionService.getGraph(request.getGraphName());
                    stores = storeNodeService.getActiveStoresByStoreGroup(graph.getStoreGroupId());
                } else {
                    stores = storeNodeService.getActiveStores();
                }
            } else {
                stores = storeNodeService.getStores(request.getGraphName());
            }
            response =
                    Pdpb.GetAllStoresResponse.newBuilder().setHeader(okHeader).addAllStores(stores)
                                             .build();
        } catch (PDException e) {
            response = Pdpb.GetAllStoresResponse.newBuilder().setHeader(getResponseHeader(e)).build();
            log.error("getAllStores exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * Handle store heartbeats
     */
    @Override
    public void storeHeartbeat(Pdpb.StoreHeartbeatRequest request,
                               io.grpc.stub.StreamObserver<Pdpb.StoreHeartbeatResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getStoreHeartbeatMethod(), request, observer);
            return;
        }

        Metapb.StoreStats stats = request.getStats();

        // save monitor data when monitor data enabled
        if (this.pdConfig.getStore().isMonitorDataEnabled()) {
            try {
                storeMonitorDataService.saveMonitorData(stats);
            } catch (PDException e) {
                log.error("save status failed, state:{}", stats);
            }
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            // remove system_metrics
            stats = Metapb.StoreStats.newBuilder()
                                     .mergeFrom(request.getStats())
                                     .clearField(Metapb.StoreStats.getDescriptor().findFieldByName(
                                             "system_metrics"))
                                     .build();
========
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        }

        // remove system_metrics
        stats = Metapb.StoreStats.newBuilder()
                .mergeFrom(request.getStats())
                .clearSystemMetrics()
                .build();

        Pdpb.StoreHeartbeatResponse response;
        try {
            Metapb.ClusterStats clusterStats = storeNodeService.heartBeat(stats);
            Pdpb.StoreHeartbeatResponse.Builder builder =
                    Pdpb.StoreHeartbeatResponse.newBuilder().setHeader(okHeader);
            if (clusterStats != null) {
                builder.setClusterStats(clusterStats);
            }
            response = builder.build();
        } catch (PDException e) {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            response =
                    Pdpb.StoreHeartbeatResponse.newBuilder().setHeader(newErrorHeader(e)).build();
========
            response = Pdpb.StoreHeartbeatResponse.newBuilder().setHeader(getResponseHeader(e)).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            log.error("storeHeartbeat exception: ", e);
        } catch (Exception e2) {
            response = Pdpb.StoreHeartbeatResponse.newBuilder().setHeader(
                    getResponseHeader(ErrorType.UNKNOWN_VALUE, e2.getMessage())).build();
            log.error("storeHeartbeat exception: ", e2);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * <pre>
     * Find the partition to which the key belongs
     * </pre>
     */
    @Override
    public void getPartition(Pdpb.GetPartitionRequest request,
                             io.grpc.stub.StreamObserver<Pdpb.GetPartitionResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetPartitionMethod(), request, observer);
            return;
        }
        Pdpb.GetPartitionResponse response = null;
        try {
            Metapb.PartitionShard partShard =
                    partitionService.getPartitionShard(request.getGraphName(),
                                                       request.getKey()
                                                              .toByteArray());
            response = Pdpb.GetPartitionResponse.newBuilder().setHeader(okHeader)
                                                .setPartition(partShard.getPartition())
                                                .setLeader(partShard.getLeader()).build();
        } catch (PDException e) {
            response = Pdpb.GetPartitionResponse.newBuilder().setHeader(getResponseHeader(e)).build();
            log.error("getPartition exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * <pre>
     * Find the partition to which the HashCode belongs
     * </pre>
     */
    @Override
    public void getPartitionByCode(Pdpb.GetPartitionByCodeRequest request,
                                   io.grpc.stub.StreamObserver<Pdpb.GetPartitionResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetPartitionByCodeMethod(), request, observer);
            return;
        }
        Pdpb.GetPartitionResponse response = null;
        try {
            Metapb.PartitionShard partShard =
                    partitionService.getPartitionByCode(request.getGraphName(),
                                                        request.getCode());
            response = Pdpb.GetPartitionResponse.newBuilder().setHeader(okHeader)
                                                .setPartition(partShard.getPartition())
                                                .setLeader(partShard.getLeader()).build();
        } catch (PDException e) {
            response = Pdpb.GetPartitionResponse.newBuilder().setHeader(getResponseHeader(e)).build();
            log.error("getPartitionByCode exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * Find partition based on partition_id
     */
    @Override
    public void getPartitionByID(Pdpb.GetPartitionByIDRequest request,
                                 io.grpc.stub.StreamObserver<Pdpb.GetPartitionResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetPartitionByIDMethod(), request, observer);
            return;
        }
        Pdpb.GetPartitionResponse response = null;
        try {
            Metapb.PartitionShard partShard =
                    partitionService.getPartitionShardById(request.getGraphName(),
                                                           request.getPartitionId());
            if (partShard == null) {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                throw new PDException(Pdpb.ErrorType.NOT_FOUND_VALUE,
                                      String.format("partition: %s-%s not found",
                                                    request.getGraphName(),
                                                    request.getPartitionId()));
========
                throw new PDException(ErrorType.NOT_FOUND_VALUE,
                        String.format("partition: %s-%s not found", request.getGraphName(), request.getPartitionId()));
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            }
            response = Pdpb.GetPartitionResponse.newBuilder().setHeader(okHeader)
                                                .setPartition(partShard.getPartition())
                                                .setLeader(partShard.getLeader()).build();
        } catch (PDException e) {
            response = Pdpb.GetPartitionResponse.newBuilder().setHeader(getResponseHeader(e)).build();
            log.error("getPartitionByID exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * <pre>
     * Update partition information, mainly used to update the partition key range, call this API with caution, otherwise it will cause data loss.
     * </pre>
     */
    @Override
    public void updatePartition(Pdpb.UpdatePartitionRequest request,
                                io.grpc.stub.StreamObserver<Pdpb.UpdatePartitionResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getUpdatePartitionMethod(), request, observer);
            return;
        }
        Pdpb.UpdatePartitionResponse response = null;
        try {
            partitionService.updatePartition(request.getPartitionList());
            response = Pdpb.UpdatePartitionResponse.newBuilder().setHeader(okHeader).build();

        } catch (PDException e) {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            response =
                    Pdpb.UpdatePartitionResponse.newBuilder().setHeader(newErrorHeader(e)).build();
========
            response = Pdpb.UpdatePartitionResponse.newBuilder().setHeader(getResponseHeader(e)).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            log.error("update partition exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * Find partition based on partition_id
     */
    @Override
    public void delPartition(Pdpb.DelPartitionRequest request,
                             io.grpc.stub.StreamObserver<Pdpb.DelPartitionResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getDelPartitionMethod(), request, observer);
            return;
        }
        Pdpb.DelPartitionResponse response = null;
        try {
            Metapb.Partition partition = partitionService.getPartitionById(request.getGraphName(),
                                                                           request.getPartitionId());
            if (partition != null) {
                partitionService.removePartition(request.getGraphName(),
                                                 request.getPartitionId());
                response = Pdpb.DelPartitionResponse.newBuilder().setHeader(okHeader)
                                                    .setPartition(partition)
                                                    .build();
            } else {
                response = Pdpb.DelPartitionResponse.newBuilder().setHeader(okHeader).build();
            }
        } catch (PDException e) {
            response = Pdpb.DelPartitionResponse.newBuilder().setHeader(getResponseHeader(e)).build();
            log.error("delPartition exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * The set of partitions to which a given key range looks
     */
    @Override
    public void scanPartitions(Pdpb.ScanPartitionsRequest request,
                               io.grpc.stub.StreamObserver<Pdpb.ScanPartitionsResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getScanPartitionsMethod(), request, observer);
            return;
        }
        Pdpb.ScanPartitionsResponse response = null;
        try {
            List<Metapb.PartitionShard> partShards =
                    partitionService.scanPartitions(request.getGraphName(),
                                                    request.getStartKey()
                                                           .toByteArray(),
                                                    request.getEndKey()
                                                           .toByteArray());
            response = Pdpb.ScanPartitionsResponse.newBuilder().setHeader(okHeader)
                                                  .addAllPartitions(partShards).build();
        } catch (PDException e) {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            response =
                    Pdpb.ScanPartitionsResponse.newBuilder().setHeader(newErrorHeader(e)).build();
========
            response = Pdpb.ScanPartitionsResponse.newBuilder().setHeader(getResponseHeader(e)).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            log.error("scanPartitions exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * Get graph information
     */
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
    @Override
========
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
    public void getGraph(GetGraphRequest request,
                         io.grpc.stub.StreamObserver<Pdpb.GetGraphResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetGraphMethod(), request, observer);
            return;
        }

        Pdpb.GetGraphResponse response = null;
        String graphName = request.getGraphName();
        try {
            Metapb.Graph graph = partitionService.getGraph(graphName);
            if (graph != null) {
                response = Pdpb.GetGraphResponse.newBuilder().setHeader(okHeader).setGraph(graph)
                                                .build();
            } else {
                ResponseHeader header = ResponseHeader.newBuilder().setError(
                        Errors.newBuilder().setType(ErrorType.NOT_FOUND).build()).build();
                response = Pdpb.GetGraphResponse.newBuilder().setHeader(header).build();
            }
        } catch (PDException e) {
            response = Pdpb.GetGraphResponse.newBuilder().setHeader(getResponseHeader(e)).build();
            log.error("getGraph exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * Modify the diagram information
     */
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
    @Override
    public void setGraph(Pdpb.SetGraphRequest request,
                         io.grpc.stub.StreamObserver<Pdpb.SetGraphResponse> observer) {
========
    public void setGraph(Pdpb.CreateGraphRequest request,
                         io.grpc.stub.StreamObserver<Pdpb.CreateGraphResponse> observer) {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getSetGraphMethod(), request, observer);
            return;
        }
        Pdpb.CreateGraphResponse response;
        Metapb.Graph graph = request.getGraph();
        try {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            graph = partitionService.updateGraph(graph);
            response =
                    Pdpb.SetGraphResponse.newBuilder().setHeader(okHeader).setGraph(graph).build();
========
            var lastGraph = partitionService.getGraph(graph.getGraphName());
            if (lastGraph != null) {
                graph = partitionService.updateGraphName(graph);
            } else {
                graph = partitionService.createGraph(graph.getGraphName(),
                        graph.getPartitionCount(), graph.getStoreGroupId());
            }

            response = Pdpb.CreateGraphResponse.newBuilder().setHeader(okHeader).setGraph(graph).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        } catch (PDException e) {
            log.error("setGraph exception: ", e);
            response = Pdpb.CreateGraphResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * Get graph information
     */
    @Override
    public void delGraph(Pdpb.DelGraphRequest request,
                         io.grpc.stub.StreamObserver<Pdpb.DelGraphResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getDelGraphMethod(), request, observer);
            return;
        }

        Pdpb.DelGraphResponse response = null;
        String graphName = request.getGraphName();
        try {
            Metapb.Graph graph = partitionService.delGraph(graphName);
            if (graph != null) {
                response = Pdpb.DelGraphResponse.newBuilder().setHeader(okHeader).setGraph(graph)
                                                .build();
            }
        } catch (PDException e) {
            response = Pdpb.DelGraphResponse.newBuilder().setHeader(getResponseHeader(e)).build();
            log.error("getGraph exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * <pre>
     * Query partition information based on conditions, such as Store and Graph
     * </pre>
     */
    @Override
    public void queryPartitions(Pdpb.QueryPartitionsRequest request,
                                io.grpc.stub.StreamObserver<Pdpb.QueryPartitionsResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getQueryPartitionsMethod(), request, observer);
            return;
        }
        // The traversal scheme is used temporarily, and when the rocksdb storage is used in
        // the future, it is implemented through KV indexes
        Metapb.PartitionQuery query = request.getQuery();
        List<Metapb.Partition> partitions = partitionService.getPartitions(query.getGraphName());
        List<Metapb.Partition> result = new ArrayList<>();
        if (!CollectionUtils.isEmpty(partitions)) {
            for (Metapb.Partition partition : partitions) {
                if (query.hasPartitionId() && partition.getId() != query.getPartitionId()) {
                    continue;
                }
                if (query.hasGraphName() &&
                    !partition.getGraphName().equals(query.getGraphName())) {
                    continue;
                }
                long storeId = query.getStoreId();
                if (query.hasStoreId() && query.getStoreId() != 0) {
                    try {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                        storeNodeService.getShardGroup(partition.getId()).getShardsList()
                                        .forEach(shard -> {
                                            if (shard.getStoreId() == storeId) {
                                                result.add(partition);
                                            }
                                        });
                    } catch (PDException e) {
                        log.error("query partitions error, req:{}, error:{}", request,
                                  e.getMessage());
========
                        var shardGroup = storeNodeService.getShardGroup(partition.getId());
                        // 清理的时候，可能导致shard group被删除
                        if (shardGroup != null) {
                            shardGroup.getShardsList().forEach(shard -> {
                                if (shard.getStoreId() == storeId) {
                                    result.add(partition);
                                }
                            });
                        }
                    }catch (PDException e){
                        log.error("query partitions error, req:{}, error:{}", request, e.getMessage());
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                    }
                } else {
                    result.add(partition);
                }
            }
        }
        Pdpb.QueryPartitionsResponse response = Pdpb.QueryPartitionsResponse.newBuilder()
                                                                            .addAllPartitions(
                                                                                    result).build();
        observer.onNext(response);
        observer.onCompleted();

    }

    @Override
    public void getId(Pdpb.GetIdRequest request,
                      StreamObserver<Pdpb.GetIdResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetIdMethod(), request, responseObserver);
            return;
        }
        long id = 0L;
        try {
            id = idService.getId(request.getKey(), request.getDelta());
        } catch (PDException e) {
            responseObserver.onError(e);
            log.error("getId exception: ", e);
            return;
        }
        Pdpb.GetIdResponse response =
                Pdpb.GetIdResponse.newBuilder().setId(id).setDelta(request.getDelta())
                                  .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void resetId(Pdpb.ResetIdRequest request,
                        StreamObserver<Pdpb.ResetIdResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getResetIdMethod(), request, responseObserver);
            return;
        }
        try {
            idService.resetId(request.getKey());
        } catch (PDException e) {
            responseObserver.onError(e);
            log.error("getId exception: ", e);
            return;
        }
        Pdpb.ResetIdResponse response = Pdpb.ResetIdResponse.newBuilder().setResult(0).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Obtain cluster member information
     */
    @Override
    public void getMembers(Pdpb.GetMembersRequest request,
                           io.grpc.stub.StreamObserver<Pdpb.GetMembersResponse> observer) {

        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetMembersMethod(), request, observer);
            return;
        }
        Pdpb.GetMembersResponse response;
        try {
            response = Pdpb.GetMembersResponse.newBuilder()
                                              .addAllMembers(RaftEngine.getInstance().getMembers())
                                              .setLeader(RaftEngine.getInstance().getLocalMember())
                                              .build();

        } catch (Exception e) {
            log.error("getMembers exception: ", e);
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            response = Pdpb.GetMembersResponse.newBuilder()
                                              .setHeader(newErrorHeader(-1, e.getMessage()))
========
            response = Pdpb.GetMembersResponse.newBuilder().setHeader(getResponseHeader(-1, e.getMessage()))
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                                              .build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getStoreStatus(Pdpb.GetAllStoresRequest request,
                               io.grpc.stub.StreamObserver<Pdpb.GetAllStoresResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetStoreStatusMethod(), request, observer);
            return;
        }
        Pdpb.GetAllStoresResponse response = null;
        try {
            List<Metapb.Store> stores = null;
            stores = storeNodeService.getStoreStatus(request.getExcludeOfflineStores());
            response =
                    Pdpb.GetAllStoresResponse.newBuilder().setHeader(okHeader).addAllStores(stores)
                                             .build();
        } catch (PDException e) {
            response = Pdpb.GetAllStoresResponse.newBuilder().setHeader(getResponseHeader(e)).build();
            log.error("getAllStores exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * Read the PD configuration
     */
    @Override
    public void getPDConfig(Pdpb.GetPDConfigRequest request,
                            io.grpc.stub.StreamObserver<Pdpb.GetPDConfigResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetPDConfigMethod(), request, observer);
            return;
        }
        Pdpb.GetPDConfigResponse response = null;
        try {
            Metapb.PDConfig pdConfig = null;
            pdConfig = configService.getPDConfig(request.getVersion());
            response =
                    Pdpb.GetPDConfigResponse.newBuilder().setHeader(okHeader).setPdConfig(pdConfig)
                                            .build();
        } catch (PDException e) {
            response = Pdpb.GetPDConfigResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * Modify the PD configuration
     */
    @Override
    public void setPDConfig(Pdpb.SetPDConfigRequest request,
                            io.grpc.stub.StreamObserver<Pdpb.SetPDConfigResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getSetPDConfigMethod(), request, observer);
            return;
        }
        Pdpb.SetPDConfigResponse response = null;
        try {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            if (request.getPdConfig().getShardCount() % 2 != 1) {
                // Parity of the number of replicas
                throw new PDException(Pdpb.ErrorType.SET_CONFIG_SHARD_COUNT_ERROR_VALUE,
                                      "shard count must be an odd number!");
            }
            if (request.getPdConfig().getShardCount() >
                storeNodeService.getActiveStores().size()) {
                // It can't be greater than the number of active stores
                throw new PDException(Pdpb.ErrorType.SET_CONFIG_SHARD_COUNT_ERROR_VALUE,
                                      "shard count can't be greater than the number of active " +
                                      "stores!");
            }
            int oldShardCount = configService.getPDConfig().getShardCount();
            int newShardCount = request.getPdConfig().getShardCount();
            if (newShardCount > oldShardCount) {
                // If the number of replicas increases, check whether the resources inside the
                // store are sufficient
                if (!isResourceEnough(oldShardCount, newShardCount)) {
                    throw new PDException(Pdpb.ErrorType.SET_CONFIG_SHARD_COUNT_ERROR_VALUE,
                                          "There is not enough disk space left!");
                }

                if (!checkShardCount(newShardCount)) {
                    throw new PDException(Pdpb.ErrorType.SET_CONFIG_SHARD_COUNT_ERROR_VALUE,
                                          "the cluster can't support so many shard count!");
========
            if (request.getPdConfig().getShardCount() % 2 != 1){
                // 副本数奇偶校验
                throw new PDException(ErrorType.SET_CONFIG_SHARD_COUNT_ERROR_VALUE,
                        "shard count must be an odd number!");
            }
            if (request.getPdConfig().getShardCount() >
                    storeNodeService.getActiveStores().size()){
                // 不能大于活跃的store数量
                throw new PDException(ErrorType.SET_CONFIG_SHARD_COUNT_ERROR_VALUE,
                        "shard count can't be greater than the number of active stores!");
            }
            int oldShardCount = configService.getPDConfig().getShardCount();
            int newShardCount = request.getPdConfig().getShardCount();
            if (newShardCount > oldShardCount){
                // 如果副本数增大，则检查store内部的资源是否够用
                if (! isResourceEnough(oldShardCount, newShardCount)) {
                    throw new PDException(ErrorType.SET_CONFIG_SHARD_COUNT_ERROR_VALUE,
                            "There is not enough disk space left!");
                }

                if (! checkShardCount(newShardCount)) {
                    throw new PDException(ErrorType.SET_CONFIG_SHARD_COUNT_ERROR_VALUE,
                            "the cluster can't support so many shard count!");
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                }
            }
            configService.setPDConfig(request.getPdConfig());
            response = Pdpb.SetPDConfigResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            response = Pdpb.SetPDConfigResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * Read the graph space configuration
     */
    @Override
    public void getGraphSpace(Pdpb.GetGraphSpaceRequest request,
                              io.grpc.stub.StreamObserver<Pdpb.GetGraphSpaceResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetGraphSpaceMethod(), request, observer);
            return;
        }
        Pdpb.GetGraphSpaceResponse response = null;
        try {
            List<Metapb.GraphSpace> graphSpaces = null;
            graphSpaces = configService.getGraphSpace(request.getGraphSpaceName());
            response = Pdpb.GetGraphSpaceResponse.newBuilder().setHeader(okHeader)
                                                 .addAllGraphSpace(graphSpaces).build();
        } catch (PDException e) {
            response = Pdpb.GetGraphSpaceResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * Modify the graph space configuration
     */
    @Override
    public void setGraphSpace(Pdpb.SetGraphSpaceRequest request,
                              io.grpc.stub.StreamObserver<Pdpb.SetGraphSpaceResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getSetGraphSpaceMethod(), request, observer);
            return;
        }
        Pdpb.SetGraphSpaceResponse response = null;
        try {
            configService.setGraphSpace(request.getGraphSpace());
            response = Pdpb.SetGraphSpaceResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            response = Pdpb.SetGraphSpaceResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * <pre>
     * Data fragmentation
     * </pre>
     */
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
    @Override
    public void splitData(Pdpb.SplitDataRequest request,
                          StreamObserver<Pdpb.SplitDataResponse> observer) {
========
    public void splitData(ClusterOp.SplitDataRequest request, StreamObserver<ClusterOp.SplitDataResponse> observer) {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getSplitDataMethod(), request, observer);
            return;
        }
        logService.insertLog(LogService.PARTITION_CHANGE, "splitData", request);
        ClusterOp.SplitDataResponse response = null;
        try {
            taskService.splitPartition(request.getMode(), request.getStoreGroupId(), request.getParamList());
            response = ClusterOp.SplitDataResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            log.error("splitData exception:", e);
            response = ClusterOp.SplitDataResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();

    }

    @Override
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
    public void splitGraphData(Pdpb.SplitGraphDataRequest request,
                               StreamObserver<Pdpb.SplitDataResponse> observer) {
========
    public void splitGraphData(ClusterOp.SplitGraphDataRequest request,
                               StreamObserver<ClusterOp.SplitDataResponse> observer) {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getSplitGraphDataMethod(), request, observer);
            return;
        }
        logService.insertLog(LogService.PARTITION_CHANGE, "splitGraphData", request);
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        Pdpb.SplitDataResponse response;
        try {
            partitionService.splitPartition(partitionService.getGraph(request.getGraphName()),
                                            request.getToCount());
            response = Pdpb.SplitDataResponse.newBuilder().setHeader(okHeader).build();
========
        ClusterOp.SplitDataResponse response ;
        try {
            partitionService.splitPartition(partitionService.getGraph(request.getGraphName()), request.getToCount());
            response = ClusterOp.SplitDataResponse.newBuilder().setHeader(okHeader).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        } catch (PDException e) {
            log.error("splitGraphData exception", e);
            response = ClusterOp.SplitDataResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * Balance data between stores
     */
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
    @Override
    public void movePartition(Pdpb.MovePartitionRequest request,
                              StreamObserver<Pdpb.MovePartitionResponse> observer) {
========
    public void movePartition(ClusterOp.MovePartitionRequest request,
                             StreamObserver<ClusterOp.MovePartitionResponse> observer) {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getMovePartitionMethod(), request, observer);
            return;
        }
        logService.insertLog(LogService.PARTITION_CHANGE, "balanceData", request);

        ClusterOp.MovePartitionResponse response = null;
        try {
            if (request.getMode() == ClusterOp.OperationMode.Auto) {
                taskService.patrolPartitions();
                taskService.balancePartitionShard(request.getStoreGroupId());
            } else {
                for (ClusterOp.MovePartitionParam p : request.getParamList()) {
                    partitionService.movePartitionsShard(p.getPartitionId(), p.getSrcStoreId(), p.getDstStoreId());
                }
            }
            response = ClusterOp.MovePartitionResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            log.error("transferData exception", e);
            response = ClusterOp.MovePartitionResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * <pre>
     * Obtain the cluster health status
     * </pre>
     */
    @Override
    public void getClusterStats(Pdpb.GetClusterStatsRequest request,
                                io.grpc.stub.StreamObserver<Pdpb.GetClusterStatsResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetClusterStatsMethod(), request, observer);
            return;
        }
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        Pdpb.GetClusterStatsResponse response = null;
        response = Pdpb.GetClusterStatsResponse.newBuilder().setHeader(okHeader)
                                               .setCluster(storeNodeService.getClusterStats())
                                               .build();
========
        Pdpb.GetClusterStatsResponse response;

        try {
            Metapb.ClusterStats state;
            if (request.getStoreId() != 0) {
                state = storeNodeService.getClusterStats(request.getStoreId());
            } else {
                state = storeNodeService.getClusterStats(request.getStoreGroup());
            }
            response = Pdpb.GetClusterStatsResponse.newBuilder().setHeader(okHeader)
                    .setCluster(state)
                    .build();
        } catch (PDException e) {
            log.error("getClusterStats exception :", e);
            response = Pdpb.GetClusterStatsResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * <pre>
     * Report the results of tasks such as partition splitting
     * </pre>
     */
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
    @Override
    public void reportTask(Pdpb.ReportTaskRequest request,
                           io.grpc.stub.StreamObserver<Pdpb.ReportTaskResponse> observer) {
========
    public void reportTask(ClusterOp.ReportTaskRequest request,
                           io.grpc.stub.StreamObserver<ClusterOp.ReportTaskResponse> observer) {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getReportTaskMethod(), request, observer);
            return;
        }
        try {
            taskService.reportTask(request.getTask());
        } catch (Exception e) {
            log.error("PDService.reportTask", e);
        }
        ClusterOp.ReportTaskResponse response = null;
        response = ClusterOp.ReportTaskResponse.newBuilder().setHeader(okHeader).build();
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     *
     */
    @Override
    public void getPartitionStats(Pdpb.GetPartitionStatsRequest request,
                                  io.grpc.stub.StreamObserver<Pdpb.GetPartitionStatsResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetPartitionStatsMethod(), request, observer);
            return;
        }
        Pdpb.GetPartitionStatsResponse response;
        // TODO
        try {
            Metapb.PartitionStats stats = partitionService.getPartitionStats(request.getGraphName(),
                                                                             request.getPartitionId());
            response = Pdpb.GetPartitionStatsResponse.newBuilder().setHeader(okHeader)
                                                     .setPartitionStats(stats).build();
        } catch (PDException e) {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            log.error("getPartitionStats exception {}", e);
            response = Pdpb.GetPartitionStatsResponse.newBuilder().setHeader(newErrorHeader(e))
                                                     .build();
========
            log.error("getPartitionStats exception ", e);
            response = Pdpb.GetPartitionStatsResponse.newBuilder().setHeader(getResponseHeader(e)).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        }

        observer.onNext(response);
        observer.onCompleted();
    }
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java

    @Override
    public boolean isLeader() {
        return RaftEngine.getInstance().isLeader();
    }

    //private <ReqT, RespT, StubT extends AbstractBlockingStub<StubT>> void redirectToLeader(
    //        MethodDescriptor<ReqT, RespT> method, ReqT req, io.grpc.stub.StreamObserver<RespT>
    //        observer) {
    //    try {
    //        var addr = RaftEngine.getInstance().getLeaderGrpcAddress();
    //        ManagedChannel channel;
    //
    //        if ((channel = channelMap.get(addr)) == null) {
    //            synchronized (this) {
    //                if ((channel = channelMap.get(addr)) == null|| channel.isShutdown()) {
    //                    channel = ManagedChannelBuilder
    //                            .forTarget(addr).usePlaintext()
    //                            .build();
    //                }
    //            }
    //            log.info("Grpc get leader address {}", RaftEngine.getInstance()
    //            .getLeaderGrpcAddress());
    //        }
    //
    //        io.grpc.stub.ClientCalls.asyncUnaryCall(channel.newCall(method, CallOptions
    //        .DEFAULT), req,
    //                                                observer);
    //    } catch (Exception e) {
    //        e.printStackTrace();
    //    }
    //}

========
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
    /**
     * Renewal peerList
     */
    @Override
    public void changePeerList(ClusterOp.ChangePeerListRequest request,
                               io.grpc.stub.StreamObserver<ClusterOp.ChangePeerListResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getChangePeerListMethod(), request, observer);
            return;
        }
        ClusterOp.ChangePeerListResponse response;
        try {
            Status status = RaftEngine.getInstance().changePeerList(request.getPeerList());
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            Pdpb.ResponseHeader responseHeader =
                    status.isOk() ? okHeader : newErrorHeader(status.getCode(),
                                                              status.getErrorMsg());
            response =
                    Pdpb.getChangePeerListResponse.newBuilder().setHeader(responseHeader).build();

        } catch (Exception e) {
            log.error("changePeerList exception: ", e);
            response = Pdpb.getChangePeerListResponse.newBuilder()
                                                     .setHeader(newErrorHeader(-1, e.getMessage()))
                                                     .build();
========
            ResponseHeader responseHeader = status.isOk() ? okHeader : getResponseHeader(status.getCode(),
                                                                                           status.getErrorMsg());
            response = ClusterOp.ChangePeerListResponse.newBuilder().setHeader(responseHeader).build();

        } catch (Exception e) {
            log.error("changePeerList exception: ", e);
            response = ClusterOp.ChangePeerListResponse.newBuilder()
                                                     .setHeader(getResponseHeader(-1, e.getMessage())).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public synchronized void onRaftLeaderChanged() {
        log.info("onLeaderChanged");
        // channel = null;
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        // TODO: uncomment later
        //if (licenseVerifierService == null) {
        //    licenseVerifierService = new LicenseVerifierService(pdConfig);
        //}
        //licenseVerifierService.init();

        try {
            PDWatchSubject.notifyNodeChange(NodeEventType.NODE_EVENT_TYPE_PD_LEADER_CHANGE,
                                            RaftEngine.getInstance().getLeaderGrpcAddress(), 0L);
        } catch (ExecutionException | InterruptedException e) {
========
        if (licenseVerifierService == null) {
            licenseVerifierService = new LicenseVerifierService(pdConfig);
        }
        licenseVerifierService.init();

        try {
            PDPulseSubjects.notifyNodeChange(StoreNodeEventType.STORE_NODE_EVENT_TYPE_PD_LEADER_CHANGE,
                    RaftEngine.getInstance().getLeaderGrpcAddress(), 0L);
        } catch (Exception e) {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            log.error("failed to notice client", e);
        }
    }

    @Override
    public void balanceLeaders(ClusterOp.BalanceLeadersRequest request,
                               StreamObserver<ClusterOp.BalanceLeadersResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getBalanceLeadersMethod(), request, observer);
            return;
        }
        logService.insertLog(LogService.PARTITION_CHANGE, "balanceLeaders", request);
        ClusterOp.BalanceLeadersResponse response = null;
        try {
            taskService.balancePartitionLeader(true);
            response = ClusterOp.BalanceLeadersResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            log.error("balance Leaders exception: ", e);
            response =
                    Pdpb.BalanceLeadersResponse.newBuilder().setHeader(newErrorHeader(e)).build();
========
            log.error("balance Leaders exception ", e);
            response = ClusterOp.BalanceLeadersResponse.newBuilder().setHeader(getResponseHeader(e)).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    // TODO: keep it now & clean it later
    @Override
    public void putLicense(PutLicenseRequest request,
                           StreamObserver<PutLicenseResponse> responseObserver) {
        PutLicenseResponse response = null;
        boolean moved = false;
        String bakPath = pdConfig.getLicensePath() + "-bak";
        File bakFile = new File(bakPath);
        File licenseFile = new File(pdConfig.getLicensePath());
        try {
            byte[] content = request.getContent().toByteArray();
            if (licenseFile.exists()) {
                if (bakFile.exists()) {
                    FileUtils.deleteQuietly(bakFile);
                }
                FileUtils.moveFile(licenseFile, bakFile);
                moved = true;
            }
            FileUtils.writeByteArrayToFile(licenseFile, content, false);
        } catch (Exception e) {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            log.error("putLicense with error:", e);
========
            log.error("putLicense with error: ", e);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            if (moved) {
                try {
                    FileUtils.moveFile(bakFile, licenseFile);
                } catch (IOException ex) {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                    log.error("failed to restore the license file:", ex);
                }
            }
            Pdpb.ResponseHeader header =
                    newErrorHeader(Pdpb.ErrorType.LICENSE_ERROR_VALUE, e.getMessage());
========
                    log.error("failed to restore the license file.", ex);
                }
            }
            ResponseHeader header = getResponseHeader(ErrorType.LICENSE_ERROR_VALUE, e.getMessage());
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            response = Pdpb.PutLicenseResponse.newBuilder().setHeader(header).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void delStore(Pdpb.DetStoreRequest request,
                         StreamObserver<Pdpb.DetStoreResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getDelStoreMethod(), request, observer);
            return;
        }
        long storeId = request.getStoreId();
        Pdpb.DetStoreResponse response = null;
        try {
            Metapb.Store store = storeNodeService.getStore(storeId);
            if (Metapb.StoreState.Tombstone == store.getState()) {
                storeNodeService.removeStore(storeId);
                response = Pdpb.DetStoreResponse.newBuilder()
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                                                .setHeader(okHeader)
                                                .setStore(store)
                                                .build();
            } else {
                throw new PDException(Pdpb.ErrorType.STORE_PROHIBIT_DELETION_VALUE,
                                      "the store can't be deleted, please check store state!");
========
                        .setHeader(okHeader)
                        .setStore(store)
                        .build();
            }else{
                throw new PDException(ErrorType.STORE_PROHIBIT_DELETION_VALUE,
                        "the store can't be deleted, please check store state!");
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            }
        } catch (PDException e) {
            log.error("delete store exception:", e);
            response = Pdpb.DetStoreResponse.newBuilder()
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                                            .setHeader(newErrorHeader(e)).build();
========
                    .setHeader(getResponseHeader(e)).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * check the shard whether exceed the cluster's max shard group count
     *
     * @param newShardCount new shard count
     * @return true if can be set to new shard count, otherwise false
     */
    private boolean checkShardCount(int newShardCount) {
        try {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            var maxCount = pdConfig.getPartition().getMaxShardsPerStore() *
                           storeNodeService.getActiveStores().size() /
                           pdConfig.getConfigService().getPartitionCount();

            if (newShardCount > maxCount) {
                log.error("new shard count :{} exceed current cluster max shard count {}",
                          newShardCount, maxCount);
                return false;
========
            var storeGroups = configService.getAllStoreGroup();
            var maxStoreShardCount = pdConfig.getPartition().getMaxShardsPerStore();
            // 检查每个分组是否可以容纳新分片数量
            for (var storeGroup : storeGroups) {
                // 每个分组最大允许的shard数量
                int maxCount = storeNodeService.getActiveStoresByStoreGroup(storeGroup.getGroupId()).size() *
                        maxStoreShardCount / configService.getPartitionCount(storeGroup.getGroupId());

                if (newShardCount > maxCount) {
                    log.error("new shard count :{} exceed current cluster max shard count {}", newShardCount, maxCount);
                    return false;
                }

>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            }
        } catch (Exception e) {
            log.error("checkShardCount: {}", e.getMessage());
        }
        return true;
    }

    /**
     * Check that the store resources are sufficient
     */
    public boolean isResourceEnough(int oldShardCount, int newShardCount) {
        // Whether the resources of the active store are sufficient
        try {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            // The multiple of the storage space occupied
            float expansionRatio = newShardCount / oldShardCount;
            // The space currently occupied
========

            double expansionRatio = newShardCount * 1.0 / oldShardCount; // 占用的存储空间膨胀的倍数
            // 当前占用的空间
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            long currentDataSize = 0L;
            // The space occupied after data bloat
            long newDataSize = 0L;
            // Total free space
            long totalAvaible = 0L;
            // Statistics on the current storage space
            for (Metapb.Store store : storeNodeService.getStores()) {
                List<GraphStats> graphStatsList = store.getStats().getGraphStatsList();
                for (GraphStats graphStats : graphStatsList) {
                    currentDataSize += graphStats.getApproximateSize();
                }
            }
            // Estimate the storage space consumed after data bloat
            newDataSize = (long) Math.ceil(currentDataSize * expansionRatio);
            // Count the available space in all active stores
            List<Metapb.Store> activeStores = storeNodeService.getActiveStores();
            for (Metapb.Store store : activeStores) {
                Metapb.StoreStats storeStats = store.getStats();
                totalAvaible += storeStats.getAvailable();
            }
            // Consider whether resources are available when partitions are evenly distributed
            return totalAvaible > newDataSize - currentDataSize;
        } catch (PDException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * <pre>
     * Compaction on rocksdb
     * </pre>
     */
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
    @Override
    public void dbCompaction(Pdpb.DbCompactionRequest request,
                             StreamObserver<Pdpb.DbCompactionResponse> observer) {
========
    public void dbCompaction(ClusterOp.DbCompactionRequest request,
                             StreamObserver<ClusterOp.DbCompactionResponse> observer) {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getDbCompactionMethod(), request, observer);
            return;
        }
        logService.insertLog(LogService.TASK, "dbCompaction", request);
        ClusterOp.DbCompactionResponse response = null;
        try {
            log.info("dbCompaction call dbCompaction");
            taskService.dbCompaction(request.getTableName());
            response = ClusterOp.DbCompactionResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            log.error("dbCompaction exception", e);
            response = ClusterOp.DbCompactionResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void combineCluster(ClusterOp.CombineClusterRequest request,
                               StreamObserver<ClusterOp.CombineClusterResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getCombineClusterMethod(), request, observer);
            return;
        }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        Pdpb.CombineClusterResponse response;

        try {
            partitionService.combinePartition(request.getToCount());
            response = Pdpb.CombineClusterResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            response =
                    Pdpb.CombineClusterResponse.newBuilder().setHeader(newErrorHeader(e)).build();
========
        ClusterOp.CombineClusterResponse response ;

        try{
            partitionService.combinePartition(request.getStoreGroupId(), request.getToCount());
            response = ClusterOp.CombineClusterResponse.newBuilder().setHeader(okHeader).build();
        }catch (PDException e){
            response = ClusterOp.CombineClusterResponse.newBuilder().setHeader(getResponseHeader(e)).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Deprecated
    @Override
    public void combineGraph(ClusterOp.CombineGraphRequest request,
                             StreamObserver<ClusterOp.CombineGraphResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getCombineGraphMethod(), request, observer);
            return;
        }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        Pdpb.CombineGraphResponse response;
========
        ClusterOp.CombineGraphResponse response ;
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java

        try {
            partitionService.combineGraphPartition(request.getGraphName(), request.getToCount());
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            response = Pdpb.CombineGraphResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            response = Pdpb.CombineGraphResponse.newBuilder().setHeader(newErrorHeader(e)).build();
========
            response = ClusterOp.CombineGraphResponse.newBuilder().setHeader(okHeader).build();
        }catch (PDException e){
            response = ClusterOp.CombineGraphResponse.newBuilder().setHeader(getResponseHeader(e)).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void deleteShardGroup(Pdpb.DeleteShardGroupRequest request,
                                 StreamObserver<Pdpb.DeleteShardGroupResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getDeleteShardGroupMethod(), request, observer);
            return;
        }

        Pdpb.DeleteShardGroupResponse response;

        try {
            storeNodeService.deleteShardGroup(request.getGroupId());
            response = Pdpb.DeleteShardGroupResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            response =
                    Pdpb.DeleteShardGroupResponse.newBuilder().setHeader(newErrorHeader(e)).build();
========
            response = Pdpb.DeleteShardGroupResponse.newBuilder().setHeader(getResponseHeader(e)).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getShardGroup(Pdpb.GetShardGroupRequest request,
                              io.grpc.stub.StreamObserver<Pdpb.GetShardGroupResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetShardGroupMethod(), request, observer);
            return;
        }
        Pdpb.GetShardGroupResponse response;

        try {
            Metapb.ShardGroup shardGroup = storeNodeService.getShardGroup(request.getGroupId());
            response = Pdpb.GetShardGroupResponse.newBuilder().setHeader(okHeader)
                                                 .setShardGroup(shardGroup).build();
        } catch (PDException e) {
            log.error("getShardGroup exception", e);
            response = Pdpb.GetShardGroupResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void updateShardGroup(Pdpb.UpdateShardGroupRequest request,
                                 StreamObserver<Pdpb.UpdateShardGroupResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getUpdateShardGroupMethod(), request, responseObserver);
            return;
        }
        Pdpb.UpdateShardGroupResponse response;

        try {
            var group = request.getShardGroup();
            storeNodeService.updateShardGroup(group.getId(), group.getShardsList(),
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                                              group.getVersion(), group.getConfVer());
            response = Pdpb.UpdateShardGroupResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            log.error("updateShardGroup exception, ", e);
            response =
                    Pdpb.UpdateShardGroupResponse.newBuilder().setHeader(newErrorHeader(e)).build();
========
                    group.getVersion(), group.getConfVer());
            response = Pdpb.UpdateShardGroupResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            log.error("updateShardGroup exception, ", e);
            response = Pdpb.UpdateShardGroupResponse.newBuilder().setHeader(getResponseHeader(e)).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
    public void updateShardGroupOp(Pdpb.ChangeShardRequest request,
                                   StreamObserver<Pdpb.ChangeShardResponse> observer) {
========
    public void updateShardGroupOp(ClusterOp.ChangeShardRequest request,
                                   StreamObserver<ClusterOp.ChangeShardResponse> observer) {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getUpdateShardGroupOpMethod(), request, observer);
            return;
        }

        ClusterOp.ChangeShardResponse response;

        try {
            storeNodeService.shardGroupOp(request.getGroupId(), request.getShardsList());
            response = ClusterOp.ChangeShardResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            log.error("changeShard exception, ", e);
            response = ClusterOp.ChangeShardResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
    public void changeShard(Pdpb.ChangeShardRequest request,
                            StreamObserver<Pdpb.ChangeShardResponse> observer) {
========
    public void changeShard(ClusterOp.ChangeShardRequest request,
                            StreamObserver<ClusterOp.ChangeShardResponse> observer) {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getChangeShardMethod(), request, observer);
            return;
        }

        ClusterOp.ChangeShardResponse response;

        try {
            partitionService.changeShard(request.getGroupId(), request.getShardsList());
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            response = Pdpb.ChangeShardResponse.newBuilder().setHeader(okHeader).build();
========
            response = ClusterOp.ChangeShardResponse.newBuilder().setHeader(okHeader).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        } catch (PDException e) {
            log.error("changeShard exception, ", e);
            response = ClusterOp.ChangeShardResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }

        observer.onNext(response);
        observer.onCompleted();
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
    @Override
    public void updatePdRaft(Pdpb.UpdatePdRaftRequest request,
                             StreamObserver<Pdpb.UpdatePdRaftResponse> observer) {
========
    public void updatePdRaft(ClusterOp.UpdatePdRaftRequest request,
                             StreamObserver<ClusterOp.UpdatePdRaftResponse> observer){
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getUpdatePdRaftMethod(), request, observer);
            return;
        }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        var list = parseConfig(request.getConfig());

        log.info("update raft request: {}, list: {}", request.getConfig(), list);

        Pdpb.UpdatePdRaftResponse response =
                Pdpb.UpdatePdRaftResponse.newBuilder().setHeader(okHeader).build();

        do {
            var leaders = list.stream().filter(s -> s.getKey().equals("leader"))
                              .collect(Collectors.toList());
========
        var list = PeerUtil.parseConfig(request.getConfig());

        log.info("update raft request: {}, list: {}", request.getConfig(), list);

        ClusterOp.UpdatePdRaftResponse response = ClusterOp.UpdatePdRaftResponse.newBuilder()
                                                                        .setHeader(okHeader).build();

        do {
            var leaders = list.stream().filter(s -> s.getKey().equals("leader")).collect(Collectors.toList());
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            var node = RaftEngine.getInstance().getRaftNode();

            if (leaders.size() == 1) {
                var leaderPeer = leaders.get(0).getValue();
                // change leader
                var peers = new HashSet<>(node.listPeers());

<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                if (!peerEquals(leaderPeer, node.getLeaderId())) {
========
                if (!PeerUtil.isPeerEquals(leaderPeer, node.getLeaderId())) {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                    if (peers.contains(leaderPeer)) {
                        log.info("updatePdRaft, transfer to {}", leaderPeer);
                        node.transferLeadershipTo(leaderPeer);
                    } else {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                        response = Pdpb.UpdatePdRaftResponse.newBuilder()
                                                            .setHeader(newErrorHeader(6667,
                                                                                      "new leader" +
                                                                                      " not in " +
                                                                                      "raft peers"))
                                                            .build();
========
                        response = ClusterOp.UpdatePdRaftResponse.newBuilder()
                                .setHeader(getResponseHeader(6667, "new leader not in raft peers"))
                                .build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                    }
                    break;
                }
            } else {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                response = Pdpb.UpdatePdRaftResponse.newBuilder()
                                                    .setHeader(newErrorHeader(6666,
                                                                              "leader size != 1"))
                                                    .build();
========
                response = ClusterOp.UpdatePdRaftResponse.newBuilder()
                        .setHeader(getResponseHeader(6666, "leader size != 1")).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                break;
            }

            Configuration config = new Configuration();
            // add peer
            for (var peer : list) {
                if (!peer.getKey().equals("learner")) {
                    config.addPeer(peer.getValue());
                } else {
                    config.addLearner(peer.getValue());
                }
            }

            log.info("pd raft update with new config: {}", config);

            node.changePeers(config, status -> {
                if (status.isOk()) {
                    log.info("updatePdRaft, change peers success");
                } else {
                    log.error("changePeers status: {}, msg:{}, code: {}, raft error:{}",
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                              status, status.getErrorMsg(), status.getCode(),
                              status.getRaftError());
========
                            status, status.getErrorMsg(), status.getCode(), status.getRaftError());
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
                }
            });
        } while (false);

        observer.onNext(response);
        observer.onCompleted();
    }

    public void getCache(GetGraphRequest request,
                         StreamObserver<CacheResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetCacheMethod(), request, observer);
            return;
        }
        CacheResponse response;
        try {
            response = CacheResponse.newBuilder().mergeFrom(storeNodeService.getCache())
                                    .setHeader(okHeader).build();
        } catch (PDException e) {
            log.error("get cache exception, ", e);
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
            response = CacheResponse.newBuilder().setHeader(newErrorHeader(e)).build();
========
            response = CacheResponse.newBuilder().setHeader(getResponseHeader(e)).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    public void getPartitions(GetGraphRequest request,
                              StreamObserver<CachePartitionResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetPartitionsMethod(), request, observer);
            return;
        }
        CachePartitionResponse response;
        List<Metapb.Partition> partitions = partitionService.getPartitions(request.getGraphName());
        response = CachePartitionResponse.newBuilder().addAllPartitions(partitions)
                                         .setHeader(okHeader).build();
        observer.onNext(response);
        observer.onCompleted();
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
    private List<KVPair<String, PeerId>> parseConfig(String conf) {
        List<KVPair<String, PeerId>> result = new LinkedList<>();

        if (conf != null && conf.length() > 0) {
            for (var s : conf.split(",")) {
                if (s.endsWith("/leader")) {
                    result.add(new KVPair<>("leader",
                                            JRaftUtils.getPeerId(s.substring(0, s.length() - 7))));
                } else if (s.endsWith("/learner")) {
                    result.add(new KVPair<>("learner",
                                            JRaftUtils.getPeerId(s.substring(0, s.length() - 8))));
                } else if (s.endsWith("/follower")) {
                    result.add(new KVPair<>("follower",
                                            JRaftUtils.getPeerId(s.substring(0, s.length() - 9))));
                } else {
                    result.add(new KVPair<>("follower", JRaftUtils.getPeerId(s)));
                }
            }
        }

        return result;
    }

    private boolean peerEquals(PeerId p1, PeerId p2) {
        if (p1 == null && p2 == null) {
            return true;
        }
        if (p1 == null || p2 == null) {
            return false;
        }
        return Objects.equals(p1.getIp(), p2.getIp()) && Objects.equals(p1.getPort(), p2.getPort());
    }
========

    @Override
    public void submitIndexTask(Pdpb.IndexTaskCreateRequest request, StreamObserver<Pdpb.TaskQueryResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getSubmitIndexTaskMethod(), request, observer);
            return;
        }

        var builder = Pdpb.TaskQueryResponse.newBuilder();
        var param = request.getParam();
        try {
            var partitions = partitionService.getPartitions(param.getGraph());

            if (partitions.isEmpty()) {
                throw new PDException(PARTITION_NOT_EXISTS, "graph has no partition");
            }

            var newTaskId = idService.getId(USER_TASK_ID_KEY, 1);

            var taskInfo = storeNodeService.getTaskInfoMeta();
            for (var partition : partitions) {
                var buildIndex = Metapb.BuildIndex.newBuilder()
                        .setPartitionId(partition.getId())
                        .setTaskId(newTaskId)
                        .setParam(param)
                        .build();

                var task = MetaTask.Task.newBuilder()
                        .setId(newTaskId)
                        .setType(MetaTask.TaskType.Build_Index)
                        .setState(MetaTask.TaskState.Task_Doing)
                        .setStartTimestamp(System.currentTimeMillis())
                        .setPartition(partition)
                        .setBuildIndex(buildIndex)
                        .build();

                taskInfo.updateUserTask(task);

                log.info("notify client build index task: {}", buildIndex);

                PDPulseSubjects.notifyClient(PartitionHeartbeatResponse.newBuilder()
                        .setPartition(partition)
                        // 给store的task id
                        .setId(newTaskId)
                        .setBuildIndex(buildIndex));
            }
            observer.onNext(builder.setHeader(okHeader).setTaskId(newTaskId).build());
        } catch (PDException e) {
            log.error("IndexTaskGrpcService.submitTask", e);
            observer.onNext(builder.setHeader(getResponseHeader(e)).build());
        }
        observer.onCompleted();
    }

    @Override
    public void submitBackupGraphTask(Pdpb.BackupGraphRequest request,
                                      StreamObserver<Pdpb.TaskQueryResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getSubmitBackupGraphTaskMethod(), request, observer);
            return;
        }

        var builder = Pdpb.TaskQueryResponse.newBuilder();

        try {

            var sourceGraph = partitionService.getGraph(request.getGraphName());
            var targetGraph = partitionService.getGraph(request.getTargetGraphName());

            if (sourceGraph == null || targetGraph == null) {
                throw new PDException(GRAPH_NOT_EXISTS, "source or target graph not exists");
            }

            var partitions = partitionService.getPartitions(request.getGraphName());
            var targetPartitions = partitionService.getPartitions(request.getTargetGraphName());

            if (partitions.isEmpty()) {
                throw new PDException(PARTITION_NOT_EXISTS, "source graph has no partition");
            }

            if (targetPartitions.isEmpty()) {
                partitionService.allocGraphPartitions(targetGraph);
                targetPartitions = partitionService.getPartitions(request.getTargetGraphName());
            }

            var newTaskId = idService.getId(USER_TASK_ID_KEY, 1);

            var taskInfo = storeNodeService.getTaskInfoMeta();

            for (var partition : partitions) {

                SplitPartition.Builder splitBuilder = SplitPartition.newBuilder().addAllNewPartition(targetPartitions);

                var task = MetaTask.Task.newBuilder()
                        .setId(newTaskId)
                        .setType(MetaTask.TaskType.Backup_Graph)
                        .setState(MetaTask.TaskState.Task_Doing)
                        .setStartTimestamp(System.currentTimeMillis())
                        .setPartition(partition)
                        .setSplitPartition(splitBuilder.build())
                        .build();

                taskInfo.updateUserTask(task);

                log.info("notify client backup graph: {} - {}", sourceGraph.getGraphName(), partition.getId());

                PDPulseSubjects.notifyClient(PartitionHeartbeatResponse.newBuilder()
                        .setPartition(partition)
                        // 给store的task id
                        .setId(newTaskId)
                        .setSplitPartition(splitBuilder.build()));
            }

            observer.onNext(builder.setHeader(okHeader).setTaskId(newTaskId).build());
        } catch (PDException e) {
            log.error("IndexTaskGrpcService.submitTask", e);
            observer.onNext(builder.setHeader(getResponseHeader(e)).build());
        }
        observer.onCompleted();
    }

    @Override
    public void queryTaskState(Pdpb.TaskQueryRequest request, StreamObserver<Pdpb.TaskQueryResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getQueryTaskStateMethod(), request, observer);
            return;
        }

        var taskInfo = storeNodeService.getTaskInfoMeta();
        var builder = Pdpb.TaskQueryResponse.newBuilder();

        try {
            var tasks = taskInfo.scanUserTask(request.getTaskId());

            if (tasks.isEmpty()) {
                throw new PDException(TASK_NOT_EXISTS, "task not found");
            } else {
                var state = MetaTask.TaskState.Task_Success;
                String message = "OK";
                int countOfSuccess = 0;
                int countOfDoing = 0;

                for (var task : tasks)  {
                    var state0 = task.getState();
                    if (state0 == MetaTask.TaskState.Task_Failure) {
                        state = MetaTask.TaskState.Task_Failure;
                        message = task.getMessage();
                        break;
                    } else if (state0 == MetaTask.TaskState.Task_Doing) {
                        state = MetaTask.TaskState.Task_Doing;
                        countOfDoing ++;
                    } else  if (state0 == MetaTask.TaskState.Task_Success) {
                        countOfSuccess ++;
                    }
                }

                if (state == MetaTask.TaskState.Task_Doing) {
                    message = "Doing/" + countOfDoing + ", Success/" + countOfSuccess;
                }

                builder.setHeader(okHeader).setState(state).setMessage(message);
            }
        } catch (PDException e) {
            builder.setHeader(getResponseHeader(e));
        }

        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    public void retryTask(Pdpb.TaskQueryRequest request, StreamObserver<Pdpb.TaskQueryResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getRetryTaskMethod(), request, observer);
            return;
        }

        var taskInfo = storeNodeService.getTaskInfoMeta();
        var builder = Pdpb.TaskQueryResponse.newBuilder();
        var taskId = request.getTaskId();

        try {
            var tasks = taskInfo.scanUserTask(taskId);

            if (tasks.isEmpty()) {
                builder.setHeader(okHeader).setState(MetaTask.TaskState.Task_Failure).setMessage("task not found");
            } else {
                var state = MetaTask.TaskState.Task_Success;
                String message = "OK";
                for (var task : tasks)  {
                    var state0 = task.getState();
                    if (state0 == MetaTask.TaskState.Task_Failure || state0 == MetaTask.TaskState.Task_Doing) {
                        var partition = task.getPartition();
                        log.info("notify client retry task: {}", task.getId());

                        var responseBuilder =  PartitionHeartbeatResponse.newBuilder()
                                        .setPartition(partition)
                                        .setId(task.getId());
                        if (task.hasBuildIndex()) {
                            responseBuilder.setBuildIndex(task.getBuildIndex());
                        } else if (task.hasSplitPartition()) {
                            responseBuilder.setSplitPartition(task.getSplitPartition());
                        } else {
                            throw new PDException(TASK_NOT_EXISTS, "task type not support");
                        }

                        PDPulseSubjects.notifyClient(responseBuilder);
                    }
                }
                builder.setHeader(okHeader).setState(state).setMessage(message);
            }
        } catch (PDException e) {
            builder.setHeader(getResponseHeader(e));
        }

        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    public void getGraphStats(GetGraphRequest request,
                              io.grpc.stub.StreamObserver<GraphStatsResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetGraphStatsMethod(), request, observer);
            return;
        }
        String graphName = request.getGraphName();
        GraphStatsResponse.Builder builder = GraphStatsResponse.newBuilder();
        try {
            List<Metapb.Store> stores = storeNodeService.getStores(graphName);
            long dataSize = 0;
            long keySize = 0;
            for (Metapb.Store store : stores) {
                List<GraphStats> gss = store.getStats().getGraphStatsList();
                if (!gss.isEmpty()) {
                    String gssGraph = gss.get(0).getGraphName();
                    String suffix = "/g";
                    if (gssGraph.split("/").length > 2 && !graphName.endsWith(suffix)) {
                        graphName += suffix;
                    }
                    for (GraphStats gs : gss) {
                        boolean nameEqual = graphName.equals(gs.getGraphName());
                        boolean roleEqual = Metapb.ShardRole.Leader.equals(gs.getRole());
                        if (nameEqual && roleEqual) {
                            dataSize += gs.getApproximateSize();
                            keySize += gs.getApproximateKeys();
                        }
                    }
                }
            }
            GraphStats stats = GraphStats.newBuilder().setApproximateSize(dataSize)
                                         .setApproximateKeys(keySize).setGraphName(request.getGraphName())
                                         .build();
            builder.setStats(stats);
        } catch (PDException e) {
            builder.setHeader(getResponseHeader(e));
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

    @Override
    public void getMembersAndClusterState(Pdpb.GetMembersRequest request,
                                          io.grpc.stub.StreamObserver<Pdpb.MembersAndClusterState> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetMembersAndClusterStateMethod(), request, observer);
            return;
        }
        Pdpb.MembersAndClusterState response;
        try {
            var stateList= storeNodeService.getAllClusterStats()
                    .entrySet().stream().map(entry -> Metapb.GroupClusterState.newBuilder()
                            .setStoreGroup(entry.getKey())
                            .setState(entry.getValue()).build())
                    .collect(Collectors.toList());

            response = Pdpb.MembersAndClusterState.newBuilder()
                                                  .addAllMembers(RaftEngine.getInstance().getMembers())
                                                  .setLeader(RaftEngine.getInstance().getLocalMember())
                                                  .addAllState(stateList)
                                                  // .setState(storeNodeService.getClusterStats().getState())
                                                  .build();

        } catch (Exception e) {
            log.error("getMembers exception: ", e);
            response = Pdpb.MembersAndClusterState.newBuilder().setHeader(getResponseHeader(-1, e.getMessage()))
                                                  .build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void createGraph(Pdpb.CreateGraphRequest request, StreamObserver<Pdpb.CreateGraphResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getCreateGraphMethod(), request, observer);
            return;
        }

        Pdpb.CreateGraphResponse response;
        Metapb.Graph graph = request.getGraph();
        try {
            graph = partitionService.createGraph(graph.getGraphName(),
                        graph.getPartitionCount(), graph.getStoreGroupId());
            response = Pdpb.CreateGraphResponse.newBuilder().setHeader(okHeader).setGraph(graph).build();
        } catch (PDException e) {
            log.error("create exception: ", e);
            response = Pdpb.CreateGraphResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void createStoreGroup(StoreGroup.CreateStoreGroupRequest request,
                                 StreamObserver<StoreGroup.CreateStoreGroupResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getCreateStoreGroupMethod(), request, observer);
            return;
        }

        StoreGroup.CreateStoreGroupResponse response;

        try {
            var storeGroup = configService.getStoreGroup(request.getGroupId());
            if (storeGroup == null) {
                storeGroup = configService.createStoreGroup(request.getGroupId(),
                        request.getName(), request.getPartitionCount());
                storeNodeService.updateClusterStatus(request.getGroupId(), Metapb.ClusterState.Cluster_Not_Ready);
            } else {
                throw new PDException(STORE_GROUP_NOT_EXISTS.getNumber(), "Store Group exists");
            }

            response = StoreGroup.CreateStoreGroupResponse.newBuilder()
                    .setHeader(okHeader).setStoreGroup(storeGroup).build();
        } catch (PDException e) {
            response = StoreGroup.CreateStoreGroupResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getStoreGroup(StoreGroup.GetStoreGroupRequest request,
                              StreamObserver<StoreGroup.GetStoreGroupResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetStoreGroupMethod(), request, observer);
            return;
        }

        StoreGroup.GetStoreGroupResponse response;
        try {
            var storeGroup = configService.getStoreGroup(request.getGroupId());
            if (storeGroup == null) {
                throw new PDException(STORE_GROUP_NOT_EXISTS);
            }
            response = StoreGroup.GetStoreGroupResponse.newBuilder().setHeader(okHeader)
                    .setStoreGroup(storeGroup).build();
        } catch (PDException e) {
            response = StoreGroup.GetStoreGroupResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getAllStoreGroup(StoreGroup.GetAllStoreGroupRequest request,
                                 StreamObserver<StoreGroup.GetAllStoreGroupResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetAllStoreGroupMethod(), request, observer);
            return;
        }

        StoreGroup.GetAllStoreGroupResponse response;

        try {
            var groupStoreList = configService.getAllStoreGroup();
            response = StoreGroup.GetAllStoreGroupResponse.newBuilder()
                    .setHeader(okHeader).addAllStoreGroups(groupStoreList).build();
        } catch (PDException e) {
            response = StoreGroup.GetAllStoreGroupResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void updateStoreGroup(StoreGroup.UpdateStoreGroupRequest request,
                                 StreamObserver<StoreGroup.UpdateStoreGroupResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getUpdateStoreGroupMethod(), request, observer);
            return;
        }

        StoreGroup.UpdateStoreGroupResponse response;

        try {
            var storeGroup = configService.updateStoreGroup(request.getGroupId(), request.getName());
            response = StoreGroup.UpdateStoreGroupResponse.newBuilder()
                    .setHeader(okHeader).setStoreGroup(storeGroup).build();
        } catch (PDException e) {
            response = StoreGroup.UpdateStoreGroupResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getStoresByStoreGroup(StoreGroup.GetGroupStoresRequest request,
                                      StreamObserver<StoreGroup.GetGroupStoresResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetStoresByStoreGroupMethod(), request, observer);
            return;
        }

        StoreGroup.GetGroupStoresResponse response;

        try {
            var stores = storeNodeService.getStoresByStoreGroup(request.getStoreGroupId());
            response = StoreGroup.GetGroupStoresResponse.newBuilder().addAllStores(stores).build();
        } catch (PDException e) {
            response = StoreGroup.GetGroupStoresResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void updateStoreGroupRelation(StoreGroup.UpdateStoreGroupRelationRequest request,
                                         StreamObserver<StoreGroup.UpdateStoreGroupRelationResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getUpdateStoreGroupRelationMethod(), request, observer);
            return;
        }

        StoreGroup.UpdateStoreGroupRelationResponse response;

        try {
            // 没做过初始化 或者初始化过但是没有shard group的分配
            if (! storeNodeService.isStoreHasStoreGroup(request.getStoreId()) || storeNodeService.getShardGroups()
                    .stream().noneMatch(shardGroup -> {
                        for (var shard : shardGroup.getShardsList()) {
                            if (shard.getStoreId() == request.getStoreId()) {
                                return true;
                            }
                        }
                        return false; })) {
                storeNodeService.updateStoreGroupRelation(request.getStoreId(), request.getStoreGroupId());
                response = StoreGroup.UpdateStoreGroupRelationResponse.newBuilder().setHeader(okHeader)
                        .setSuccess(true).setMessage("").build();
            } else {
                response = StoreGroup.UpdateStoreGroupRelationResponse.newBuilder()
                        .setHeader(getResponseHeader(-1, "store has partitions yet")).build();
            }
        } catch (PDException e) {
            response = StoreGroup.UpdateStoreGroupRelationResponse.newBuilder().setHeader(getResponseHeader(e)).build();
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void getLeaderGrpcAddress(NoArg request,
                                     StreamObserver<GetLeaderGrpcAddressResponse> observer) {
        GetLeaderGrpcAddressResponse.Builder response = GetLeaderGrpcAddressResponse.newBuilder();
        try {
            String grpcAddress = RaftEngine.getInstance().getLeaderGrpcAddress(false);
            response.setHeader(okHeader).setAddress(grpcAddress);
        } catch (PDException e) {
            response.setHeader(getResponseHeader(e));
        }
        observer.onNext(response.build());
        observer.onCompleted();
    }

    /**
     */
    @Override
    public void clearGrpcAddressCache(NoArg request, StreamObserver<VoidResponse> observer) {
        VoidResponse.Builder response = VoidResponse.newBuilder();
        try {
            RaftEngine.getInstance().clearGrpcAddresses();
            response.setHeader(okHeader);
        } catch (Exception e) {
            response.setHeader(getResponseHeader(ErrorType.ERROR_VALUE, e.getMessage()));
        }
        observer.onNext(response.build());
        observer.onCompleted();
    }

    @Override
    public void getAllGrpcAddresses(NoArg request,
                                    StreamObserver<Pdpb.GetAllGrpcAddressesResponse> observer) {
        boolean allows = pdConfig.isAllowsAddressAcquisition();
        Pdpb.GetAllGrpcAddressesResponse.Builder builder =
                Pdpb.GetAllGrpcAddressesResponse.newBuilder().setAllowed(allows);
        try {
            if (!isLeader()) {
                redirectToLeader(PDGrpc.getGetAllGrpcAddressesMethod(), request, observer);
                return;
            }
            if (allows) {
                List<String> grpcAddresses = RaftEngine.getInstance().getPeerGrpcAddressesByCache();
                builder.addAllAddresses(grpcAddresses).setHeader(okHeader);
            }
        } catch (Exception e) {
            log.error("getAllGrpcAddresses error", e);
            builder.setHeader(getResponseHeader(ErrorType.ERROR_VALUE, e.getMessage()));
        }
        observer.onNext(builder.build());
        observer.onCompleted();
    }

>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDService.java
}
