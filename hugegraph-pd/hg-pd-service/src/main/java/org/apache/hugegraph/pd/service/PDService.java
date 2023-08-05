/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
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

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

// TODO: uncomment later - remove license verifier service now
@Slf4j
@GRpcService
public class PDService extends PDGrpc.PDImplBase implements ServiceGrpc, RaftStateListener {

    static String TASK_ID_KEY = "task_id";
    private final Pdpb.ResponseHeader okHeader = Pdpb.ResponseHeader.newBuilder().setError(
            Pdpb.Error.newBuilder().setType(Pdpb.ErrorType.OK)).build();
    // private ManagedChannel channel;
    private final Map<String, ManagedChannel> channelMap = new ConcurrentHashMap<>();
    @Autowired
    private PDConfig pdConfig;
    private StoreNodeService storeNodeService;
    private PartitionService partitionService;
    private TaskScheduleService taskService;
    private IdService idService;
    private ConfigService configService;
    private LogService logService;
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

    /**
     * 初始化
     */
    @PostConstruct
    public void init() throws PDException {
        log.info("PDService init………… {}", pdConfig);
        configService = new ConfigService(pdConfig);

        RaftEngine.getInstance().addStateListener(this);
        RaftEngine.getInstance().addStateListener(configService);
        RaftEngine.getInstance().init(pdConfig.getRaft());
        //pdConfig = configService.loadConfig(); onLeaderChanged 中加载
        storeNodeService = new StoreNodeService(pdConfig);
        partitionService = new PartitionService(pdConfig, storeNodeService);
        taskService = new TaskScheduleService(pdConfig, storeNodeService, partitionService);
        idService = new IdService(pdConfig);
        logService = new LogService(pdConfig);
        storeMonitorDataService = new StoreMonitorDataService(pdConfig);
        //if (licenseVerifierService == null) {
        //    licenseVerifierService = new LicenseVerifierService(pdConfig);
        //}
        RaftEngine.getInstance().addStateListener(partitionService);
        pdConfig.setIdService(idService);

        // 接收心跳消息
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
         * 监听分区指令，并转发给 Store
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
         * 监听分区状态改变消息，并转发给 Client
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
         * 监听 store 状态改变消息，并转发给 Client
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
        storeNodeService.init(partitionService);
        partitionService.init();
        taskService.init();
        // log.info("init .......");
        // licenseVerifierService.init();

        // UpgradeService upgradeService = new UpgradeService(pdConfig);
        // upgradeService.upgrade();
    }

    /**
     * <pre>
     * 注册 store，首次注册会生成新的 store_id，store_id 是 store 唯一标识
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
            response = Pdpb.RegisterStoreResponse.newBuilder().setHeader(newErrorHeader(e)).build();
            log.error("registerStore exception: ", e);
        }
        // 拉取所有分区信息，并返回
        observer.onNext(response);
        observer.onCompleted();

    }

    /**
     * 根据 store_id 查找 store
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
            response = Pdpb.GetStoreResponse.newBuilder().setHeader(newErrorHeader(e)).build();
            log.error("{} getStore exception: {}", StreamObserverUtil.getRemoteIP(observer), e);
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * <pre>
     * 修改 Store 状态等信息。
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
            // 处于 Pending 状态，才可以上线
            Metapb.Store lastStore = storeNodeService.getStore(request.getStore().getId());
            if (lastStore == null) {
                // storeId 不存在，抛出异常
                throw new PDException(Pdpb.ErrorType.STORE_ID_NOT_EXIST_VALUE,
                                      String.format("Store id %d does not exist!", storeId));
            }
            if (Metapb.StoreState.Up.equals(state)) {
                if (!Metapb.StoreState.Pending.equals(lastStore.getState())) {
                    throw new PDException(Pdpb.ErrorType.UPDATE_STORE_STATE_ERROR_VALUE,
                                          "only stores in Pending state can be set to Up!");
                }
            }
            if (state.equals(Metapb.StoreState.Offline)) {
                Metapb.ClusterStats stats = storeNodeService.getClusterStats();
                if (stats.getState() != Metapb.ClusterState.Cluster_OK) {
                    Pdpb.ResponseHeader errorHeader = newErrorHeader(-1,
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
            // 检查失败，状态改为 Pending，把错误原因返回去
            if (state.equals(Metapb.StoreState.Up)) {
                int cores = 0;
                long id = request.getStore().getId();
                List<Metapb.Store> stores = storeNodeService.getStores();
                int nodeCount = 0;
                for (Metapb.Store store : stores) {
                    if (store.getId() == id) {
                        // 获取之前注册的 store 中的 cores 作为验证参数
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
                    throw new PDException(Pdpb.ErrorType.LICENSE_ERROR_VALUE,
                                          "check license with error :"
                                          + e.getMessage()
                                          + ", and changed node state to 'Pending'");
                }
            }
            Metapb.Store store = request.getStore();
            // 下线之前先判断一下，活跃机器数是否大于最小阈值
            if (state.equals(Metapb.StoreState.Tombstone)) {
                List<Metapb.Store> activeStores = storeNodeService.getActiveStores();
                if (lastStore.getState() == Metapb.StoreState.Up
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
                    // 如果已经是下线中的状态，则不作进一步处理
                    throw new PDException(Pdpb.ErrorType.Store_Tombstone_Doing_VALUE,
                                          "Downline is in progress, do not resubmit");
                }
                Map<String, Object> resultMap = taskService.canAllPartitionsMovedOut(lastStore);
                if ((boolean) resultMap.get("flag")) {
                    if (resultMap.get("current_store_is_online") != null
                        && (boolean) resultMap.get("current_store_is_online")) {
                        log.info("updateStore removeActiveStores store {}", store.getId());
                        // 将在线的 store 的状态设置为下线中，等待副本迁移
                        store = Metapb.Store.newBuilder(lastStore)
                                            .setState(Metapb.StoreState.Exiting).build();
                        // 进行分区迁移操作
                        taskService.movePartitions((Map<Integer, KVPair<Long, Long>>) resultMap.get(
                                "movedPartitions"));
                    } else {
                        // store 已经离线的，不做副本迁移
                        // 将状态改为 Tombstone
                    }
                } else {
                    throw new PDException(Pdpb.ErrorType.UPDATE_STORE_STATE_ERROR_VALUE,
                                          "the resources on other stores may be not enough to " +
                                          "store " +
                                          "the partitions of current store!");
                }
            }
            // 替换 license 都走 grpc
            store = storeNodeService.updateStore(store);
            response =
                    Pdpb.SetStoreResponse.newBuilder().setHeader(okHeader).setStore(store).build();
        } catch (PDException e) {
            response = Pdpb.SetStoreResponse.newBuilder().setHeader(newErrorHeader(e)).build();
            log.error("setStore exception: ", e);
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * 返回所有的 store,exclude_offline_stores=true，返回活跃的 stores
     */
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
                stores = storeNodeService.getActiveStores(request.getGraphName());
            } else {
                stores = storeNodeService.getStores(request.getGraphName());
            }
            response =
                    Pdpb.GetAllStoresResponse.newBuilder().setHeader(okHeader).addAllStores(stores)
                                             .build();
        } catch (PDException e) {
            response = Pdpb.GetAllStoresResponse.newBuilder().setHeader(newErrorHeader(e)).build();
            log.error("getAllStores exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * 处理 store 心跳
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
            // remove system_metrics
            stats = Metapb.StoreStats.newBuilder()
                                     .mergeFrom(request.getStats())
                                     .clearField(Metapb.StoreStats.getDescriptor().findFieldByName(
                                             "system_metrics"))
                                     .build();
        }

        Pdpb.StoreHeartbeatResponse response = null;
        try {
            Metapb.ClusterStats clusterStats = storeNodeService.heartBeat(stats);
            response = Pdpb.StoreHeartbeatResponse.newBuilder().setHeader(okHeader)
                                                  .setClusterStats(clusterStats).build();
        } catch (PDException e) {
            response =
                    Pdpb.StoreHeartbeatResponse.newBuilder().setHeader(newErrorHeader(e)).build();
            log.error("storeHeartbeat exception: ", e);
        } catch (Exception e2) {
            response = Pdpb.StoreHeartbeatResponse.newBuilder().setHeader(
                    newErrorHeader(Pdpb.ErrorType.UNKNOWN_VALUE, e2.getMessage())).build();
            log.error("storeHeartbeat exception: ", e2);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * <pre>
     * 查找 key 所属的分区
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
            response = Pdpb.GetPartitionResponse.newBuilder().setHeader(newErrorHeader(e)).build();
            log.error("getPartition exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * <pre>
     * 查找 HashCode 所属的分区
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
            response = Pdpb.GetPartitionResponse.newBuilder().setHeader(newErrorHeader(e)).build();
            log.error("getPartitionByCode exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * 根据 partition_id 查找 partition
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
                throw new PDException(Pdpb.ErrorType.NOT_FOUND_VALUE,
                                      String.format("partition: %s-%s not found",
                                                    request.getGraphName(),
                                                    request.getPartitionId()));
            }
            response = Pdpb.GetPartitionResponse.newBuilder().setHeader(okHeader)
                                                .setPartition(partShard.getPartition())
                                                .setLeader(partShard.getLeader()).build();
        } catch (PDException e) {
            response = Pdpb.GetPartitionResponse.newBuilder().setHeader(newErrorHeader(e)).build();
            log.error("getPartitionByID exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * <pre>
     * 更新分区信息，主要用来更新分区 key 范围，调用此接口需谨慎，否则会造成数据丢失。
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
            response =
                    Pdpb.UpdatePartitionResponse.newBuilder().setHeader(newErrorHeader(e)).build();
            log.error("update partition exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * 根据 partition_id 查找 partition
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
            response = Pdpb.DelPartitionResponse.newBuilder().setHeader(newErrorHeader(e)).build();
            log.error("delPartition exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * 给定 key 范围查找所属的 partition 集合
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
            response =
                    Pdpb.ScanPartitionsResponse.newBuilder().setHeader(newErrorHeader(e)).build();
            log.error("scanPartitions exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * 获得图信息
     */
    @Override
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
                Pdpb.ResponseHeader header = Pdpb.ResponseHeader.newBuilder().setError(
                        Pdpb.Error.newBuilder().setType(Pdpb.ErrorType.NOT_FOUND).build()).build();
                response = Pdpb.GetGraphResponse.newBuilder().setHeader(header).build();
            }
        } catch (PDException e) {
            response = Pdpb.GetGraphResponse.newBuilder().setHeader(newErrorHeader(e)).build();
            log.error("getGraph exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * 修改图信息
     */
    @Override
    public void setGraph(Pdpb.SetGraphRequest request,
                         io.grpc.stub.StreamObserver<Pdpb.SetGraphResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getSetGraphMethod(), request, observer);
            return;
        }
        Pdpb.SetGraphResponse response = null;
        Metapb.Graph graph = request.getGraph();
        try {
            graph = partitionService.updateGraph(graph);
            response =
                    Pdpb.SetGraphResponse.newBuilder().setHeader(okHeader).setGraph(graph).build();
        } catch (PDException e) {
            log.error("setGraph exception: ", e);
            response = Pdpb.SetGraphResponse.newBuilder().setHeader(newErrorHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * 获得图信息
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
            response = Pdpb.DelGraphResponse.newBuilder().setHeader(newErrorHeader(e)).build();
            log.error("getGraph exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * <pre>
     * 根据条件查询分区信息，包括 Store、Graph 等条件
     * </pre>
     */
    @Override
    public void queryPartitions(Pdpb.QueryPartitionsRequest request,
                                io.grpc.stub.StreamObserver<Pdpb.QueryPartitionsResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getQueryPartitionsMethod(), request, observer);
            return;
        }
        //TODO 临时采用遍历方案，后续使用 rocksdb 存储时，通过 kv 索引实现
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
                        storeNodeService.getShardGroup(partition.getId()).getShardsList()
                                        .forEach(shard -> {
                                            if (shard.getStoreId() == storeId) {
                                                result.add(partition);
                                            }
                                        });
                    } catch (PDException e) {
                        log.error("query partitions error, req:{}, error:{}", request,
                                  e.getMessage());
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
     * 获取集群成员信息
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
            response = Pdpb.GetMembersResponse.newBuilder()
                                              .setHeader(newErrorHeader(-1, e.getMessage()))
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
            response = Pdpb.GetAllStoresResponse.newBuilder().setHeader(newErrorHeader(e)).build();
            log.error("getAllStores exception: ", e);
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * 读取 PD 配置
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
            response = Pdpb.GetPDConfigResponse.newBuilder().setHeader(newErrorHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * 修改 PD 配置
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
            if (request.getPdConfig().getShardCount() % 2 != 1) {
                // 副本数奇偶校验
                throw new PDException(Pdpb.ErrorType.SET_CONFIG_SHARD_COUNT_ERROR_VALUE,
                                      "shard count must be an odd number!");
            }
            if (request.getPdConfig().getShardCount() >
                storeNodeService.getActiveStores().size()) {
                // 不能大于活跃的 store 数量
                throw new PDException(Pdpb.ErrorType.SET_CONFIG_SHARD_COUNT_ERROR_VALUE,
                                      "shard count can't be greater than the number of active " +
                                      "stores!");
            }
            int oldShardCount = configService.getPDConfig().getShardCount();
            int newShardCount = request.getPdConfig().getShardCount();
            if (newShardCount > oldShardCount) {
                // 如果副本数增大，则检查 store 内部的资源是否够用
                if (!isResourceEnough(oldShardCount, newShardCount)) {
                    throw new PDException(Pdpb.ErrorType.SET_CONFIG_SHARD_COUNT_ERROR_VALUE,
                                          "There is not enough disk space left!");
                }

                if (!checkShardCount(newShardCount)) {
                    throw new PDException(Pdpb.ErrorType.SET_CONFIG_SHARD_COUNT_ERROR_VALUE,
                                          "the cluster can't support so many shard count!");
                }
            }
            configService.setPDConfig(request.getPdConfig());
            response = Pdpb.SetPDConfigResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            response = Pdpb.SetPDConfigResponse.newBuilder().setHeader(newErrorHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * 读取图空间配置
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
            response = Pdpb.GetGraphSpaceResponse.newBuilder().setHeader(newErrorHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * 修改图空间配置
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
            response = Pdpb.SetGraphSpaceResponse.newBuilder().setHeader(newErrorHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * <pre>
     * 数据分裂
     * </pre>
     */
    @Override
    public void splitData(Pdpb.SplitDataRequest request,
                          StreamObserver<Pdpb.SplitDataResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getSplitDataMethod(), request, observer);
            return;
        }
        logService.insertLog(LogService.PARTITION_CHANGE, "splitData", request);
        Pdpb.SplitDataResponse response = null;
        try {
            taskService.splitPartition(request.getMode(), request.getParamList());
            response = Pdpb.SplitDataResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            log.error("splitData exception {}", e);
            response = Pdpb.SplitDataResponse.newBuilder().setHeader(newErrorHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();

    }

    @Override
    public void splitGraphData(Pdpb.SplitGraphDataRequest request,
                               StreamObserver<Pdpb.SplitDataResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getSplitGraphDataMethod(), request, observer);
            return;
        }
        logService.insertLog(LogService.PARTITION_CHANGE, "splitGraphData", request);
        Pdpb.SplitDataResponse response;
        try {
            partitionService.splitPartition(partitionService.getGraph(request.getGraphName()),
                                            request.getToCount());
            response = Pdpb.SplitDataResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            log.error("splitGraphData exception {}", e);
            response = Pdpb.SplitDataResponse.newBuilder().setHeader(newErrorHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * 在 store 之间平衡数据
     */
    @Override
    public void movePartition(Pdpb.MovePartitionRequest request,
                              StreamObserver<Pdpb.MovePartitionResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getMovePartitionMethod(), request, observer);
            return;
        }
        logService.insertLog(LogService.PARTITION_CHANGE, "balanceData", request);
        Pdpb.MovePartitionResponse response = null;
        try {
            taskService.patrolPartitions();
            taskService.balancePartitionShard();
            response = Pdpb.MovePartitionResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            log.error("transferData exception {}", e);
            response = Pdpb.MovePartitionResponse.newBuilder().setHeader(newErrorHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * <pre>
     * 获取集群健康状态
     * </pre>
     */
    @Override
    public void getClusterStats(Pdpb.GetClusterStatsRequest request,
                                io.grpc.stub.StreamObserver<Pdpb.GetClusterStatsResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getGetClusterStatsMethod(), request, observer);
            return;
        }
        Pdpb.GetClusterStatsResponse response = null;
        response = Pdpb.GetClusterStatsResponse.newBuilder().setHeader(okHeader)
                                               .setCluster(storeNodeService.getClusterStats())
                                               .build();
        observer.onNext(response);
        observer.onCompleted();
    }

    /**
     * <pre>
     * 汇报分区分裂等任务执行结果
     * </pre>
     */
    @Override
    public void reportTask(Pdpb.ReportTaskRequest request,
                           io.grpc.stub.StreamObserver<Pdpb.ReportTaskResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getReportTaskMethod(), request, observer);
            return;
        }
        try {
            taskService.reportTask(request.getTask());
        } catch (Exception e) {
            log.error("PDService.reportTask {}", e);
        }
        Pdpb.ReportTaskResponse response = null;
        response = Pdpb.ReportTaskResponse.newBuilder().setHeader(okHeader).build();
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
            log.error("getPartitionStats exception {}", e);
            response = Pdpb.GetPartitionStatsResponse.newBuilder().setHeader(newErrorHeader(e))
                                                     .build();
        }

        observer.onNext(response);
        observer.onCompleted();
    }

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

    /**
     * 更新 peerList
     */
    @Override
    public void changePeerList(Pdpb.ChangePeerListRequest request,
                               io.grpc.stub.StreamObserver<Pdpb.getChangePeerListResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getChangePeerListMethod(), request, observer);
            return;
        }
        Pdpb.getChangePeerListResponse response;
        try {
            Status status = RaftEngine.getInstance().changePeerList(request.getPeerList());
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
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public synchronized void onRaftLeaderChanged() {
        log.info("onLeaderChanged");
        // channel = null;
        // TODO: uncomment later
        //if (licenseVerifierService == null) {
        //    licenseVerifierService = new LicenseVerifierService(pdConfig);
        //}
        //licenseVerifierService.init();

        try {
            PDWatchSubject.notifyNodeChange(NodeEventType.NODE_EVENT_TYPE_PD_LEADER_CHANGE,
                                            RaftEngine.getInstance().getLeaderGrpcAddress(), 0L);
        } catch (ExecutionException | InterruptedException e) {
            log.error("failed to notice client", e);
        }
    }

    @Override
    public void balanceLeaders(Pdpb.BalanceLeadersRequest request,
                               StreamObserver<Pdpb.BalanceLeadersResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getBalanceLeadersMethod(), request, observer);
            return;
        }
        logService.insertLog(LogService.PARTITION_CHANGE, "balanceLeaders", request);
        Pdpb.BalanceLeadersResponse response = null;
        try {
            taskService.balancePartitionLeader(true);
            response = Pdpb.BalanceLeadersResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            log.error("balance Leaders exception: ", e);
            response =
                    Pdpb.BalanceLeadersResponse.newBuilder().setHeader(newErrorHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

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
            log.error("putLicense with error: {}", e);
            if (moved) {
                try {
                    FileUtils.moveFile(bakFile, licenseFile);
                } catch (IOException ex) {
                    log.error("failed to restore the license file.{}", ex);
                }
            }
            Pdpb.ResponseHeader header =
                    newErrorHeader(Pdpb.ErrorType.LICENSE_ERROR_VALUE, e.getMessage());
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
                // 只有已经被下线 (Tombstone) 的 store 可以被删除
                storeNodeService.removeStore(storeId);
                response = Pdpb.DetStoreResponse.newBuilder()
                                                .setHeader(okHeader)
                                                .setStore(store)
                                                .build();
            } else {
                throw new PDException(Pdpb.ErrorType.STORE_PROHIBIT_DELETION_VALUE,
                                      "the store can't be deleted, please check store state!");
            }
        } catch (PDException e) {
            log.error("delete store exception: {}", e);
            response = Pdpb.DetStoreResponse.newBuilder()
                                            .setHeader(newErrorHeader(e)).build();
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
            var maxCount = pdConfig.getPartition().getMaxShardsPerStore() *
                           storeNodeService.getActiveStores().size() /
                           pdConfig.getConfigService().getPartitionCount();

            if (newShardCount > maxCount) {
                log.error("new shard count :{} exceed current cluster max shard count {}",
                          newShardCount, maxCount);
                return false;
            }
        } catch (Exception e) {
            log.error("checkShardCount: {}", e.getMessage());
        }
        return true;
    }

    /**
     * 检查 store 资源是否够用
     */
    public boolean isResourceEnough(int oldShardCount, int newShardCount) {
        // 活跃的 store 的资源是否够用
        try {

            float expansionRatio = newShardCount / oldShardCount; // 占用的存储空间膨胀的倍数
            // 当前占用的空间
            long currentDataSize = 0L;
            // 数据膨胀后占用的空间
            long newDataSize = 0L;
            // 总的可用空间
            long totalAvaible = 0L;
            // 统计当前占用的存储空间
            for (Metapb.Store store : storeNodeService.getStores()) {
                List<Metapb.GraphStats> graphStatsList = store.getStats().getGraphStatsList();
                for (Metapb.GraphStats graphStats : graphStatsList) {
                    currentDataSize += graphStats.getApproximateSize();
                }
            }
            // 估计数据膨胀后占用的存储空间
            newDataSize = (long) Math.ceil(currentDataSize * expansionRatio);
            // 统计所有活跃的 store 里面可用的空间
            List<Metapb.Store> activeStores = storeNodeService.getActiveStores();
            for (Metapb.Store store : activeStores) {
                Metapb.StoreStats storeStats = store.getStats();
                totalAvaible += storeStats.getAvailable();
            }
            // 考虑当分区均匀分配的情况下，资源是否可用
            return totalAvaible > newDataSize - currentDataSize;
        } catch (PDException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * <pre>
     * 对 rocksdb 进行 compaction
     * </pre>
     */
    @Override
    public void dbCompaction(Pdpb.DbCompactionRequest request,
                             StreamObserver<Pdpb.DbCompactionResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getDbCompactionMethod(), request, observer);
            return;
        }
        logService.insertLog(LogService.TASK, "dbCompaction", request);
        Pdpb.DbCompactionResponse response = null;
        try {
            log.info("dbCompaction call dbCompaction");
            taskService.dbCompaction(request.getTableName());
            response = Pdpb.DbCompactionResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            log.error("dbCompaction exception {}", e);
            response = Pdpb.DbCompactionResponse.newBuilder().setHeader(newErrorHeader(e)).build();
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void combineCluster(Pdpb.CombineClusterRequest request,
                               StreamObserver<Pdpb.CombineClusterResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getCombineClusterMethod(), request, observer);
            return;
        }

        Pdpb.CombineClusterResponse response;

        try {
            partitionService.combinePartition(request.getToCount());
            response = Pdpb.CombineClusterResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            response =
                    Pdpb.CombineClusterResponse.newBuilder().setHeader(newErrorHeader(e)).build();
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void combineGraph(Pdpb.CombineGraphRequest request,
                             StreamObserver<Pdpb.CombineGraphResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getCombineGraphMethod(), request, observer);
            return;
        }

        Pdpb.CombineGraphResponse response;

        try {
            partitionService.combineGraphPartition(request.getGraphName(), request.getToCount());
            response = Pdpb.CombineGraphResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            response = Pdpb.CombineGraphResponse.newBuilder().setHeader(newErrorHeader(e)).build();
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
            response =
                    Pdpb.DeleteShardGroupResponse.newBuilder().setHeader(newErrorHeader(e)).build();
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
        // TODO
        try {
            Metapb.ShardGroup shardGroup = storeNodeService.getShardGroup(request.getGroupId());
            response = Pdpb.GetShardGroupResponse.newBuilder().setHeader(okHeader)
                                                 .setShardGroup(shardGroup).build();
        } catch (PDException e) {
            log.error("getPartitionStats exception", e);
            response = Pdpb.GetShardGroupResponse.newBuilder().setHeader(newErrorHeader(e)).build();
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
                                              group.getVersion(), group.getConfVer());
            response = Pdpb.UpdateShardGroupResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            log.error("updateShardGroup exception, ", e);
            response =
                    Pdpb.UpdateShardGroupResponse.newBuilder().setHeader(newErrorHeader(e)).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void updateShardGroupOp(Pdpb.ChangeShardRequest request,
                                   StreamObserver<Pdpb.ChangeShardResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getUpdateShardGroupOpMethod(), request, observer);
            return;
        }

        Pdpb.ChangeShardResponse response;

        try {
            storeNodeService.shardGroupOp(request.getGroupId(), request.getShardsList());
            response = Pdpb.ChangeShardResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            log.error("changeShard exception, ", e);
            response = Pdpb.ChangeShardResponse.newBuilder().setHeader(newErrorHeader(e)).build();
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void changeShard(Pdpb.ChangeShardRequest request,
                            StreamObserver<Pdpb.ChangeShardResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getChangeShardMethod(), request, observer);
            return;
        }

        Pdpb.ChangeShardResponse response;

        try {
            partitionService.changeShard(request.getGroupId(), request.getShardsList());
            response = Pdpb.ChangeShardResponse.newBuilder().setHeader(okHeader).build();
        } catch (PDException e) {
            log.error("changeShard exception, ", e);
            response = Pdpb.ChangeShardResponse.newBuilder().setHeader(newErrorHeader(e)).build();
        }

        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void updatePdRaft(Pdpb.UpdatePdRaftRequest request,
                             StreamObserver<Pdpb.UpdatePdRaftResponse> observer) {
        if (!isLeader()) {
            redirectToLeader(PDGrpc.getUpdatePdRaftMethod(), request, observer);
            return;
        }

        var list = parseConfig(request.getConfig());

        log.info("update raft request: {}, list: {}", request.getConfig(), list);

        Pdpb.UpdatePdRaftResponse response =
                Pdpb.UpdatePdRaftResponse.newBuilder().setHeader(okHeader).build();

        do {
            var leaders = list.stream().filter(s -> s.getKey().equals("leader"))
                              .collect(Collectors.toList());
            var node = RaftEngine.getInstance().getRaftNode();

            if (leaders.size() == 1) {
                var leaderPeer = leaders.get(0).getValue();
                // change leader
                var peers = new HashSet<>(node.listPeers());

                if (!peerEquals(leaderPeer, node.getLeaderId())) {
                    if (peers.contains(leaderPeer)) {
                        log.info("updatePdRaft, transfer to {}", leaderPeer);
                        node.transferLeadershipTo(leaderPeer);
                    } else {
                        response = Pdpb.UpdatePdRaftResponse.newBuilder()
                                                            .setHeader(newErrorHeader(6667,
                                                                                      "new leader" +
                                                                                      " not in " +
                                                                                      "raft peers"))
                                                            .build();
                    }
                    break;
                }
            } else {
                response = Pdpb.UpdatePdRaftResponse.newBuilder()
                                                    .setHeader(newErrorHeader(6666,
                                                                              "leader size != 1"))
                                                    .build();
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
                              status, status.getErrorMsg(), status.getCode(),
                              status.getRaftError());
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
            response = CacheResponse.newBuilder().setHeader(newErrorHeader(e)).build();
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
}
