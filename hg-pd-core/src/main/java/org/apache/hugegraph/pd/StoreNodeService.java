<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
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

========
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
package org.apache.hugegraph.pd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
========
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import org.apache.hugegraph.pd.common.Consts;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Metapb.GraphMode;
import org.apache.hugegraph.pd.grpc.Metapb.GraphModeReason;
import org.apache.hugegraph.pd.grpc.Metapb.GraphState;
import org.apache.hugegraph.pd.grpc.Pdpb.CacheResponse;
import org.apache.hugegraph.pd.grpc.common.ErrorType;
import org.apache.hugegraph.pd.grpc.pulse.ConfChangeType;
import org.apache.hugegraph.pd.listener.ShardGroupStatusListener;
import org.apache.hugegraph.pd.listener.StoreStatusListener;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.meta.MetadataKeyHelper;
import org.apache.hugegraph.pd.meta.StoreInfoMeta;
import org.apache.hugegraph.pd.meta.TaskInfoMeta;
import com.google.gson.Gson;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Metapb.GraphMode;
import org.apache.hugegraph.pd.grpc.Metapb.GraphModeReason;
import org.apache.hugegraph.pd.grpc.Metapb.GraphState;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.Pdpb.CacheResponse;
import org.apache.hugegraph.pd.grpc.pulse.ConfChangeType;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.meta.MetadataKeyHelper;
import org.apache.hugegraph.pd.meta.StoreInfoMeta;
import org.apache.hugegraph.pd.meta.TaskInfoMeta;

import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;

/**
 * Hg Store registration and keep-alive management
 */
@Slf4j
public class StoreNodeService {

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
========
    // Store状态监听
    private List<StoreStatusListener> statusListeners;
    private List<ShardGroupStatusListener> shardGroupStatusListeners;

    private PartitionService partitionService;
    @Getter
    private StoreInfoMeta storeInfoMeta;
    @Getter
    private TaskInfoMeta taskInfoMeta;
    private Random random = new Random(System.currentTimeMillis());
    private Map<Integer, Metapb.ClusterStats> clusterStats = new ConcurrentHashMap<>();
    private KvService kvService;
    private ConfigService configService;
    private PDConfig pdConfig;
    private static Metapb.ClusterStats statsNotReady =
            Metapb.ClusterStats.newBuilder().setState(Metapb.ClusterState.Cluster_Not_Ready).build();

>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
    private static final Long STORE_HEART_BEAT_INTERVAL = 30000L;
    private static final String graphSpaceConfPrefix = "HUGEGRAPH/hg/GRAPHSPACE/CONF/";
    private final List<StoreStatusListener> statusListeners;
    private final List<ShardGroupStatusListener> shardGroupStatusListeners;
    private final StoreInfoMeta storeInfoMeta;
    private final TaskInfoMeta taskInfoMeta;
    private final Random random = new Random(System.currentTimeMillis());
    private final KvService kvService;
    private final ConfigService configService;
    private final PDConfig pdConfig;
    private PartitionService partitionService;
    private final Runnable quotaChecker = () -> {
        try {
            getQuota();
        } catch (Exception e) {
            log.error(
                    "obtaining and sending graph space quota information with error: ",
                    e);
        }
    };
    private Metapb.ClusterStats clusterStats;

    public StoreNodeService(PDConfig config) {
        this.pdConfig = config;
        storeInfoMeta = MetadataFactory.newStoreInfoMeta(pdConfig);
        taskInfoMeta = MetadataFactory.newTaskInfoMeta(pdConfig);
        shardGroupStatusListeners = Collections.synchronizedList(new ArrayList<>());
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        statusListeners = Collections.synchronizedList(new ArrayList<StoreStatusListener>());
        clusterStats = Metapb.ClusterStats.newBuilder()
                                          .setState(Metapb.ClusterState.Cluster_Not_Ready)
                                          .setTimestamp(System.currentTimeMillis())
                                          .build();
        kvService = new KvService(pdConfig);
        configService = new ConfigService(pdConfig);
========
        statusListeners = Collections.synchronizedList(new ArrayList<>());
        configService = new ConfigService(pdConfig);
        kvService = new KvService(pdConfig);

        try {
            for (var group: configService.getAllStoreGroup()) {
                clusterStats.put(group.getGroupId(), getDefaultClusterStats());
            }
        } catch (PDException e) {
            log.error("init exception: ", e);
        }

    }

    private Metapb.ClusterStats getDefaultClusterStats() {
        return Metapb.ClusterStats.newBuilder()
                .setState(Metapb.ClusterState.Cluster_Not_Ready)
                .setTimestamp(System.currentTimeMillis())
                .build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
    }

    public void init(PartitionService partitionService) {
        this.partitionService = partitionService;
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        partitionService.addStatusListener(new PartitionStatusListener() {
            @Override
            public void onPartitionChanged(Metapb.Partition old, Metapb.Partition partition) {
                if (old != null && old.getState() != partition.getState()) {
                    try {
                        List<Metapb.Partition> partitions =
                                partitionService.getPartitionById(partition.getId());
                        Metapb.PartitionState state = Metapb.PartitionState.PState_Normal;
                        for (Metapb.Partition pt : partitions) {
                            if (pt.getState().getNumber() > state.getNumber()) {
                                state = pt.getState();
                            }
                        }
                        updateShardGroupState(partition.getId(), state);

                        for (Metapb.ShardGroup group : getShardGroups()) {
                            if (group.getState().getNumber() > state.getNumber()) {
                                state = group.getState();
                            }
                        }
                        updateClusterStatus(state);
                    } catch (PDException e) {
                        log.error("onPartitionChanged exception: ", e);
                    }
                }
            }

            @Override
            public void onPartitionRemoved(Metapb.Partition partition) {

            }
        });
========
//        partitionService.addStatusListener(new PartitionStatusListener() {
//            @Override
//            public void onPartitionChanged(Metapb.Partition old, Metapb.Partition partition) {
//                if (old != null && old.getState() != partition.getState()) {
//                    // 状态改变，重置集群状态
//                    try {
//                        List<Metapb.Partition> partitions = partitionService.getPartitionById(partition.getId());
//                        Metapb.PartitionState state = Metapb.PartitionState.PState_Normal;
//                        for(Metapb.Partition pt : partitions){
//                            if ( pt.getState().getNumber() > state.getNumber()) {
//                                state = pt.getState();
//                            }
//                        }
//                        updateShardGroupState(partition.getId(), state);
//
//                        for(Metapb.ShardGroup group : getShardGroups()){
//                            if ( group.getState().getNumber() > state.getNumber())
//                                state = group.getState();
//                        }
//
//                        updateClusterStatus(state);
//                    } catch (PDException e) {
//                        log.error("onPartitionChanged exception: ", e);
//                    }
//                }
//            }
//
//            @Override
//            public void onPartitionRemoved(Metapb.Partition partition) {
//
//            }
//        });
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
    }

    /**
     * Whether the cluster is ready or not
     *
     * @return
     */
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
    public boolean isOK() {
        return this.clusterStats.getState().getNumber() <
               Metapb.ClusterState.Cluster_Offline.getNumber();
========
    public boolean isOK(int storeGroup){
        if (! this.clusterStats.containsKey(storeGroup)) {
            return false;
        }
        return this.clusterStats.get(storeGroup).getState().getNumber() <
                Metapb.ClusterState.Cluster_Offline.getNumber();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
    }

    /**
     * Store registration, record the IP address of the Store, and the first registration needs
     * to generate a store_ID
     *
     * @param store
     */
    public Metapb.Store register(Metapb.Store store) throws PDException {
        if (store.getId() == 0) {
            // Initial registration, generate a new ID, and ensure that the ID is not duplicated.
            store = newStoreNode(store);
        }

        if (!storeInfoMeta.storeExists(store.getId())) {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
            log.error("Store id {} does not belong to this PD, address = {}", store.getId(),
                      store.getAddress());
            // storeId does not exist, an exception is thrown
            throw new PDException(Pdpb.ErrorType.STORE_ID_NOT_EXIST_VALUE,
                                  String.format("Store id %d doest not exist.", store.getId()));
========
            log.error("Store id {} does not belong to this PD, address = {}", store.getId(), store.getAddress());
            // storeId不存在，抛出异常
            throw new PDException(ErrorType.STORE_ID_NOT_EXIST_VALUE,
                    String.format("Store id %d doest not exist.", store.getId()));
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        }

        // If the store status is Tombstone, the registration is denied.
        Metapb.Store lastStore = storeInfoMeta.getStore(store.getId());
        if (lastStore.getState() == Metapb.StoreState.Tombstone) {
            log.error("Store id {} has been removed, Please reinitialize, address = {}",
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
                      store.getId(), store.getAddress());
            // storeId does not exist, an exception is thrown
            throw new PDException(Pdpb.ErrorType.STORE_HAS_BEEN_REMOVED_VALUE,
                                  String.format("Store id %d has been removed. %s", store.getId(),
                                                store.getAddress()));
========
                    store.getId(), store.getAddress());
            // storeId不存在，抛出异常
            throw new PDException(ErrorType.STORE_HAS_BEEN_REMOVED_VALUE,
                    String.format("Store id %d has been removed. %s", store.getId(), store.getAddress()));
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        }

        // offline or up, or in the initial activation list, go live automatically
        Metapb.StoreState storeState = lastStore.getState();
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        if (storeState == Metapb.StoreState.Offline || storeState == Metapb.StoreState.Up
            || inInitialStoreList(store)) {
========
        if (storeState == Metapb.StoreState.Offline || storeState == Metapb.StoreState.Up || inInitialStoreList(store)){
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
            storeState = Metapb.StoreState.Up;
        } else {
            storeState = Metapb.StoreState.Pending;
        }

        store = Metapb.Store.newBuilder(lastStore)
                            .setAddress(store.getAddress())
                            .setRaftAddress(store.getRaftAddress())
                            .setDataVersion(store.getDataVersion())
                            .setDeployPath(store.getDeployPath())
                            .setVersion(store.getVersion())
                            .setDataPath(store.getDataPath())
                            .setState(storeState).setCores(store.getCores())
                            .clearLabels().addAllLabels(store.getLabelsList())
                            .setLastHeartbeat(System.currentTimeMillis()).build();

        long current = System.currentTimeMillis();
        boolean raftChanged = false;
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        // On-line status Raft Address there has been a change
        if (!Objects.equals(lastStore.getRaftAddress(), store.getRaftAddress()) &&
            storeState == Metapb.StoreState.Up) {
            // If the time interval is too short and the raft changes, it is considered an
            // invalid store
            if (current - lastStore.getLastHeartbeat() < STORE_HEART_BEAT_INTERVAL * 0.8) {
                throw new PDException(Pdpb.ErrorType.STORE_PROHIBIT_DUPLICATE_VALUE,
                                      String.format("Store id %d may be duplicate. addr: %s",
                                                    store.getId(), store.getAddress()));
            } else if (current - lastStore.getLastHeartbeat() > STORE_HEART_BEAT_INTERVAL * 1.2) {
                // It is considered that a change has occurred
========
        // 上线状态的Raft Address 发生了变更
        if (!Objects.equals(lastStore.getRaftAddress(), store.getRaftAddress()) && storeState == Metapb.StoreState.Up) {
            // 时间间隔太短，而且raft有变更，则认为是无效的store
            if (current - lastStore.getLastHeartbeat() < STORE_HEART_BEAT_INTERVAL * 0.8){
                throw new PDException(ErrorType.STORE_PROHIBIT_DUPLICATE_VALUE,
                        String.format("Store id %d may be duplicate. addr: %s", store.getId(), store.getAddress()));
            } else if(current - lastStore.getLastHeartbeat() > STORE_HEART_BEAT_INTERVAL * 1.2 ) {
                // 认为发生了变更
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
                raftChanged = true;
            } else {
                // Wait for the next registration
                return Metapb.Store.newBuilder(store).setId(0L).build();
            }
        }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        // Store information
========
        // 兼容性处理，如果在初始化列表中，则自动插入storeGroup
        if (inInitialStoreList(store) && ! isStoreHasStoreGroup(store.getId())) {
            int groupId = this.pdConfig.getInitialStoreGroup(store.getAddress());
            updateStoreGroupRelation(store.getId(), groupId);
        }

        // 存储store信息
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        storeInfoMeta.updateStore(store);
        if (storeState == Metapb.StoreState.Up) {
            // Update the store active status
            storeInfoMeta.keepStoreAlive(store);
            onStoreStatusChanged(store, Metapb.StoreState.Offline, Metapb.StoreState.Up);
            checkStoreStatus(storeInfoMeta.getStoreGroupByStoreId(store.getId()));
        }

        // Wait for the store information to be saved before sending the changes
        if (raftChanged) {
            onStoreRaftAddressChanged(store);
        }

        log.info("Store register, id = {} {}", store.getId(), store);
        return store;
    }

    private boolean inInitialStoreList(Metapb.Store store) {
        return this.pdConfig.getInitialStoreMap().containsKey(store.getAddress());
    }

    /**
     * Creates a new store object
     *
     * @param store
     * @return
     * @throws PDException
     */
    private synchronized Metapb.Store newStoreNode(Metapb.Store store) throws PDException {
        long id = random.nextLong() & Long.MAX_VALUE;
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        while (id == 0 || storeInfoMeta.storeExists(id)) {
========
        while( id == 0 || storeInfoMeta.storeExists(id) ) {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
            id = random.nextLong() & Long.MAX_VALUE;
        }
        store = Metapb.Store.newBuilder(store)
                            .setId(id)
                            .setState(Metapb.StoreState.Pending)
                            .setStartTimestamp(System.currentTimeMillis()).build();
        storeInfoMeta.updateStore(store);
        return store;
    }

    /**
     * Returns Store information based on store_id
     *
     * @param id
     * @return
     * @throws PDException
     */
    public Metapb.Store getStore(long id) throws PDException {
        Metapb.Store store = storeInfoMeta.getStore(id);
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        if (store == null) {
            throw new PDException(Pdpb.ErrorType.STORE_ID_NOT_EXIST_VALUE,
                                  String.format("Store id %x doest not exist.", id));
========
        if ( store == null ) {
            throw new PDException(ErrorType.STORE_ID_NOT_EXIST_VALUE,
                    String.format("Store id %x doest not exist.", id));
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        }
        return store;
    }

    /**
     * Update the store information, detect the change of store status, and notify Hugestore
     */
    public synchronized Metapb.Store updateStore(Metapb.Store store) throws PDException {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        log.info("updateStore storeId: {}, address: {}, state: {}", store.getId(),
                 store.getAddress(), store.getState());
========
        log.info("updateStore storeId: {}, address: {}, state: {}",
                store.getId(), store.getAddress(), store.getState());
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        Metapb.Store lastStore = storeInfoMeta.getStore(store.getId());
        if (lastStore == null) {
            return null;
        }
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        Metapb.Store.Builder builder =
                Metapb.Store.newBuilder(lastStore).clearLabels().clearStats();
========

        Metapb.Store.Builder builder = Metapb.Store.newBuilder(lastStore).clearLabels().clearStats();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        store = builder.mergeFrom(store).build();
        if (store.getState() == Metapb.StoreState.Tombstone) {
            List<Metapb.Store> activeStores = getStores(getStoreInfoMeta().getStoreGroupByStoreId(store.getId()));
            if (lastStore.getState() == Metapb.StoreState.Up
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
                && activeStores.size() - 1 < pdConfig.getMinStoreCount()) {
                throw new PDException(Pdpb.ErrorType.LESS_ACTIVE_STORE_VALUE,
                                      "The number of active stores is less then " +
                                      pdConfig.getMinStoreCount());
========
                    && activeStores.size() - 1 < pdConfig.getMinStoreCount()) {
                throw new PDException(ErrorType.LESS_ACTIVE_STORE_VALUE,
                        "The number of active stores is less then " + pdConfig.getMinStoreCount());
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
            }
        }

        storeInfoMeta.updateStore(store);
        if (store.getState() != Metapb.StoreState.Unknown &&
            store.getState() != lastStore.getState()) {
            // If you want to take the store offline
            if (store.getState() == Metapb.StoreState.Exiting) {
                if (lastStore.getState() == Metapb.StoreState.Exiting) {
                    // If it is already in the offline state, no further processing will be made
                    return lastStore;
                }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
                List<Metapb.Store> activeStores = this.getActiveStores();
                Map<Long, Metapb.Store> storeMap = new HashMap<>();
                activeStores.forEach(s -> {
                    storeMap.put(s.getId(), s);
                });
                // If the store is offline, delete it directly from active, and if the store is
                // online, temporarily delete it from active, and then delete it when the status
                // is set to Tombstone
========
//                List<Metapb.Store> activeStores = this.getActiveStores(getStoreInfoMeta().getStoreGroup(store.getId()));
//                Map<Long, Metapb.Store> storeMap = new HashMap<>();
//                activeStores.forEach(s -> {
//                    storeMap.put(s.getId(), s);
//                });

                var storeMap = getActiveStoresByStoreGroup(getStoreInfoMeta().getStoreGroupByStoreId(store.getId()))
                            .stream()
                            .collect(Collectors.toMap(Metapb.Store::getId, store1 -> store1));

                //如果store已经离线，直接从活跃中删除，如果store在线，暂时不从活跃中删除，等把状态置成Tombstone的时候再删除
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
                if (!storeMap.containsKey(store.getId())) {
                    log.info("updateStore removeActiveStores store {}", store.getId());
                    storeInfoMeta.removeActiveStore(store);
                }
                storeTurnoff(store);
            } else if (store.getState() == Metapb.StoreState.Offline) {
                // Monitor that the store has gone offline and is removed from the active
                storeInfoMeta.removeActiveStore(store);
            } else if (store.getState() == Metapb.StoreState.Tombstone) {
                // When the status changes, the store is shut down, the shardGroup is modified,
                // and the replica is migrated
                log.info("updateStore removeActiveStores store {}", store.getId());
                storeInfoMeta.removeActiveStore(store);
                // Storage goes offline
                storeTurnoff(store);
            } else if (store.getState() == Metapb.StoreState.Up) {
                storeInfoMeta.keepStoreAlive(store);
                checkStoreStatus(storeInfoMeta.getStoreGroupByStoreId(store.getId()));
            }
            onStoreStatusChanged(lastStore, lastStore.getState(), store.getState());
        }
        return store;
    }

    /**
     * The shard of the shardGroup is reassigned
     *
     * @param store
     * @throws PDException
     */
    public synchronized void storeTurnoff(Metapb.Store store) throws PDException {
        // Traverse ShardGroup,redistribution
        for (Metapb.ShardGroup group : getShardGroupsByStore(store.getId())) {
            Metapb.ShardGroup.Builder builder = Metapb.ShardGroup.newBuilder(group);
            builder.clearShards();
            group.getShardsList().forEach(shard -> {
                if (shard.getStoreId() != store.getId()) {
                    builder.addShards(shard);
                }
            });
            reallocShards(builder.build());
        }
    }

    /**
     * Returns stores information based on the graph name, and if graphName is empty, all store
     * information is returned
     *
     * @throws PDException
     */
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
    public List<Metapb.Store> getStores() throws PDException {
        return storeInfoMeta.getStores(null);
    }

    public List<Metapb.Store> getStores(String graphName) throws PDException {
        return storeInfoMeta.getStores(graphName);
========
    public List<Metapb.Store> getStores() throws PDException{
        return storeInfoMeta.getAllStores();
    }

    public List<Metapb.Store> getStores(String graphName) throws PDException {
        Metapb.Graph graph = partitionService.getGraph(graphName);
        return graph == null ? getStores()  : getStoresByStoreGroup(graph.getStoreGroupId());
    }

    public List<Metapb.Store> getStores(int storeGroupId) throws PDException{
        Set<Long> set = storeInfoMeta.getStoreIdsByGroup(storeGroupId);
        return storeInfoMeta.getAllStores().stream()
                .filter(store -> set.contains(store.getId()))
                .collect(Collectors.toList());
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
    }

    public List<Metapb.Store> getStoreStatus(boolean isActive) throws PDException {
        return storeInfoMeta.getStoreStatus(isActive);
    }

    public List<Metapb.ShardGroup> getShardGroups() throws PDException {
        return storeInfoMeta.getShardGroups();
    }

    public List<Metapb.ShardGroup> getShardGroups(int storeGroup) throws PDException {
        Set<Long> storeIds = storeInfoMeta.getStoreIdsByGroup(storeGroup);
        return storeInfoMeta.getShardGroups().stream().filter(shardGroup -> {
                for (var shard : shardGroup.getShardsList()) {
                    if (storeIds.contains(shard.getStoreId())) {
                        return true;
                    }
                }
                return false;
            }).collect(Collectors.toList());
    }

    public Metapb.ShardGroup getShardGroup(int groupId) throws PDException {
        return storeInfoMeta.getShardGroup(groupId);
    }

    public List<Metapb.Shard> getShardList(int groupId) throws PDException {
        var shardGroup = getShardGroup(groupId);
        if (shardGroup != null) {
            return shardGroup.getShardsList();
        }
        return new ArrayList<>();
    }

    public List<Metapb.ShardGroup> getShardGroupsByStore(long storeId) throws PDException {
        List<Metapb.ShardGroup> shardGroups = new ArrayList<>();
        storeInfoMeta.getShardGroups().forEach(shardGroup -> {
            shardGroup.getShardsList().forEach(shard -> {
                if (shard.getStoreId() == storeId) {
                    shardGroups.add(shardGroup);
                }
            });
        });
        return shardGroups;
    }

    /**
     * Returns the active store
     *
     * @param graphName
     * @return
     * @throws PDException
     */
    public List<Metapb.Store> getActiveStores(String graphName) throws PDException {
        // todo:
        Metapb.Graph graph = partitionService.getGraph(graphName);
        return graph == null ? List.of() : getActiveStoresByStoreGroup(graph.getStoreGroupId());
    }

    public List<Metapb.Store> getActiveStores() throws PDException {
        return storeInfoMeta.getActiveStores();
    }

    public List<Metapb.Store> getActiveStoresByStoreGroup(int storeGroupId) throws PDException {
        Set<Long> ids = storeInfoMeta.getStoreIdsByGroup(storeGroupId);
        return storeInfoMeta.getActiveStores()
                .stream()
                .filter(store -> ids.contains(store.getId()))
                .collect(Collectors.toList());
    }

    public List<Long> getActiveStoresByPartition(int partitionId) throws PDException {
        var shardGroup = getShardGroup(partitionId);
        if (shardGroup != null) {
            return shardGroup.getShardsList().stream().map(Metapb.Shard::getStoreId).collect(Collectors.toList());
        }
        return List.of();
    }

    public List<Metapb.Store> getTombStores() throws PDException {
        List<Metapb.Store> stores = new ArrayList<>();
        for (Metapb.Store store : this.getStores()) {
            if (store.getState() == Metapb.StoreState.Tombstone) {
                stores.add(store);
            }
        }
        return stores;
    }

    public long removeStore(Long storeId) throws PDException {
        return storeInfoMeta.removeStore(storeId);
    }

    /**
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
     * todo : New logic
     * Assign a store to the partition and decide how many peers to allocate according to the
     * configuration of the graph
     * After allocating all the shards, save the ShardGroup object (store does not change, only
     * executes once)
========
     * todo : 新逻辑
     * 给partition分配store，根据图的配置，决定分配几个peer
     * 分配完所有的shards，保存ShardGroup对象（store不变动，只执行一次）
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
     */
    public synchronized List<Metapb.Shard> allocShards(Metapb.Graph graph, int partId) throws
                                                                                       PDException {
        // Multiple graphs share raft grouping, so assigning shard only depends on partitionId.
        // The number of partitions can be set based on the size of the data, but the total
        // number cannot exceed the number of raft groups
        if (storeInfoMeta.getShardGroup(partId) == null) {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
            // Get active store key
            List<Metapb.Store> stores = storeInfoMeta.getActiveStores();

            if (stores.size() == 0) {
                throw new PDException(Pdpb.ErrorType.NO_ACTIVE_STORE_VALUE,
                                      "There is no any online store");
            }

            if (stores.size() < pdConfig.getMinStoreCount()) {
                throw new PDException(Pdpb.ErrorType.LESS_ACTIVE_STORE_VALUE,
                                      "The number of active stores is less then " +
                                      pdConfig.getMinStoreCount());
========
            // 获取活跃的store key
            // 根据 partionID计算store
            List<Metapb.Store> stores = storeInfoMeta.getActiveStores(graph.getStoreGroupId());

            if (stores.isEmpty()) {
                throw new PDException(ErrorType.NO_ACTIVE_STORE_VALUE, "There is no any online store");
            }

            var minStoreCount = Math.max(pdConfig.getMinStoreCount(), configService.getPDConfig().getShardCount());

            if (stores.size() < minStoreCount) {
                throw new PDException(ErrorType.LESS_ACTIVE_STORE_VALUE,
                        "The number of active stores is less then " + minStoreCount);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
            }

            // todo: 根据graph 配置的store group，获取 shard count
            int shardCount = pdConfig.getPartition().getShardCount();
            shardCount = Math.min(shardCount, stores.size());
            // Two shards could not elect a leader
            // It cannot be 0

            if (shardCount == 2 || shardCount < 1) {
                shardCount = 1;
            }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
            // All ShardGroups are created at one time to ensure that the initial groupIDs are
            // orderly and easy for humans to read
            for (int groupId = 0; groupId < pdConfig.getConfigService().getPartitionCount();
                 groupId++) {
                int storeIdx = groupId % stores.size();  // Assignment rules, simplified to modulo
========
            // todo: 获取partition count by group
            var partitionCount = pdConfig.getConfigService().getPartitionCount(graph.getStoreGroupId());
            int baseId = partId / Consts.PARTITION_GAP * Consts.PARTITION_GAP;
            // 一次创建完所有的ShardGroup，保证初始的groupID有序，方便人工阅读
            for (int groupId = 0; groupId <partitionCount; groupId++) {
                int storeIdx = groupId % stores.size();  //store分配规则，简化为取模
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
                List<Metapb.Shard> shards = new ArrayList<>();
                for (int i = 0; i < shardCount; i++) {
                    Metapb.Shard shard =
                            Metapb.Shard.newBuilder().setStoreId(stores.get(storeIdx).getId())
                                        .setRole(i == 0 ? Metapb.ShardRole.Leader :
                                                 Metapb.ShardRole.Follower) //
                                        .build();
                    shards.add(shard);
                    storeIdx = (storeIdx + 1) >= stores.size() ? 0 : ++storeIdx; // Sequential
                }

                Metapb.ShardGroup group = Metapb.ShardGroup.newBuilder()
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
                                                           .setId(groupId)
                                                           .setState(
                                                                   Metapb.PartitionState.PState_Normal)
                                                           .addAllShards(shards).build();
========
                        .setId(groupId + baseId)
                        .setState(Metapb.PartitionState.PState_Normal)
                        .addAllShards(shards).build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java

                // new group
                storeInfoMeta.updateShardGroup(group);
                updateShardGroupCache(group);
                onShardGroupStatusChanged(null, group);
                log.info("alloc shard group: id {}", groupId + baseId);
            }
        }

        return storeInfoMeta.getShardGroup(partId).getShardsList();
    }

    /**
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
     * Based on the shard_count of the graph, reallocate shards
     * Send change shard
     */
    public synchronized List<Metapb.Shard> reallocShards(Metapb.ShardGroup shardGroup) throws
                                                                                       PDException {
        List<Metapb.Store> stores = storeInfoMeta.getActiveStores();

        if (stores.size() == 0) {
            throw new PDException(Pdpb.ErrorType.NO_ACTIVE_STORE_VALUE,
                                  "There is no any online store");
        }

        if (stores.size() < pdConfig.getMinStoreCount()) {
            throw new PDException(Pdpb.ErrorType.LESS_ACTIVE_STORE_VALUE,
                                  "The number of active stores is less then " +
                                  pdConfig.getMinStoreCount());
========
     * todo : 新逻辑
     * 根据graph的shard_count，重新分配shard
     * 发送变更change shard指令
     */
    public synchronized List<Metapb.Shard> reallocShards(Metapb.ShardGroup shardGroup) throws PDException {
        // todo：检查 shard group在哪个store group里面, 以及 shard group 对应的partition count
        // todo: store group 内部分组
        int storeGroup = getShardGroupBelongsToStoreGroup(shardGroup);
        List<Metapb.Store> stores = storeInfoMeta.getActiveStores(storeGroup);

        if (stores.isEmpty()) {
            throw new PDException(ErrorType.NO_ACTIVE_STORE_VALUE, "There is no any online store");
        }

        if (stores.size() < pdConfig.getMinStoreCount()) {
            throw new PDException(ErrorType.LESS_ACTIVE_STORE_VALUE,
                    "The number of active stores is less then " + pdConfig.getMinStoreCount());
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        }

        // todo: check partition count by store group
        int shardCount = pdConfig.getPartition().getShardCount();
        shardCount = Math.min(shardCount, stores.size());
        if (shardCount == 2 || shardCount < 1) {
            // Two shards could not elect a leader
            // It cannot be 0
            shardCount = 1;
        }

        List<Metapb.Shard> shards = new ArrayList<>(shardGroup.getShardsList());

        if (shardCount > shards.size()) {
            // Need to add shards
            log.info("reallocShards ShardGroup {}, add shards from {} to {}",
                     shardGroup.getId(), shards.size(), shardCount);
            int storeIdx = shardGroup.getId() % stores.size();
            for (int addCount = shardCount - shards.size(); addCount > 0; ) {
                // Check if it already exists
                if (!isStoreInShards(shards, stores.get(storeIdx).getId())) {
                    Metapb.Shard shard = Metapb.Shard.newBuilder()
                                                     .setStoreId(stores.get(storeIdx).getId())
                                                     .build();
                    shards.add(shard);
                    addCount--;
                }
                storeIdx = (storeIdx + 1) >= stores.size() ? 0 : ++storeIdx;
            }
        } else if (shardCount < shards.size()) {
            // Need to reduce shard
            log.info("reallocShards ShardGroup {}, remove shards from {} to {}",
                     shardGroup.getId(), shards.size(), shardCount);

            int subCount = shards.size() - shardCount;
            Iterator<Metapb.Shard> iterator = shards.iterator();
            while (iterator.hasNext() && subCount > 0) {
                if (iterator.next().getRole() != Metapb.ShardRole.Leader) {
                    iterator.remove();
                    subCount--;
                }
            }
        } else {
            return shards;
        }

        Metapb.ShardGroup group = Metapb.ShardGroup.newBuilder(shardGroup)
                                                   .clearShards()
                                                   .addAllShards(shards).build();
        storeInfoMeta.updateShardGroup(group);
        updateShardGroupCache(group);
        // change shard group
        // onShardGroupStatusChanged(shardGroup, group);

        var partitions = partitionService.getPartitionById(shardGroup.getId());
        if (!partitions.isEmpty()) {
            // send one message, change shard is regardless with partition/graph
            partitionService.fireChangeShard(partitions.get(0), shards,
                                             ConfChangeType.CONF_CHANGE_TYPE_ADJUST);
        }

        log.info("reallocShards ShardGroup {}, shards: {}", group.getId(), group.getShardsList());
        return shards;
    }

    /**
     * According to the number of partitions, distribute group shard
     *
     * @param groups list of (partition id, count)
     * @return total groups
     */
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
    public synchronized int splitShardGroups(List<KVPair<Integer, Integer>> groups) throws
                                                                                    PDException {
        int sum = groups.stream().map(pair -> pair.getValue()).reduce(0, Integer::sum);
        // shard group is too big
        if (sum > getActiveStores().size() * pdConfig.getPartition().getMaxShardsPerStore()) {
            throw new PDException(Pdpb.ErrorType.Too_Many_Partitions_Per_Store_VALUE,
                                  "can't satisfy target shard group count");
========
    public synchronized int splitShardGroups(List<KVPair<Integer, Integer>> groups) throws PDException {
        // 1. 检查所有的groups是否属于一个 store group
        Set<Integer> storeGroups = new HashSet<>();

        for (var group : groups) {
            var shardGroup = storeInfoMeta.getShardGroup(group.getKey());
            if (shardGroup == null){
                throw new PDException(ErrorType.SHARD_GROUPS_NOT_EXISTS);
            }
            storeGroups.add(getShardGroupBelongsToStoreGroup(shardGroup));
        }
        assert storeGroups.size() == 1;

        int storeGroup = storeGroups.iterator().next();
        int sum = groups.stream().map(KVPair::getValue).reduce(0, Integer::sum);

        // 2. 检查split后的count, 增加的 + 原有的
        int newCount = (sum  - groups.size()) + getShardGroups(storeGroup).size();

        // shard group 太大
        if (newCount > getActiveStoresByStoreGroup(storeGroup).size() * pdConfig.getPartition().getMaxShardsPerStore()){
            throw new PDException(ErrorType.Too_Many_Partitions_Per_Store);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        }

        partitionService.splitPartition(groups);

        return newCount;
    }

    /**
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
     * Alloc shard group, prepare for the split
     *
========
     * 分配shard group，为分裂做准备
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
     * @param
     * @return true
     * @throws PDException
     */
    private boolean isStoreInShards(List<Metapb.Shard> shards, long storeId) {
        AtomicBoolean exist = new AtomicBoolean(false);
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        shards.forEach(s -> {
            if (s.getStoreId() == storeId) {
========
        shards.forEach(s->{
            if (s.getStoreId() == storeId ) {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
                exist.set(true);
            }
        });
        return exist.get();
    }

    /**
     * update shard group and cache.
     * send shard group change message.
     *
     * @param groupId     : shard group
     * @param shards      : shard lists
     * @param version:    term version, ignored if less than 0
     * @param confVersion : conf version, ignored if less than 0
     * @return
     */
    public synchronized Metapb.ShardGroup updateShardGroup(int groupId, List<Metapb.Shard> shards,
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
                                                           long version, long confVersion) throws
                                                                                           PDException {
========
                                                           long version, long confVersion) throws PDException {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        Metapb.ShardGroup group = this.storeInfoMeta.getShardGroup(groupId);

        if (group == null) {
            return null;
        }

        var builder = Metapb.ShardGroup.newBuilder(group);
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        if (version >= 0) {
            builder.setVersion(version);
        }

        if (confVersion >= 0) {
            builder.setConfVer(confVersion);
        }

        var newGroup = builder.clearShards().addAllShards(shards).build();

        storeInfoMeta.updateShardGroup(newGroup);
        partitionService.updateShardGroupCache(newGroup);
        onShardGroupStatusChanged(group, newGroup);
        log.info("Raft {} updateShardGroup {}", groupId, newGroup);
========
        if (version >= 0){
            builder.setVersion(version);
        }

        if (confVersion >= 0){
            builder.setConfVer(confVersion);
        }

        var newGroup = builder.clearShards() .addAllShards(shards) .build();

        storeInfoMeta.updateShardGroup(newGroup);
        updateShardGroupCache(newGroup);
        onShardGroupStatusChanged(group, newGroup);
        // log.info("Raft {} updateShardGroup {}", groupId, newGroup);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        return group;
    }

    /**
     * Notify the Store to rebuild the shard group
     *
     * @param groupId raft group id
     * @param shards  shard list: If it is empty, delete the corresponding one partition engine
     */
    public void shardGroupOp(int groupId, List<Metapb.Shard> shards) throws PDException {

        var shardGroup = getShardGroup(groupId);

        if (shardGroup == null) {
            return;
        }

        var newGroup = shardGroup.toBuilder().clearShards().addAllShards(shards).build();
        if (shards.isEmpty()) {
            var partitions = partitionService.getPartitionById(groupId);
            for (var partition : partitions) {
                partitionService.removePartition(partition.getGraphName(), groupId);
            }
            deleteShardGroup(groupId);
        }

        onShardGroupOp(newGroup);
    }

    /**
     * Delete shard group
     *
     * @param groupId shard group id
     */
    public synchronized void deleteShardGroup(int groupId) throws PDException {
        Metapb.ShardGroup group = this.storeInfoMeta.getShardGroup(groupId);
        int storeGroupId = getShardGroupBelongsToStoreGroup(group);

        if (group != null) {
            storeInfoMeta.deleteShardGroup(groupId);
        }

        onShardGroupStatusChanged(group, null);

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        // Fix the number of partitions for the store. (Result from partition merge)
        var shardGroups = getShardGroups();
        if (shardGroups != null) {
            var count1 = pdConfig.getConfigService().getPDConfig().getPartitionCount();
            var maxGroupId =
                    getShardGroups().stream().map(Metapb.ShardGroup::getId).max(Integer::compareTo);
            if (maxGroupId.get() < count1) {
                pdConfig.getConfigService().setPartitionCount(maxGroupId.get() + 1);
========
        // 修正store的分区数. (分区合并导致)
        var shardGroups = getShardGroups(storeGroupId);
        if (shardGroups != null) {
            var count1 = pdConfig.getConfigService().getPartitionCount(storeGroupId);
            var maxGroupId = getShardGroups(storeGroupId)
                    .stream().map(Metapb.ShardGroup::getId)
                    .max(Integer::compareTo);
            // 考虑分组的影响
            var groupId2 = maxGroupId.get() % Consts.PARTITION_GAP;
            if (groupId2 < count1) {
                configService.setPartitionCount(storeGroupId, groupId2 + 1);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
            }
        }

        // 没有数据的partition，可能上报不上来
        var partitions = partitionService.getPartitions().stream()
                .filter(partition -> partition.getId() == groupId)
                .collect(Collectors.toList());

        for (var partition : partitions) {
            partitionService.removePartition(partition.getGraphName(), groupId);
        }
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
    public synchronized void updateShardGroupState(int groupId, Metapb.PartitionState state) throws
                                                                                             PDException {
        Metapb.ShardGroup shardGroup = storeInfoMeta.getShardGroup(groupId)
                                                    .toBuilder()
                                                    .setState(state).build();
        storeInfoMeta.updateShardGroup(shardGroup);
        partitionService.updateShardGroupCache(shardGroup);
========
    // todo : update cluster state
    public synchronized void updateShardGroupState(int groupId, Metapb.PartitionState state) throws PDException {
        Metapb.ShardGroup shardGroup = storeInfoMeta.getShardGroup(groupId);

        if (state != shardGroup.getState()) {
            var newShardGroup = shardGroup.toBuilder().setState(state).build();
            storeInfoMeta.updateShardGroup(newShardGroup);

            updateShardGroupCache(newShardGroup);

            log.debug("update shard group {} state: {}", groupId, state);
        }

        // 检查集群的状态
        // todo : 更明确的集群状态定义
        Metapb.PartitionState clusterState = Metapb.PartitionState.PState_None;
        for(Metapb.ShardGroup group : getShardGroups()){
            if (group.getState().getNumber() > state.getNumber()) {
                clusterState = group.getState();
             }
        }

        var storeGroupId = getShardGroupBelongsToStoreGroup(shardGroup);
        updateClusterStatus(storeGroupId, clusterState);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
    }

    /**
     * Receive the heartbeat of the Store
     *
     * @param storeStats
     * @throws PDException
     */
    public Metapb.ClusterStats heartBeat(Metapb.StoreStats storeStats) throws PDException {
        this.storeInfoMeta.updateStoreStats(storeStats);
        Metapb.Store lastStore = this.getStore(storeStats.getStoreId());
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        if (lastStore == null) {
            // store does not exist
            throw new PDException(Pdpb.ErrorType.STORE_ID_NOT_EXIST_VALUE,
                                  String.format("Store id %d does not exist.",
                                                storeStats.getStoreId()));
        }
        if (lastStore.getState() == Metapb.StoreState.Tombstone) {
            throw new PDException(Pdpb.ErrorType.STORE_HAS_BEEN_REMOVED_VALUE,
                                  String.format(
                                          "Store id %d is useless since it's state is Tombstone",
                                          storeStats.getStoreId()));
        }
        Metapb.Store nowStore;
        // If you are going to take the store offline
        if (lastStore.getState() == Metapb.StoreState.Exiting) {
            List<Metapb.Store> activeStores = this.getActiveStores();
            Map<Long, Metapb.Store> storeMap = new HashMap<>();
            activeStores.forEach(store -> {
                storeMap.put(store.getId(), store);
            });
            // If the partition of the offline store is 0, it means that the migration has been
            // completed and can be taken offline, if it is not 0, the migration is still in
            // progress and you need to wait
            if (storeStats.getPartitionCount() > 0 &&
                storeMap.containsKey(storeStats.getStoreId())) {
========
        if (lastStore == null){
            //store不存在
            throw new PDException(ErrorType.STORE_ID_NOT_EXIST_VALUE,
                    String.format("Store id %d does not exist.", storeStats.getStoreId()));
        }
        if (lastStore.getState() == Metapb.StoreState.Tombstone){
            throw new PDException(ErrorType.STORE_HAS_BEEN_REMOVED_VALUE,
                    String.format("Store id %d is useless since it's state is Tombstone",
                            storeStats.getStoreId()));
        }
        Metapb.Store nowStore;
        // 如果正在做store下线操作
        if (lastStore.getState() == Metapb.StoreState.Exiting){
            var storeMap = this.getActiveStoresByStoreGroup(storeInfoMeta.getStoreGroupByStoreId(lastStore.getId()))
                    .stream().collect(Collectors.toMap(Metapb.Store::getId, store -> store));

            // 下线的store的分区为0，说明已经迁移完毕，可以下线，如果非0，则迁移还在进行，需要等待
            if (storeStats.getPartitionCount() > 0 && storeMap.containsKey(storeStats.getStoreId())){
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
                nowStore = Metapb.Store.newBuilder(lastStore)
                                       .setStats(storeStats)
                                       .setLastHeartbeat(System.currentTimeMillis())
                                       .setState(Metapb.StoreState.Exiting).build();
                this.storeInfoMeta.updateStore(nowStore);
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
                return this.clusterStats;
            } else {
========
            }else {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
                nowStore = Metapb.Store.newBuilder(lastStore)
                                       .setStats(storeStats)
                                       .setLastHeartbeat(System.currentTimeMillis())
                                       .setState(Metapb.StoreState.Tombstone).build();
                this.storeInfoMeta.updateStore(nowStore);
                storeInfoMeta.removeActiveStore(nowStore);
            }
        } else if (lastStore.getState() == Metapb.StoreState.Pending) {
            nowStore = Metapb.Store.newBuilder(lastStore)
                                   .setStats(storeStats)
                                   .setLastHeartbeat(System.currentTimeMillis())
                                   .setState(Metapb.StoreState.Pending).build();
            this.storeInfoMeta.updateStore(nowStore);
        } else {
            if (lastStore.getState() == Metapb.StoreState.Offline) {
                this.updateStore(
                        Metapb.Store.newBuilder(lastStore).setState(Metapb.StoreState.Up).build());
            }
            nowStore = Metapb.Store.newBuilder(lastStore)
                                   .setState(Metapb.StoreState.Up)
                                   .setStats(storeStats)
                                   .setLastHeartbeat(System.currentTimeMillis()).build();
            this.storeInfoMeta.updateStore(nowStore);
            this.storeInfoMeta.keepStoreAlive(nowStore);
            this.checkStoreStatus(storeInfoMeta.getStoreGroupByStoreId(lastStore.getId()));
        }

        return this.clusterStats.get(getStoreGroupByStore(lastStore.getId()));
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
    public synchronized Metapb.ClusterStats updateClusterStatus(Metapb.ClusterState state) {
        this.clusterStats = clusterStats.toBuilder().setState(state).build();
        return this.clusterStats;
    }

    public Metapb.ClusterStats updateClusterStatus(Metapb.PartitionState state) {
========

    public synchronized Metapb.ClusterStats updateClusterStatus(int storeGroupId, Metapb.ClusterState state)
            throws PDException {
        var stats = this.clusterStats.get(storeGroupId);
        if (stats == null) {
            var storeGroup = configService.getStoreGroup(storeGroupId);
            if (storeGroup != null) {
                this.clusterStats.put(storeGroupId, Metapb.ClusterStats.newBuilder().setState(state).build());
            } else {
                throw new PDException(ErrorType.NOT_FOUND.getNumber(), "store group not exists");
            }
        } else if (stats != null && stats.getState() != state) {
            this.clusterStats.put(storeGroupId, stats.toBuilder().setState(state).build());
        }
        return this.clusterStats.get(storeGroupId);
    }

    public Metapb.ClusterStats updateClusterStatus(int storeGroupId, Metapb.PartitionState state) throws PDException {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        Metapb.ClusterState cstate = Metapb.ClusterState.Cluster_OK;
        switch (state) {
            case PState_Normal:
                cstate = Metapb.ClusterState.Cluster_OK;
                break;
            case PState_Warn:
                cstate = Metapb.ClusterState.Cluster_Warn;
                break;
            case PState_Fault:
                cstate = Metapb.ClusterState.Cluster_Fault;
                break;
            case PState_Offline:
                cstate = Metapb.ClusterState.Cluster_Offline;
                break;
            default:
                cstate = Metapb.ClusterState.Cluster_Not_Ready;
        }
        return updateClusterStatus(storeGroupId, cstate);
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
    public Metapb.ClusterStats getClusterStats() {
        return this.clusterStats;
========
    public Metapb.ClusterStats getClusterStats(int storeGroup) {
        return this.clusterStats.getOrDefault(storeGroup, statsNotReady);
    }

    public Metapb.ClusterStats getClusterStats(long storeId) throws PDException {
        return this.clusterStats.getOrDefault(storeInfoMeta.getStoreGroupByStoreId(storeId), statsNotReady);
    }

    public Map<Integer, Metapb.ClusterState> getAllClusterStats() {
        return this.clusterStats.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getState()));
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
    }

    /**
     * Check the cluster health status
     * Whether the number of active machines is greater than the minimum threshold
     * The number of partition shards online has exceeded half
     */
    public synchronized void checkStoreStatus(int storeGroup) {
        Metapb.ClusterStats.Builder builder = Metapb.ClusterStats.newBuilder()
                                                                 .setState(
                                                                         Metapb.ClusterState.Cluster_OK);
        try {
            List<Metapb.Store> activeStores = this.getActiveStoresByStoreGroup(storeGroup);
            if (activeStores.size() < pdConfig.getMinStoreCount()) {
                builder.setState(Metapb.ClusterState.Cluster_Not_Ready);
                builder.setMessage("The number of active stores is " + activeStores.size()
                                   + ", less than pd.initial-store-count:" +
                                   pdConfig.getMinStoreCount());
            }
            Map<Long, Metapb.Store> storeMap = new HashMap<>();
            activeStores.forEach(store -> {
                storeMap.put(store.getId(), store);
            });

            if (builder.getState() == Metapb.ClusterState.Cluster_OK) {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
                // Check whether the number of online shards for each partition is greater than half
                for (Metapb.ShardGroup group : this.getShardGroups()) {
========
                // 检查每个分区的在线shard数量是否大于半数
                for (Metapb.ShardGroup group : this.getShardGroups(storeGroup)) {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
                    int count = 0;
                    for (Metapb.Shard shard : group.getShardsList()) {
                        count += storeMap.containsKey(shard.getStoreId()) ? 1 : 0;
                    }
                    if (count * 2 < group.getShardsList().size()) {
                        builder.setState(Metapb.ClusterState.Cluster_Not_Ready);
                        builder.setMessage(
                                "Less than half of active shard, partitionId is " + group.getId());
                        break;
                    }
                }
            }

        } catch (PDException e) {
            log.error("StoreNodeService updateClusterStatus exception", e);
        }
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        this.clusterStats = builder.setTimestamp(System.currentTimeMillis()).build();
        if (this.clusterStats.getState() != Metapb.ClusterState.Cluster_OK) {
========

        this.clusterStats.put(storeGroup, builder.setTimestamp(System.currentTimeMillis()).build());

        if (this.clusterStats.get(storeGroup).getState() != Metapb.ClusterState.Cluster_OK) {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
            log.error("The cluster is not ready, {}", this.clusterStats);
        }
    }

    public void addStatusListener(StoreStatusListener listener) {
        statusListeners.add(listener);
    }

    protected void onStoreRaftAddressChanged(Metapb.Store store) {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        log.info("onStoreRaftAddressChanged storeId = {}, new raft addr:", store.getId(),
                 store.getRaftAddress());
========
        log.info("onStoreRaftAddressChanged storeId = {}, new raft address: {}", store.getId(), store.getRaftAddress());
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        statusListeners.forEach(e -> {
            e.onStoreRaftChanged(store);
        });
    }

    public void addShardGroupStatusListener(ShardGroupStatusListener listener) {
        shardGroupStatusListeners.add(listener);
    }

    protected void onStoreStatusChanged(Metapb.Store store, Metapb.StoreState old,
                                        Metapb.StoreState stats) {
        log.info("onStoreStatusChanged storeId = {} from {} to {}", store.getId(), old, stats);
        statusListeners.forEach(e -> {
            e.onStoreStatusChanged(store, old, stats);
        });
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
    protected void onShardGroupStatusChanged(Metapb.ShardGroup group, Metapb.ShardGroup newGroup) {
        log.info("onShardGroupStatusChanged, groupId: {}, from {} to {}", group.getId(), group,
                 newGroup);
        shardGroupStatusListeners.forEach(e -> e.onShardListChanged(group, newGroup));
========
    protected void onShardGroupStatusChanged(Metapb.ShardGroup group, Metapb.ShardGroup newGroup){
        if (group == null && newGroup == null) {
            return;
        }

        var id = group == null ? newGroup.getId() : group.getId();
        log.info("onShardGroupStatusChanged, groupId: {}, from {} to {}", id, group, newGroup);
        shardGroupStatusListeners.forEach( e -> e.onShardListChanged(group, newGroup));
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
    }

    protected void onShardGroupOp(Metapb.ShardGroup shardGroup) {
        log.info("onShardGroupOp, group id: {}, shard group:{}", shardGroup.getId(), shardGroup);
        shardGroupStatusListeners.forEach(e -> e.onShardListOp(shardGroup));
    }

    /**
     * Check whether the current store can be discontinued
     * If the number of active machines is less than or equal to the minimum threshold, they
     * cannot be taken offline
     * If the number of shards in the partition is not more than half, it cannot be offline
     */
    public boolean checkStoreCanOffline(Metapb.Store currentStore) {
        try {
            Map<Long, Metapb.Store> storeMap = getActiveStoresByStoreGroup(getStoreGroupByStore(currentStore.getId()))
                    .stream().collect(Collectors.toMap(Metapb.Store::getId, store -> store));

            if (storeMap.size() < pdConfig.getMinStoreCount()) {
                return false;
            }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
            // Check whether the number of online shards for each partition is greater than half
            for (Metapb.ShardGroup group : this.getShardGroups()) {
========
            // 检查每个分区的在线shard数量是否大于半数
            for (Metapb.ShardGroup group : this.getShardGroups(getStoreGroupByStore(currentStore.getId()))) {
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
                int count = 0;
                for (Metapb.Shard shard : group.getShardsList()) {
                    long storeId = shard.getStoreId();
                    count += storeMap.containsKey(storeId) ? 1 : 0;
                }
                if (count * 2 < group.getShardsList().size()) {
                    return false;
                }
            }
        } catch (PDException e) {
            log.error("StoreNodeService checkStoreCanOffline exception ", e);
            return false;
        }

        return true;
    }

    /**
     * Compaction on rocksdb on the store
     *
     * @param groupId
     * @param tableName
     * @return
     */
    public synchronized void shardGroupsDbCompaction(int groupId, String tableName) throws
                                                                                    PDException {

        // Notify all stores to compaction rocksdb
        partitionService.fireDbCompaction(groupId, tableName);
        // TODO How to deal with exceptions?
    }

    public Map getQuota() throws PDException {
        List<Metapb.Graph> graphs = partitionService.getGraphs();
        String delimiter = String.valueOf(MetadataKeyHelper.DELIMITER);
        HashMap<String, Long> storages = new HashMap<>();
        for (Metapb.Graph g : graphs) {
            String graphName = g.getGraphName();
            String[] splits = graphName.split(delimiter);
            if (splits.length < 2) {
                continue;
            }
            String graphSpace = splits[0];
            storages.putIfAbsent(graphSpace, 0L);
            List<Metapb.Store> stores = getStores(graphName);
            long dataSize = 0;
            for (Metapb.Store store : stores) {
                List<Metapb.GraphStats> gss = store.getStats()
                                                   .getGraphStatsList();
                for (Metapb.GraphStats gs : gss) {
                    boolean nameEqual = graphName.equals(gs.getGraphName());
                    boolean roleEqual = Metapb.ShardRole.Leader.equals(
                            gs.getRole());
                    if (nameEqual && roleEqual) {
                        dataSize += gs.getApproximateSize();
                    }
                }
            }
            Long size = storages.get(graphSpace);
            size += dataSize;
            storages.put(graphSpace, size);

        }
        Metapb.GraphSpace.Builder spaceBuilder = Metapb.GraphSpace.newBuilder();
        HashMap<String, Boolean> limits = new HashMap<>();
        for (Map.Entry<String, Long> item : storages.entrySet()) {
            String spaceName = item.getKey();
            String value = kvService.get(graphSpaceConfPrefix + spaceName);
            if (!StringUtils.isEmpty(value)) {
                HashMap config = new Gson().fromJson(value, HashMap.class);
                Long size = item.getValue();
                int limit = ((Double) config.get("storage_limit")).intValue();
                long limitByLong = limit * 1024L * 1024L;
                try {
                    spaceBuilder.setName(spaceName).setStorageLimit(limitByLong).setUsedSize(size);
                    Metapb.GraphSpace graphSpace = spaceBuilder.build();
                    configService.setGraphSpace(graphSpace);
                } catch (Exception e) {
                    log.error("update graph space with error:", e);
                }
                // KB and GB * 1024L * 1024L
                if (size > limitByLong) {
                    limits.put(spaceName, true);
                    continue;
                }
            }
            limits.put(spaceName, false);

        }
        GraphState.Builder stateBuilder = GraphState.newBuilder()
                                                    .setMode(GraphMode.ReadOnly)
                                                    .setReason(
                                                            GraphModeReason.Quota);
        for (Metapb.Graph g : graphs) {
            String graphName = g.getGraphName();
            String[] splits = graphName.split(delimiter);
            if (splits.length < 2) {
                continue;
            }
            String graphSpace = splits[0];
            Metapb.GraphState gsOld = g.getGraphState();
            GraphMode gmOld = gsOld != null ? gsOld.getMode() : GraphMode.ReadWrite;
            GraphMode gmNew = limits.get(
                    graphSpace) ? GraphMode.ReadOnly : GraphMode.ReadWrite;
            if (gmOld == null || gmOld.getNumber() != gmNew.getNumber()) {
                stateBuilder.setMode(gmNew);
                if (gmNew.getNumber() == GraphMode.ReadOnly.getNumber()) {
                    stateBuilder.setReason(GraphModeReason.Quota);
                }
                GraphState gsNew = stateBuilder.build();
                Metapb.Graph newGraph = g.toBuilder().setGraphState(gsNew)
                                         .build();
                partitionService.updateGraph(newGraph);
                statusListeners.forEach(listener -> {
                    listener.onGraphChange(newGraph, gsOld, gsNew);
                });
            }
        }

        return limits;
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
    public Runnable getQuotaChecker() {
        return quotaChecker;
    }

    public TaskInfoMeta getTaskInfoMeta() {
        return taskInfoMeta;
    }

    public StoreInfoMeta getStoreInfoMeta() {
        return storeInfoMeta;
    }
========

    @Getter
    private Runnable quotaChecker = () -> {
        try {
            getQuota();
        } catch (Exception e) {
            log.error(
                    "obtaining and sending graph space quota information with error: ",
                    e);
        }
    };
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java

    /**
     * Get the leader of the partition
     *
     * @param partition
     * @param initIdx
     * @return
     */
    public Metapb.Shard getLeader(Metapb.Partition partition, int initIdx) {
        Metapb.Shard leader = null;
        try {
            var shardGroup = this.getShardGroup(partition.getId());
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
            for (Metapb.Shard shard : shardGroup.getShardsList()) {
                if (shard.getRole() == Metapb.ShardRole.Leader) {
                    leader = shard;
                }
            }
        } catch (Exception e) {
            log.error("get leader error: group id:{}, error: {}",
                      partition.getId(), e.getMessage());
========
            if (shardGroup != null) {
                for (Metapb.Shard shard : shardGroup.getShardsList()) {
                    if (shard.getRole() == Metapb.ShardRole.Leader) {
                        leader = shard;
                    }
                }
            }
        }catch (Exception e){
            log.error("get leader error: group id:{}, error:", partition.getId(), e);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
        }
        return leader;
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
========
    private void updateShardGroupCache(Metapb.ShardGroup group) {
        if (group == null || group.getShardsList().isEmpty()) {
            return;
        }
        partitionService.updateShardGroupCache(group);
    }

>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
    public CacheResponse getCache() throws PDException {

        List<Metapb.Store> stores = getStores();
        List<Metapb.ShardGroup> groups = getShardGroups();
        List<Metapb.Graph> graphs = partitionService.getGraphs();
        CacheResponse cache = CacheResponse.newBuilder().addAllGraphs(graphs)
                                           .addAllShards(groups)
                                           .addAllStores(stores)
                                           .build();
        return cache;
    }
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
========

    public int getStoreGroupByStore(Metapb.Store store ) throws PDException {
        return getStoreGroupByStore(store.getId());
    }

    /**
     * get the store group id by the store id
     * @param storeId store id
     * @return group id
     * @throws PDException
     */
    public int getStoreGroupByStore(Long storeId) throws PDException {
        return storeInfoMeta.getStoreGroupByStoreId(storeId);
    }

    public boolean isStoreHasStoreGroup(long storeId) throws PDException {
        return storeInfoMeta.isStoreHasGroup(storeId);
    }

    public List<Metapb.Store> getStoresByStoreGroup(int storeGroupId) throws PDException {
        Set<Long> storeIds = storeInfoMeta.getStoreIdsByGroup(storeGroupId);
        return getStores().stream().filter(store -> storeIds.contains(store.getId())).collect(Collectors.toList());
    }

    /**
     * need check the store group id is exist && the store has no partition
     *
     * @param storeId store id
     * @param storeGroupId group id
     * @throws PDException
     */
    public void updateStoreGroupRelation(long storeId, int storeGroupId) throws PDException {
        var storeGroup = configService.getStoreGroup(storeGroupId);
        if (storeGroup != null) {
            storeInfoMeta.updateStoreGroup(storeId, storeGroupId);
        } else {
            throw new PDException(-1, "store group not found");
        }
    }

    public int getShardGroupBelongsToStoreGroup(Metapb.ShardGroup group) throws PDException {
        if (group == null || group.getShardsList().isEmpty()) {
            return 0;
        }
        return getStoreGroupByStore(group.getShardsList().get(0).getStoreId());
    }
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/StoreNodeService.java
}
