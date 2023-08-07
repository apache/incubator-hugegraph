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

package org.apache.hugegraph.pd;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.meta.TaskInfoMeta;
import org.apache.hugegraph.pd.raft.RaftEngine;

import lombok.extern.slf4j.Slf4j;


/**
 * 任务调度服务，定时检查Store、资源、分区的状态，及时迁移数据，错误节点
 * 1、监测Store是否离线
 * 2、监测Partition的副本是否正确
 * 3、监测Partition的工作模式是否正确
 * 4、监测Partition是否需要分裂,监测分裂是否完成
 */
@Slf4j
public class TaskScheduleService {
    private static final String BALANCE_SHARD_KEY = "BALANCE_SHARD_KEY";
    private final long TurnOffAndBalanceInterval = 30 * 60 * 1000; //机器下线30后才能进行动态平衡
    private final long BalanceLeaderInterval = 30 * 1000;   // leader平衡时间间隔
    private final PDConfig pdConfig;
    private final long clusterStartTime;    //
    private final StoreNodeService storeService;
    private final PartitionService partitionService;
    private final ScheduledExecutorService executor;
    private final TaskInfoMeta taskInfoMeta;
    private final StoreMonitorDataService storeMonitorDataService;
    private final KvService kvService;
    private final LogService logService;
    // 先按照value排序，再按照key排序
    private final Comparator<KVPair<Long, Integer>> kvPairComparatorAsc = (o1, o2) -> {
        if (o1.getValue() == o2.getValue()) {
            return o1.getKey().compareTo(o2.getKey());
        }
        return o1.getValue().compareTo(o2.getValue());
    };
    // 先按照value排序(倒序)，再按照key排序(升序）
    private final Comparator<KVPair<Long, Integer>> kvPairComparatorDesc = (o1, o2) -> {
        if (o1.getValue() == o2.getValue()) {
            return o2.getKey().compareTo(o1.getKey());
        }
        return o2.getValue().compareTo(o1.getValue());
    };
    private long lastStoreTurnoffTime = 0;
    private long lastBalanceLeaderTime = 0;


    public TaskScheduleService(PDConfig config, StoreNodeService storeService,
                               PartitionService partitionService) {
        this.pdConfig = config;
        this.storeService = storeService;
        this.partitionService = partitionService;
        this.taskInfoMeta = new TaskInfoMeta(config);
        this.logService = new LogService(pdConfig);
        this.storeMonitorDataService = new StoreMonitorDataService(pdConfig);
        this.clusterStartTime = System.currentTimeMillis();
        this.kvService = new KvService(pdConfig);
        this.executor = new ScheduledThreadPoolExecutor(16);
    }

    public void init() {
        executor.scheduleWithFixedDelay(() -> {
            try {
                patrolStores();
            } catch (Throwable e) {
                log.error("patrolStores exception: ", e);
            }

        }, 60, 60, TimeUnit.SECONDS);
        executor.scheduleWithFixedDelay(() -> {
            try {
                patrolPartitions();
                balancePartitionLeader(false);
                balancePartitionShard();
            } catch (Throwable e) {
                log.error("patrolPartitions exception: ", e);
            }
        }, pdConfig.getPatrolInterval(), pdConfig.getPatrolInterval(), TimeUnit.SECONDS);
        executor.scheduleWithFixedDelay(() -> {
            if (isLeader()) {
                kvService.clearTTLData();
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(
                () -> {
                    if (isLeader()) {
                        storeService.getQuotaChecker();
                    }
                }, 2, 30,
                TimeUnit.SECONDS);
        // clean expired monitor data each 10 minutes, delay 3min.
        if (isLeader() && this.pdConfig.getStore().isMonitorDataEnabled()) {
            executor.scheduleAtFixedRate(() -> {
                Long expTill = System.currentTimeMillis() / 1000 -
                               this.pdConfig.getStore().getRetentionPeriod();
                log.debug("monitor data keys before " + expTill + " will be deleted");
                int records = 0;
                try {
                    for (Metapb.Store store : storeService.getStores()) {
                        int cnt =
                                this.storeMonitorDataService.removeExpiredMonitorData(store.getId(),
                                                                                      expTill);
                        log.debug("store id :{}, records:{}", store.getId(), cnt);
                        records += cnt;
                    }
                } catch (PDException e) {
                    throw new RuntimeException(e);
                }
                log.debug(String.format("%d records has been deleted", records));
            }, 180, 600, TimeUnit.SECONDS);
        }

        storeService.addStatusListener(new StoreStatusListener() {
            @Override
            public void onStoreStatusChanged(Metapb.Store store, Metapb.StoreState old,
                                             Metapb.StoreState status) {
                if (status == Metapb.StoreState.Tombstone) {
                    lastStoreTurnoffTime = System.currentTimeMillis();
                }

                if (status == Metapb.StoreState.Up) {
                    executor.schedule(() -> {
                        try {  //store 上线后延时1分钟进行leader平衡
                            balancePartitionLeader(false);
                        } catch (PDException e) {
                            log.error("exception {}", e);
                        }
                    }, BalanceLeaderInterval, TimeUnit.MILLISECONDS);

                }
            }

            @Override
            public void onGraphChange(Metapb.Graph graph,
                                      Metapb.GraphState stateOld,
                                      Metapb.GraphState stateNew) {

            }

            @Override
            public void onStoreRaftChanged(Metapb.Store store) {

            }
        });
    }

    public void shutDown() {
        executor.shutdownNow();
    }

    private boolean isLeader() {
        return RaftEngine.getInstance().isLeader();
    }

    /**
     * 巡查所有的store，检查是否在线，存储空间是否充足
     */
    public List<Metapb.Store> patrolStores() throws PDException {
        if (!isLeader()) {
            return null;
        }

        List<Metapb.Store> changedStores = new ArrayList<>();
        // 检查store在线状态
        List<Metapb.Store> stores = storeService.getStores("");
        Map<Long, Metapb.Store> activeStores = storeService.getActiveStores("")
                                                           .stream().collect(
                        Collectors.toMap(Metapb.Store::getId, t -> t));
        for (Metapb.Store store : stores) {
            Metapb.Store changeStore = null;
            if ((store.getState() == Metapb.StoreState.Up
                 || store.getState() == Metapb.StoreState.Unknown)
                && !activeStores.containsKey(store.getId())) {
                // 不在线，修改状态为离线
                changeStore = Metapb.Store.newBuilder(store)
                                          .setState(Metapb.StoreState.Offline)
                                          .build();

            } else if ((store.getState() == Metapb.StoreState.Exiting &&
                        !activeStores.containsKey(store.getId())) ||
                       (store.getState() == Metapb.StoreState.Offline &&
                        (System.currentTimeMillis() - store.getLastHeartbeat() >
                         pdConfig.getStore().getMaxDownTime() * 1000) &&
                        (System.currentTimeMillis() - clusterStartTime >
                         pdConfig.getStore().getMaxDownTime() * 1000))) {
                //手工修改为下线或者离线达到时长
                // 修改状态为关机, 增加 checkStoreCanOffline 检测
                if (storeService.checkStoreCanOffline(store)) {
                    changeStore = Metapb.Store.newBuilder(store)
                                              .setState(Metapb.StoreState.Tombstone).build();
                    this.logService.insertLog(LogService.NODE_CHANGE,
                                              LogService.TASK, changeStore);
                    log.info("patrolStores store {} Offline", changeStore.getId());
                }
            }
            if (changeStore != null) {
                storeService.updateStore(changeStore);
                changedStores.add(changeStore);
            }
        }
        return changedStores;
    }


    /**
     * 巡查所有的分区，检查副本数是否正确
     */
    public List<Metapb.Partition> patrolPartitions() throws PDException {
        if (!isLeader()) {
            return null;
        }

        // 副本数不一致，重新分配副本
        for (Metapb.ShardGroup group : storeService.getShardGroups()) {
            if (group.getShardsCount() != pdConfig.getPartition().getShardCount()) {
                storeService.reallocShards(group);
                // 避免后面的 balance partition shard 马上执行.
                kvService.put(BALANCE_SHARD_KEY, "DOING", 180 * 1000);
            }
        }
        //检查shard是否在线。
        Map<Long, Metapb.Store> tombStores = storeService.getTombStores().stream().collect(
                Collectors.toMap(Metapb.Store::getId, t -> t));

        var partIds = new HashSet<Integer>();

        for (var pair : tombStores.entrySet()) {
            for (var partition : partitionService.getPartitionByStore(pair.getValue())) {
                if (partIds.contains(partition.getId())) {
                    continue;
                }
                partIds.add(partition.getId());

                storeService.storeTurnoff(pair.getValue());
                partitionService.shardOffline(partition, pair.getValue().getId());
            }

        }

        return null;
    }


    /**
     * 在Store之间平衡分区的数量
     * 机器转为UP半小时后才能进行动态平衡
     */
    public synchronized Map<Integer, KVPair<Long, Long>> balancePartitionShard() throws
                                                                                 PDException {
        log.info("balancePartitions starting, isleader:{}", isLeader());

        if (!isLeader()) {
            return null;
        }

        if (System.currentTimeMillis() - lastStoreTurnoffTime < TurnOffAndBalanceInterval) {
            return null;//机器下线半小时后才能进行动态平衡
        }


        int activeStores = storeService.getActiveStores().size();
        if (activeStores == 0) {
            log.warn("balancePartitionShard non active stores, skip to balancePartitionShard");
            return null;
        }

        // 避免频繁调用. (当改变副本数，需要调整shard list，此时又需要平衡分区）会发送重复的指令。造成结果不可预料。
        // 严重会删除掉分区.
        if (Objects.equals(kvService.get(BALANCE_SHARD_KEY), "DOING")) {
            return null;
        }

        int totalShards = pdConfig.getConfigService().getPartitionCount() *
                          pdConfig.getPartition().getShardCount();
        int averageCount = totalShards / activeStores;
        int remainder = totalShards % activeStores;

        // 统计每个store上分区, StoreId ->PartitionID, ShardRole
        Map<Long, Map<Integer, Metapb.ShardRole>> partitionMap = new HashMap<>();
        storeService.getActiveStores().forEach(store -> {
            partitionMap.put(store.getId(), new HashMap<>());
        });

        // 如果是leaner 说明迁移正在进行，不要重复提交任务
        AtomicReference<Boolean> isLeaner = new AtomicReference<>(false);
        partitionService.getPartitions().forEach(partition -> {

            try {
                storeService.getShardList(partition.getId()).forEach(shard -> {
                    Long storeId = shard.getStoreId();
                    // 判断每个shard为leaner或者状态非正常状态
                    if (shard.getRole() == Metapb.ShardRole.Learner
                        || partition.getState() != Metapb.PartitionState.PState_Normal) {
                        isLeaner.set(true);
                    }
                    if (partitionMap.containsKey(storeId)) {
                        partitionMap.get(storeId).put(partition.getId(), shard.getRole());
                    }
                });
            } catch (PDException e) {
                log.error("get partition {} shard list error:{}.", partition.getId(),
                          e.getMessage());
            }
        });

        if (isLeaner.get()) {
            log.warn("balancePartitionShard is doing, skip this balancePartitionShard task");
            return null;
        }

        // 按照shard数量由高到低排序store
        List<KVPair<Long, Integer>> sortedList = new ArrayList<>();
        partitionMap.forEach((storeId, shards) -> {
            sortedList.add(new KVPair(storeId, shards.size()));
        });
        // 由大到小排序的list
        sortedList.sort(((o1, o2) -> o2.getValue().compareTo(o1.getValue())));
        // 最大堆
        PriorityQueue<KVPair<Long, Integer>> maxHeap = new PriorityQueue<>(sortedList.size(),
                                                                           (o1, o2) -> o2.getValue()
                                                                                         .compareTo(
                                                                                                 o1.getValue()));

        // 各个副本的 committedIndex
        Map<Integer, Map<Long, Long>> committedIndexMap = partitionService.getCommittedIndexStats();
        // 分区ID --> 源StoreID,目标StoreID
        Map<Integer, KVPair<Long, Long>> movedPartitions = new HashMap<>();
        // 移除多余的shard, 按照shards由多到少的顺序遍历store，余数remainder优先给shards多的store分配，减少迁移的概率
        for (int index = 0; index < sortedList.size(); index++) {
            long storeId = sortedList.get(index).getKey();
            if (!partitionMap.containsKey(storeId)) {
                log.error("cannot found storeId {} in partitionMap", storeId);
                return null;
            }
            Map<Integer, Metapb.ShardRole> shards = partitionMap.get(storeId);
            int targetCount = index < remainder ? averageCount + 1 : averageCount;
            //  移除多余的shard, 添加源StoreID. 非Leader，并且该分区唯一
            if (shards.size() > targetCount) {
                int movedCount = shards.size() - targetCount;
                log.info(
                        "balancePartitionShard storeId {}, shardsSize {}, targetCount {}, " +
                        "moveCount {}",
                        storeId, shards.size(), targetCount, movedCount);
                for (Iterator<Integer> iterator = shards.keySet().iterator();
                     movedCount > 0 && iterator.hasNext(); ) {
                    Integer id = iterator.next();

                    if (!movedPartitions.containsKey(id)) {
                        log.info("store {}, shard of partition {} can be moved", storeId, id);
                        movedPartitions.put(id, new KVPair<>(storeId, 0L));
                        movedCount--;
                    }
                }
            } else if (shards.size() < targetCount) {
                int addCount = targetCount - shards.size();
                log.info(
                        "balancePartitionShard storeId {}, shardsSize {}, targetCount {}, " +
                        "addCount {}",
                        storeId, shards.size(), targetCount, addCount);
                maxHeap.add(new KVPair<>(storeId, addCount));
            }
        }

        if (movedPartitions.size() == 0) {
            log.warn(
                    "movedPartitions is empty, totalShards:{} averageCount:{} remainder:{} " +
                    "sortedList:{}",
                    totalShards, averageCount, remainder, sortedList);
        }
        Iterator<Map.Entry<Integer, KVPair<Long, Long>>> moveIterator =
                movedPartitions.entrySet().iterator();

        while (moveIterator.hasNext()) {
            if (maxHeap.size() == 0) {
                break;
            }
            Map.Entry<Integer, KVPair<Long, Long>> moveEntry = moveIterator.next();
            int partitionId = moveEntry.getKey();
            long sourceStoreId = moveEntry.getValue().getKey();

            List<KVPair<Long, Integer>> tmpList = new ArrayList<>(maxHeap.size());
            while (maxHeap.size() > 0) {
                KVPair<Long, Integer> pair = maxHeap.poll();
                long destStoreId = pair.getKey();
                boolean destContains = false;
                if (partitionMap.containsKey(destStoreId)) {
                    destContains = partitionMap.get(destStoreId).containsKey(partitionId);
                }
                // 如果目的store已经包含了该partition，则取一下store
                if (!destContains) {
                    moveEntry.getValue().setValue(pair.getKey());
                    log.info(
                            "balancePartitionShard will move partition {} from store {} to store " +
                            "{}",
                            moveEntry.getKey(),
                            moveEntry.getValue().getKey(),
                            moveEntry.getValue().getValue());
                    if (pair.getValue() > 1) {
                        pair.setValue(pair.getValue() - 1);
                        tmpList.add(pair);
                    }
                    break;
                }
                tmpList.add(pair);
            }
            maxHeap.addAll(tmpList);
        }

        kvService.put(BALANCE_SHARD_KEY, "DOING", 180 * 1000);

        // 开始迁移
        movedPartitions.forEach((partId, storePair) -> {
            // 源和目标storeID都不为0
            if (storePair.getKey() > 0 && storePair.getValue() > 0) {
                partitionService.movePartitionsShard(partId, storePair.getKey(),
                                                     storePair.getValue());
            } else {
                log.warn("balancePartitionShard key or value is zero, partId:{} storePair:{}",
                         partId, storePair);
            }
        });
        return movedPartitions;
    }

    /**
     * 在Store之间平衡分区的Leader的数量
     */
    public synchronized Map<Integer, Long> balancePartitionLeader(boolean immediately) throws
                                                                                       PDException {
        Map<Integer, Long> results = new HashMap<>();

        if (!isLeader()) {
            return results;
        }

        if (!immediately &&
            System.currentTimeMillis() - lastBalanceLeaderTime < BalanceLeaderInterval) {
            return results;
        }
        lastBalanceLeaderTime = System.currentTimeMillis();

        List<Metapb.ShardGroup> shardGroups = storeService.getShardGroups();

        // 分裂或者缩容任务的时候，退出
        var taskMeta = storeService.getTaskInfoMeta();
        if (taskMeta.hasSplitTaskDoing() || taskMeta.hasMoveTaskDoing()) {
            throw new PDException(1001, "split or combine task is processing, please try later!");
        }

        // 数据迁移的时候，退出
        if (Objects.equals(kvService.get(BALANCE_SHARD_KEY), "DOING")) {
            throw new PDException(1001, "balance shard is processing, please try later!");
        }

        if (shardGroups.size() == 0) {
            return results;
        }

        Map<Long, Integer> storeShardCount = new HashMap<>();

        shardGroups.forEach(group -> {
            group.getShardsList().forEach(shard -> {
                storeShardCount.put(shard.getStoreId(),
                                    storeShardCount.getOrDefault(shard.getStoreId(), 0) + 1);
            });
        });

        log.info("balancePartitionLeader, shard group size: {}, by store: {}", shardGroups.size(),
                 storeShardCount);

        // 按照 target count， store id稳定排序
        PriorityQueue<KVPair<Long, Integer>> targetCount =
                new PriorityQueue<>(kvPairComparatorDesc);

        var sortedGroups = storeShardCount.entrySet().stream()
                                          .map(entry -> new KVPair<>(entry.getKey(),
                                                                     entry.getValue()))
                                          .sorted(kvPairComparatorAsc)
                                          .collect(Collectors.toList());
        int sum = 0;

        for (int i = 0; i < sortedGroups.size() - 1; i++) {
            // at least one
            int v = Math.max(
                    sortedGroups.get(i).getValue() / pdConfig.getPartition().getShardCount(), 1);
            targetCount.add(new KVPair<>(sortedGroups.get(i).getKey(), v));
            sum += v;
        }
        // 最后一个, 除不尽的情况，保证总数正确
        targetCount.add(new KVPair<>(sortedGroups.get(sortedGroups.size() - 1).getKey(),
                                     shardGroups.size() - sum));
        log.info("target count: {}", targetCount);

        for (var group : shardGroups) {
            var map = group.getShardsList().stream()
                           .collect(Collectors.toMap(Metapb.Shard::getStoreId, shard -> shard));
            var tmpList = new ArrayList<KVPair<Long, Integer>>();
            // store比较多的情况，可能不包含对应的store id. 则先将不符合的store保存到临时列表，直到找到一个合适的store
            while (!targetCount.isEmpty()) {
                var pair = targetCount.poll();
                var storeId = pair.getKey();
                if (map.containsKey(storeId)) {
                    if (map.get(storeId).getRole() != Metapb.ShardRole.Leader) {
                        log.info("shard group{}, store id:{}, set to leader", group.getId(),
                                 storeId);
                        partitionService.transferLeader(group.getId(), map.get(storeId));
                        results.put(group.getId(), storeId);
                    } else {
                        log.info("shard group {}, store id :{}, is leader, no need change",
                                 group.getId(), storeId);
                    }

                    if (pair.getValue() > 1) {
                        // count -1
                        pair.setValue(pair.getValue() - 1);
                        tmpList.add(pair);
                    }
                    // 找到了，则处理完成
                    break;
                } else {
                    tmpList.add(pair);
                }
            }
            targetCount.addAll(tmpList);
        }

        return results;
    }


    private long getMaxIndexGap(Map<Integer, Map<Long, Long>> committedIndexMap, int partitionId) {
        long maxGap = Long.MAX_VALUE;
        if (committedIndexMap == null || !committedIndexMap.containsKey(partitionId)) {
            return maxGap;
        }
        Map<Long, Long> shardMap = committedIndexMap.get(partitionId);
        if (shardMap == null || shardMap.size() == 0) {
            return maxGap;
        }
        List<Long> sortedList = new ArrayList<>();
        shardMap.forEach((storeId, committedIndex) -> {
            sortedList.add(committedIndex);
        });
        // 由大到小排序的list
        sortedList.sort(Comparator.reverseOrder());
        maxGap = sortedList.get(0) - sortedList.get(sortedList.size() - 1);
        return maxGap;
    }


    /**
     * 执行分区分裂，分为自动分裂和手工分裂
     *
     * @return
     * @throws PDException
     */
    public List<Metapb.Partition> splitPartition(
            Pdpb.OperationMode mode, List<Pdpb.SplitDataParam> params) throws PDException {

        if (mode == Pdpb.OperationMode.Auto) {
            return autoSplitPartition();
        }

        var list = params.stream()
                         .map(param -> new KVPair<>(param.getPartitionId(), param.getCount()))
                         .collect(Collectors.toList());

        storeService.splitShardGroups(list);
        return null;
    }

    /**
     * 自动进行分区分裂，每个store达到最大分区数量
     * 执行条件
     * 分裂后每台机器分区数量少于partition.max-partitions-per-store
     *
     * @throws PDException
     */
    public List<Metapb.Partition> autoSplitPartition() throws PDException {
        if (!isLeader()) {
            return null;
        }

        if (Metapb.ClusterState.Cluster_OK != storeService.getClusterStats().getState()) {
            if (Metapb.ClusterState.Cluster_Offline == storeService.getClusterStats().getState()) {
                throw new PDException(Pdpb.ErrorType.Split_Partition_Doing_VALUE,
                                      "The data is splitting");
            } else {
                throw new PDException(Pdpb.ErrorType.Cluster_State_Forbid_Splitting_VALUE,
                                      "The current state of the cluster prohibits splitting data");
            }
        }

        //For TEST
        //   pdConfig.getPartition().setMaxShardsPerStore(pdConfig.getPartition()
        //   .getMaxShardsPerStore()*2);

        // 计算集群能能支持的最大split count
        int splitCount = pdConfig.getPartition().getMaxShardsPerStore() *
                         storeService.getActiveStores().size() /
                         (storeService.getShardGroups().size() *
                          pdConfig.getPartition().getShardCount());

        if (splitCount < 2) {
            throw new PDException(Pdpb.ErrorType.Too_Many_Partitions_Per_Store_VALUE,
                                  "Too many partitions per store, partition.store-max-shard-count" +
                                  " = "
                                  + pdConfig.getPartition().getMaxShardsPerStore());
        }

        // 每store未达最大分区数，进行分裂
        log.info("Start to split partitions..., split count = {}", splitCount);

        // 设置集群状态为下线
        storeService.updateClusterStatus(Metapb.ClusterState.Cluster_Offline);
        // 修改默认分区数量
        // pdConfig.getConfigService().setPartitionCount(storeService.getShardGroups().size() *
        // splitCount);

        var list = storeService.getShardGroups().stream()
                               .map(shardGroup -> new KVPair<>(shardGroup.getId(), splitCount))
                               .collect(Collectors.toList());
        storeService.splitShardGroups(list);

        return null;
    }


    /**
     * Store汇报任务状态
     * 分区状态发生改变，重新计算分区所在的ShardGroup、图和整个集群的状态
     *
     * @param task
     */
    public void reportTask(MetaTask.Task task) {
        try {
            switch (task.getType()) {
                case Split_Partition:
                    partitionService.handleSplitTask(task);
                    break;
                case Move_Partition:
                    partitionService.handleMoveTask(task);
                    break;
                case Clean_Partition:
                    partitionService.handleCleanPartitionTask(task);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            log.error("Report task exception {}, {}", e, task);
        }
    }

    /**
     * 对rocksdb进行compaction
     *
     * @throws PDException
     */
    public Boolean dbCompaction(String tableName) throws PDException {
        if (!isLeader()) {
            return false;
        }

        for (Metapb.ShardGroup shardGroup : storeService.getShardGroups()) {
            storeService.shardGroupsDbCompaction(shardGroup.getId(), tableName);
        }

        //
        return true;
    }

    /**
     * 判断是否能把一个store的分区全部迁出，给出判断结果和迁移方案
     */
    public Map<String, Object> canAllPartitionsMovedOut(Metapb.Store sourceStore) throws
                                                                                  PDException {
        if (!isLeader()) {
            return null;
        }
        // 分析一个store上面的分区是否可以完全迁出
        Map<String, Object> resultMap = new HashMap<>();
        // 定义对象用于保存源store上面的分区 StoreId ->PartitionID, ShardRole
        Map<Long, Map<Integer, Metapb.ShardRole>> sourcePartitionMap = new HashMap<>();
        sourcePartitionMap.put(sourceStore.getId(), new HashMap<>());
        // 定义对象用于保存其他活跃store上面的分区 StoreId ->PartitionID, ShardRole
        Map<Long, Map<Integer, Metapb.ShardRole>> otherPartitionMap = new HashMap<>();
        Map<Long, Long> availableDiskSpace = new HashMap<>(); // 每个store剩余的磁盘空间
        Map<Integer, Long> partitionDataSize = new HashMap<>(); // 记录待迁移的分区的数据量

        storeService.getActiveStores().forEach(store -> {
            if (store.getId() != sourceStore.getId()) {
                otherPartitionMap.put(store.getId(), new HashMap<>());
                // 记录其他store的剩余的磁盘空间, 单位为Byte
                availableDiskSpace.put(store.getId(), store.getStats().getAvailable());
            } else {
                resultMap.put("current_store_is_online", true);
            }
        });
        // 统计待迁移的分区的数据大小 (从storeStats中统计，单位为KB)
        for (Metapb.GraphStats graphStats : sourceStore.getStats().getGraphStatsList()) {
            partitionDataSize.put(graphStats.getPartitionId(),
                                  partitionDataSize.getOrDefault(graphStats.getPartitionId(), 0L)
                                  + graphStats.getApproximateSize());
        }
        // 给sourcePartitionMap 和 otherPartitionMap赋值
        partitionService.getPartitions().forEach(partition -> {
            try {
                storeService.getShardList(partition.getId()).forEach(shard -> {
                    long storeId = shard.getStoreId();
                    if (storeId == sourceStore.getId()) {
                        sourcePartitionMap.get(storeId).put(partition.getId(), shard.getRole());
                    } else {
                        if (otherPartitionMap.containsKey(storeId)) {
                            otherPartitionMap.get(storeId).put(partition.getId(), shard.getRole());
                        }
                    }

                });
            } catch (PDException e) {
                throw new RuntimeException(e);
            }
        });
        // 统计待移除的分区：即源store上面的所有分区
        Map<Integer, KVPair<Long, Long>> movedPartitions = new HashMap<>();
        for (Map.Entry<Integer, Metapb.ShardRole> entry : sourcePartitionMap.get(
                sourceStore.getId()).entrySet()) {
            movedPartitions.put(entry.getKey(), new KVPair<>(sourceStore.getId(), 0L));
        }
        // 统计其他store的分区数量, 用小顶堆保存，以便始终把分区数量较少的store优先考虑
        PriorityQueue<KVPair<Long, Integer>> minHeap = new PriorityQueue<>(otherPartitionMap.size(),
                                                                           (o1, o2) -> o1.getValue()
                                                                                         .compareTo(
                                                                                                 o2.getValue()));
        otherPartitionMap.forEach((storeId, shards) -> {
            minHeap.add(new KVPair(storeId, shards.size()));
        });
        // 遍历待迁移的分区,优先迁移到分区比较少的store
        Iterator<Map.Entry<Integer, KVPair<Long, Long>>> moveIterator =
                movedPartitions.entrySet().iterator();
        while (moveIterator.hasNext()) {
            Map.Entry<Integer, KVPair<Long, Long>> moveEntry = moveIterator.next();
            int partitionId = moveEntry.getKey();
            List<KVPair<Long, Integer>> tmpList = new ArrayList<>(); // 记录已经弹出优先队列的元素
            while (minHeap.size() > 0) {
                KVPair<Long, Integer> pair = minHeap.poll(); //弹出首个元素
                long storeId = pair.getKey();
                int partitionCount = pair.getValue();
                Map<Integer, Metapb.ShardRole> shards = otherPartitionMap.get(storeId);
                final int unitRate = 1024; // 平衡不同存储单位的进率
                if ((!shards.containsKey(partitionId)) && (
                        availableDiskSpace.getOrDefault(storeId, 0L) / unitRate >=
                        partitionDataSize.getOrDefault(partitionId, 0L))) {
                    // 如果目标store上面不包含该分区，且目标store剩余空间能容纳该分区，则进行迁移
                    moveEntry.getValue().setValue(storeId); //设置移动的目标store
                    log.info("plan to move partition {} to store {}, " +
                             "available disk space {}, current partitionSize:{}",
                             partitionId,
                             storeId,
                             availableDiskSpace.getOrDefault(storeId, 0L) / unitRate,
                             partitionDataSize.getOrDefault(partitionId, 0L)
                    );
                    // 更新该store预期的剩余空间
                    availableDiskSpace.put(storeId, availableDiskSpace.getOrDefault(storeId, 0L)
                                                    - partitionDataSize.getOrDefault(partitionId,
                                                                                     0L) *
                                                      unitRate);
                    // 更新统计变量中该store的分区数量
                    partitionCount += 1;
                    pair.setValue(partitionCount);
                    tmpList.add(pair);
                    break;
                } else {
                    tmpList.add(pair);
                }
            }
            minHeap.addAll(tmpList);
        }
        //检查是否未存在未分配目标store的分区
        List<Integer> remainPartitions = new ArrayList<>();
        movedPartitions.forEach((partId, storePair) -> {
            if (storePair.getValue() == 0L) {
                remainPartitions.add(partId);
            }
        });
        if (remainPartitions.size() > 0) {
            resultMap.put("flag", false);
            resultMap.put("movedPartitions", null);
        } else {
            resultMap.put("flag", true);
            resultMap.put("movedPartitions", movedPartitions);
        }
        return resultMap;

    }

    public Map<Integer, KVPair<Long, Long>> movePartitions(
            Map<Integer, KVPair<Long, Long>> movedPartitions) {
        if (!isLeader()) {
            return null;
        }
        // 开始迁移
        log.info("begin move partitions:");
        movedPartitions.forEach((partId, storePair) -> {
            // 源和目标storeID都不为0
            if (storePair.getKey() > 0 && storePair.getValue() > 0) {
                partitionService.movePartitionsShard(partId, storePair.getKey(),
                                                     storePair.getValue());
            }
        });
        return movedPartitions;
    }


}
