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

package org.apache.hugegraph.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.business.BusinessHandlerImpl;
import org.apache.hugegraph.store.business.DataMover;
import org.apache.hugegraph.store.cmd.HgCmdClient;
import org.apache.hugegraph.store.cmd.HgCmdProcessor;
import org.apache.hugegraph.store.cmd.UpdatePartitionRequest;
import org.apache.hugegraph.store.cmd.UpdatePartitionResponse;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.PartitionManager;
import org.apache.hugegraph.store.meta.ShardGroup;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.metric.HgMetricService;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.apache.hugegraph.store.options.PartitionEngineOptions;
import org.apache.hugegraph.store.pd.DefaultPdProvider;
import org.apache.hugegraph.store.pd.FakePdServiceProvider;
import org.apache.hugegraph.store.pd.PdProvider;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.raft.RaftOperation;
import org.apache.hugegraph.store.util.HgRaftError;
import org.apache.hugegraph.store.util.Lifecycle;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.Endpoint;

import lombok.extern.slf4j.Slf4j;

/**
 * The core class of the storage engine, initializing PD client and raft client
 */
@Slf4j
public class HgStoreEngine implements Lifecycle<HgStoreEngineOptions>, HgStoreStateListener {

    private final static HgStoreEngine instance = new HgStoreEngine();
    private static ConcurrentHashMap<Integer, Object> engineLocks = new ConcurrentHashMap<>();
    // 分区raft引擎，key为GraphName_PartitionID
    private final Map<Integer, PartitionEngine> partitionEngines = new ConcurrentHashMap<>();
    private RpcServer rpcServer;
    private HgStoreEngineOptions options;
    private PdProvider pdProvider;
    private HgCmdClient hgCmdClient;
    private PartitionManager partitionManager;
    private HeartbeatService heartbeatService;
    private BusinessHandler businessHandler;
    private HgMetricService metricService;
    private DataMover dataMover;

    public static HgStoreEngine getInstance() {
        return instance;
    }

    /**
     * 1、读取StoreId，向pd注册，初次注册由PD生成StoreId，存储到本地
     * 2、注册成功，启动raft服务
     * 3、定时发送Store心跳和Partition心跳，与PD保持联系
     *
     * @param opts
     * @return
     */
    @Override
    public synchronized boolean init(final HgStoreEngineOptions opts) {
        if (rpcServer != null) {
            log.info("HgStoreEngine already started.");
            return true;
        }

        this.options = opts;

        BusinessHandlerImpl.initRocksdb(opts.getRocksdbConfig(), getRocksdbListener());

        if (opts.isFakePD()) {
            pdProvider = new FakePdServiceProvider(opts.getFakePdOptions());
        } else {
            pdProvider = new DefaultPdProvider(opts.getPdAddress());
            pdProvider.addPartitionInstructionListener(new PartitionInstructionProcessor(this));
        }
        options.setPdProvider(pdProvider);

        partitionManager = new PartitionManager(pdProvider, opts);

        partitionManager.addPartitionChangedListener(new PartitionChangedListener());

        businessHandler = new BusinessHandlerImpl(partitionManager);
        // 需要businessHandler 初始化后
        partitionManager.load();

        rpcServer = createRaftRpcServer(opts.getRaftAddress());

        hgCmdClient = new HgCmdClient();
        hgCmdClient.init(new RpcOptions(), (graphName, ptId) -> {
            // 分裂的时候，还未及时的上报pd
            if (getPartitionEngine(ptId) != null) {
                return getPartitionEngine(ptId).waitForLeader(
                        options.getWaitLeaderTimeout() * 1000);
            } else {
                // 可能出现跨分区的迁移
                Metapb.Shard shard = pdProvider.getPartitionLeader(graphName, ptId);
                return JRaftUtils.getEndPoint(
                        pdProvider.getStoreByID(shard.getStoreId()).getRaftAddress());
            }
        });

        heartbeatService = new HeartbeatService(this);
        heartbeatService.setStoreMetadata(partitionManager.getStoreMetadata());
        heartbeatService.addStateListener(this).init(options);

        metricService = HgMetricService.getInstance();
        metricService.setHgStoreEngine(this).init(null);

        dataMover = opts.getDataTransfer();
        if (dataMover != null) {
            this.dataMover.setBusinessHandler(this.businessHandler);
            this.dataMover.setCmdClient(hgCmdClient);
        }
        return true;
    }

    /**
     * 创建raft rpc server，用于store之间通讯
     */
    private RpcServer createRaftRpcServer(String raftAddr) {
        Endpoint endpoint = JRaftUtils.getEndPoint(raftAddr);
        RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(endpoint,
                                                                       JRaftUtils.createExecutor(
                                                                               "RAFT-RPC-",
                                                                               options.getRaftRpcThreadPoolSize()),
                                                                       null);
        HgCmdProcessor.registerProcessor(rpcServer, this);
        rpcServer.init(null);
        return rpcServer;
    }

    @Override
    public void shutdown() {
        if (rpcServer == null) {
            return;
        }
        partitionEngines.forEach((k, v) -> {
            v.shutdown();
        });
        partitionEngines.clear();
        rpcServer.shutdown();
        // HgStoreEngine.init function check rpcServer whether is null, skipped if the instance
        // exists even shut down.
        rpcServer = null;
        heartbeatService.shutdown();
        metricService.shutdown();
        // close all db session
        RocksDBFactory.getInstance().releaseAllGraphDB();
    }

    public void snapshotForTest() {
        partitionEngines.forEach((k, v) -> {
            v.snapshot();
        });
    }

    /**
     * Store注册状态发生改变
     */
    @Override
    public void stateChanged(Store store, Metapb.StoreState oldState, Metapb.StoreState newState) {
        log.info("stateChanged, oldState {}, newState {}", oldState, newState);
        if (newState == Metapb.StoreState.Up) {
            // 状态变为上线，记录store信息
            partitionManager.setStore(store);
            partitionManager.loadPartition();
            restoreLocalPartitionEngine();
        }
    }

    /**
     * 恢复本地的PartitionEngine，恢复PD返回的分区信息
     * 1、需要检查本次保存的分区，删除作废的分区
     */
    public void restoreLocalPartitionEngine() {
        try {
            if (!options.isFakePD()) {  // FakePD模式不需要同步
                partitionManager.syncPartitionsFromPD(partition -> {
                    log.warn(
                            "The local partition information is inconsistent with the PD server. " +
                            "Please delete the redundant data manually,  {}", partition);
                });
            }
            partitionManager.getPartitions().forEach((k, g) -> {
                g.forEach((id, p) -> {
                    try {
                        createPartitionEngine(p);
                    } catch (Exception e) {
                        log.error("Partition {}-{} restore  exception {}", p.getGraphName(),
                                  p.getId(), e);
                    }
                });
            });
        } catch (PDException e) {
            log.error("HgStoreEngine restoreLocalPartitionEngine error {}", e);
        }
    }

    /**
     * 收到store raft addr 变更，需要重新创建raft group
     *
     * @param storeId 变更的store id
     */
    public void rebuildRaftGroup(long storeId) {
        partitionEngines.forEach((partId, engine) -> {
            try {
                var partitions = pdProvider.getPartitionsByStore(storeId);
                if (partitions.size() > 0) {
                    var shards = pdProvider.getShardGroup(partId).getShardsList();
                    if (shards.stream().anyMatch(s -> s.getStoreId() == storeId)) {
                        var peers = partitionManager.shards2Peers(shards);
                        Configuration initConf = engine.getOptions().getConf();
                        if (initConf == null) {
                            engine.getOptions().setPeerList(peers);
                        } else {
                            peers.stream()
                                 .forEach(peer -> initConf.addPeer(JRaftUtils.getPeerId(peer)));
                        }

                        // engine.getOptions().getConf().setPeers();
                        engine.restartRaftNode();
                    }
                }
            } catch (PDException e) {
                log.error("rebuild raft group error: {}", e.getMessage());
            }
        });
    }

    /**
     * 创建 raft Node
     *
     * @param partition
     * @return
     */
    public PartitionEngine createPartitionEngine(Partition partition) {
        return createPartitionEngine(partition, null);
    }

    public PartitionEngine createPartitionEngine(Partition partition, Configuration conf) {
        partitionManager.updatePartition(partition, false);

        var shardGroup = partitionManager.getShardGroup(partition.getId());
        return createPartitionEngine(partition.getId(), shardGroup, conf);
    }

    private PartitionEngine createPartitionEngine(int groupId, ShardGroup shardGroup,
                                                  Configuration conf) {
        PartitionEngine engine;
        if ((engine = partitionEngines.get(groupId)) == null) {
            engineLocks.computeIfAbsent(groupId, k -> new Object());
            synchronized (engineLocks.get(groupId)) {
                // 分区分裂时特殊情况(集群中图分区数量不一样)，会导致分裂的分区，可能不在本机器上.
                if (conf != null) {
                    var list = conf.listPeers();
                    list.addAll(conf.listLearners());
                    if (!list.stream().anyMatch(
                            p -> p.getEndpoint().toString().equals(options.getRaftAddress()))) {
                        log.info(
                                "raft {}, conf {} does not contains raft address:{}, skipped " +
                                "create partition engine",
                                groupId, conf, options.getRaftAddress());
                        return null;
                    }
                } else {
                    var storeId = partitionManager.getStore().getId();
                    if (!shardGroup.getShards().stream().anyMatch(s -> s.getStoreId() == storeId)) {
                        log.info("raft {}, shard group {} does not contains current storeId {}, " +
                                 "skipped create partition engine", groupId, shardGroup, storeId);
                        return null;
                    }
                }

                if ((engine = partitionEngines.get(groupId)) == null) {
                    log.info("createPartitionEngine {}, with shards: {}", groupId, shardGroup);

                    engine = new PartitionEngine(this, shardGroup);
                    PartitionEngineOptions ptOpts = new PartitionEngineOptions();
                    if (conf != null) {
                        ptOpts.setConf(conf);
                    } else {
                        ptOpts.setPeerList(partitionManager.getPartitionPeers(shardGroup));
                    }
                    ptOpts.setGroupId(groupId);

                    ptOpts.setRaftAddress(options.getRaftAddress());
                    ptOpts.setRaftDataPath(partitionManager.getRaftDataPath(groupId));
                    ptOpts.setRaftSnapShotPath(partitionManager.getRaftSnapShotPath(groupId));
                    ptOpts.setRaftOptions(options.getRaftOptions());
                    // raft任务处理器
                    ptOpts.setTaskHandler(options.getTaskHandler());

                    // 分区状态监听
                    engine.addStateListener(this.heartbeatService);
                    engine.init(ptOpts);
                    partitionEngines.put(ptOpts.getGroupId(), engine);
                }
            }
        }
        // 检查是否活跃，如果不活跃，则重新创建
        engine.checkActivity();
        return engine;
    }

    /**
     * 创建 raft分组，除了创建本地raft node，还要通知其他peer创建raft node
     * 1、遍历partition.shards
     * 2、根据storeId获取Store信息
     * 3、建立向其他store的raft rpc，发送StartRaft消息
     *
     * @param partition
     * @return
     */
    public PartitionEngine createPartitionGroups(Partition partition) {
        PartitionEngine engine = partitionEngines.get(partition.getId());
        if (engine == null) {
            engine = createPartitionEngine(partition);
            if (engine == null) {
                return null;
            }

            var shardGroup = partitionManager.getShardGroup(partition.getId());
            if (shardGroup != null) {
                // raft不存在，通知follower创建raft
                shardGroup.getShards().forEach((shard) -> {
                    Store store = partitionManager.getStore(shard.getStoreId());
                    if (store == null || partitionManager.isLocalStore(store)) {
                        return;
                    }
                    // 向其他peer发消息，创建raft 分组。此处是异步发送
                    hgCmdClient.createRaftNode(store.getRaftAddress(), List.of(partition),
                                               status -> {
                                                   log.info(
                                                           "send to {} createRaftNode rpc call " +
                                                           "result {} partitionId {}",
                                                           store.getRaftAddress(), status,
                                                           partition.getId());
                                               });
                });
            }
        } else {
            // raft存在，修改分区列表，通过raft同步给follower
            engine = createPartitionEngine(partition);
        }
        return engine;
    }

    public void destroyPartitionGroups(Partition partition) {
        var shardGroup = partitionManager.getShardGroup(partition.getId());
        if (shardGroup != null) {
            shardGroup.getShards().forEach((shard) -> {
                Store store = partitionManager.getStore(shard.getStoreId());
                if (store == null) {
                    return;
                }
                // 向其他peer发消息，创建raft 分组。此处是异步发送
                hgCmdClient.destroyRaftNode(store.getRaftAddress(),
                                            Arrays.asList(new Partition[]{partition}),
                                            status -> {
                                                log.info(
                                                        "send to {} - {} DestroyRaftNode rpc call" +
                                                        " result {}",
                                                        store.getRaftAddress(), partition.getId(),
                                                        status);
                                            });
            });
        }
    }

    /**
     * 停止分区，并销毁数据
     */
    public synchronized void destroyPartitionEngine(Integer groupId, List<String> graphNames) {
        log.info("Partition {} start to be destroyed", groupId);
        if (!partitionEngines.containsKey(groupId)) {
            return;
        }
        PartitionEngine ptEngine = partitionEngines.get(groupId);
        graphNames.forEach(graphName -> {
            ptEngine.removePartition(graphName);
            // 删除数据
            businessHandler.deletePartition(graphName, groupId);
        });

        if (ptEngine.getPartitions().size() == 0) {
            ptEngine.destroy();
            partitionEngines.remove(groupId);
            // 删除对应的db文件夹
            businessHandler.destroyGraphDB(graphNames.get(0), groupId);
        } else {
            graphNames.forEach(graphName -> {
                businessHandler.dbCompaction(graphName, groupId);
            });
        }
        log.info("Partition {} has been destroyed", groupId);
    }

    /**
     * 删除图数据，删除本地数据，并删除PD上的分区信息
     */
    public void deletePartition(Integer groupId, String graphName) {
        log.info("Partition {}-{} deletePartition", graphName, groupId);
        if (!partitionEngines.containsKey(groupId)) {
            return;
        }
        PartitionEngine ptEngine = partitionEngines.get(groupId);
        ptEngine.removePartition(graphName);
        // 删除数据
        businessHandler.deletePartition(graphName, groupId);
        //通知PD删除分区数据
        if (ptEngine.isLeader()) {
            synchronized (this) {
                partitionManager.deletePartition(graphName, groupId);
            }
        }
    }

    /**
     * 获取所有的leader分区
     *
     * @return
     */
    public List<PartitionEngine> getLeaderPartition() {
        List<PartitionEngine> partitions = new ArrayList<>();
        this.partitionEngines.forEach((k, v) -> {
            if (v.isLeader()) {
                partitions.add(v);
            }
        });
        return partitions;
    }

    /**
     * 获取分区所有活跃的peer
     *
     * @return
     */
    public Map<Long, PeerId> getAlivePeers(int groupId) {
        PartitionEngine engine = this.partitionEngines.get(groupId);
        try {
            if (engine != null) {
                return engine.getAlivePeers();
            }
        } catch (Exception e) {
            log.error("getAlivePeers {}", e);
        }
        return new HashMap<>();
    }

    /**
     * 获取分区的最后提交的日志id
     *
     * @param groupId
     * @return
     */
    public long getLeaderTerm(int groupId) {
        PartitionEngine engine = this.partitionEngines.get(groupId);
        return engine.getLeaderTerm();
    }

    public long getCommittedIndex(int groupId) {
        PartitionEngine engine = this.partitionEngines.get(groupId);
        if (engine != null) {
            return engine.getCommittedIndex();
        }
        return 0;
    }

    public RpcServer getRaftRpcServer() {
        return rpcServer;
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    // For test
    public void setPartitionManager(PartitionManager ptm) {
        this.partitionManager = ptm;
    }

    public DataMover getDataMover() {
        return dataMover;
    }

    public PdProvider getPdProvider() {
        return pdProvider;
    }

    public BusinessHandler getBusinessHandler() {
        return businessHandler;
    }

    public HgCmdClient getHgCmdClient() {
        return hgCmdClient;
    }

    public HeartbeatService getHeartbeatService() {
        return heartbeatService;
    }

    public boolean isClusterReady() {
        return heartbeatService.isClusterReady();
    }

    public List<String> getDataLocations() {
        return partitionManager.getStoreMetadata().getDataLocations();
    }

    /**
     * 添加raft任务
     * 1、检查partition是否存在
     * 1.1、如果不存在，则向PD查询分区是否属于本地
     * 1.1.1 如果分区属于本地，则创建raft分组,并通知其他Store
     * 1.1.2 如果分区不属于本地，则抛出异常
     * 1.2 检查Partition是否是leader
     * 1.2.1 如果是leader，则提交任务
     * 1.2.2 否则，返回错误
     *
     * @param partId
     * @param operation
     */
    public void addRaftTask(String graphName, Integer partId, RaftOperation operation,
                            RaftClosure closure) {
        PartitionEngine engine = getPartitionEngine(graphName, partId);
        if (engine == null) {
            engineLocks.computeIfAbsent(partId, k -> new Object());
            synchronized (engineLocks.get(partId)) {
                engine = getPartitionEngine(graphName, partId);
                if (engine == null) {
                    Partition partition = partitionManager.findPartition(graphName, partId);
                    if (partition != null) {
                        engine = this.createPartitionGroups(partition);
                        // 可能迁移，不应该创建, 放到 synchronize体中，避免后面的
                        if (engine != null) {
                            engine.waitForLeader(options.getWaitLeaderTimeout() * 1000);
                        }
                    }
                }
            }
        }

        if (engine != null) {
            // 等待Leader
            Endpoint leader = engine.waitForLeader(options.getWaitLeaderTimeout() * 1000);
            if (engine.isLeader()) {
                engine.addRaftTask(operation, closure);
            } else if (leader != null) {
                // 当前不是leader，返回leader所在的storeId
                Store store = partitionManager.getStoreByRaftEndpoint(engine.getShardGroup(),
                                                                      leader.toString());
                if (store.getId() == 0) {
                    // 本地未找到Leader的Store信息，可能Partition还未同步过来，重新向Leader获取。
                    Store leaderStore = hgCmdClient.getStoreInfo(leader.toString());
                    store = leaderStore != null ? leaderStore : store;
                    log.error("getStoreByRaftEndpoint error store:{}, shard: {}, leader is {}",
                              store, engine.getShardGroup().toString(), leader);
                }
                // Leader 不是本机，通知客户端
                closure.onLeaderChanged(partId, store.getId());
                closure.run(new Status(HgRaftError.NOT_LEADER.getNumber(),
                                       String.format("Partition %s-%d leader changed to %x",
                                                     graphName, partId, store.getId())));
                log.error("Raft Partition {}-{} not leader, redirectTo leader {}.", graphName,
                          partId, leader);
            } else {
                closure.run(new Status(HgRaftError.WAIT_LEADER_TIMEOUT.getNumber(),
                                       HgRaftError.WAIT_LEADER_TIMEOUT.getMsg()));
                log.error("Partition {}-{} waiting for leader timeout.", graphName, partId);
            }
        } else {
            closure.run(
                    new Status(HgRaftError.NOT_LOCAL.getNumber(), HgRaftError.NOT_LOCAL.getMsg()));
            log.error("Partition {}-{} does not belong to local store.", graphName, partId);
        }
    }

    public PartitionEngine getPartitionEngine(Integer partitionId) {
        PartitionEngine engine = partitionEngines.get(partitionId);
        return engine;
    }

    public PartitionEngine getPartitionEngine(String graphName, Integer partitionId) {
        PartitionEngine engine = partitionEngines.get(partitionId);
        if (engine != null && engine.hasPartition(graphName)) {
            return engine;
        }
        return null;
    }

    public Map<Integer, PartitionEngine> getPartitionEngines() {
        return partitionEngines;
    }

    public Map<String, NodeMetrics> getNodeMetrics() {
        Map<String, NodeMetrics> metrics = new HashMap();
        partitionEngines.forEach((k, v) -> {
            metrics.put(Integer.toString(k), v.getNodeMetrics());
        });
        return metrics;
    }

    /**
     * Number of raft-group.
     *
     * @return
     */
    public int getRaftGroupCount() {
        return partitionEngines.size();
    }

    /**
     * 监听rocksdb事件
     *
     * @return
     */
    private RocksDBFactory.RocksdbChangedListener getRocksdbListener() {
        return new RocksDBFactory.RocksdbChangedListener() {
            @Override
            public void onCompacted(String dbName) {
                String sid = dbName.substring(dbName.lastIndexOf("/") + 1);
                try {
                    Integer groupId = Integer.parseInt(sid);
                    PartitionEngine engine = getPartitionEngine(groupId);
                    if (engine != null) {
                        engine.addBlankRaftTask();
                    }
                } catch (Exception e) {

                }
            }
        };
    }

    class PartitionChangedListener implements PartitionManager.PartitionChangedListener {

        /**
         * Partition对象发生改变，leader通知到其他的follower
         */
        @Override
        public void onChanged(Partition partition) {
            PartitionEngine engine = getPartitionEngine(partition.getId());

            if (engine != null && engine.isLeader()) {
                try {
                    engine.addRaftTask(RaftOperation.create(RaftOperation.SYNC_PARTITION,
                                                            partition.getProtoObj()),
                                       new RaftClosure() {
                                           @Override
                                           public void run(Status status) {
                                               log.info(
                                                       "Partition {}-{}-{} sync partition status " +
                                                       "is {}",
                                                       partition.getGraphName(), partition.getId(),
                                                       partition.getWorkState(),
                                                       status);
                                           }
                                       });
                } catch (IOException e) {
                    log.error("Partition {}-{} sync partition exception {}",
                              partition.getGraphName(), partition.getId(), e);
                }
            }
        }

        /**
         * Partition对象key范围、状态发生改变，通过主动寻找leader再通知到其他的follower
         */
        @Override
        public UpdatePartitionResponse rangeOrStateChanged(UpdatePartitionRequest request) {
            UpdatePartitionResponse response = null;
            try {
                response = hgCmdClient.raftUpdatePartition(request);

                log.info("not leader request threadId:{} pId:{} range:{}-{} state:{} response:{}",
                         Thread.currentThread().getId(), request.getPartitionId(),
                         request.getStartKey(),
                         request.getEndKey(), request.getWorkState(), response.getStatus());

            } catch (Exception e) {
                e.printStackTrace();
            }

            return response;
        }

    }
}
