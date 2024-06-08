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

import static org.apache.hugegraph.pd.grpc.MetaTask.TaskType.Clean_Partition;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.cmd.BatchPutRequest;
import org.apache.hugegraph.store.cmd.CleanDataRequest;
import org.apache.hugegraph.store.cmd.DbCompactionRequest;
import org.apache.hugegraph.store.cmd.HgCmdClient;
import org.apache.hugegraph.store.cmd.UpdatePartitionRequest;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.PartitionManager;
import org.apache.hugegraph.store.meta.Shard;
import org.apache.hugegraph.store.meta.ShardGroup;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.meta.TaskManager;
import org.apache.hugegraph.store.options.PartitionEngineOptions;
import org.apache.hugegraph.store.raft.HgStoreStateMachine;
import org.apache.hugegraph.store.raft.RaftClosure;
import org.apache.hugegraph.store.raft.RaftOperation;
import org.apache.hugegraph.store.raft.RaftStateListener;
import org.apache.hugegraph.store.raft.RaftTaskHandler;
import org.apache.hugegraph.store.raft.util.RaftUtils;
import org.apache.hugegraph.store.snapshot.HgSnapshotHandler;
import org.apache.hugegraph.store.util.FutureClosure;
import org.apache.hugegraph.store.util.HgRaftError;
import org.apache.hugegraph.store.util.HgStoreException;
import org.apache.hugegraph.store.util.Lifecycle;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.DefaultJRaftServiceFactory;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.core.Replicator;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage;
import com.alipay.sofa.jraft.storage.log.RocksDBSegmentLogStorage;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.SystemPropertyUtil;
import com.alipay.sofa.jraft.util.ThreadId;
import com.alipay.sofa.jraft.util.Utils;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;
import com.google.protobuf.CodedInputStream;

import lombok.extern.slf4j.Slf4j;

/**
 * Partition processing engine
 */
@Slf4j
public class PartitionEngine implements Lifecycle<PartitionEngineOptions>, RaftStateListener {

    private static final ThreadPoolExecutor raftLogWriteExecutor = null;
    public final String raftPrefix = "hg_";

    private final HgStoreEngine storeEngine;
    private final PartitionManager partitionManager;
    private final List<PartitionStateListener> stateListeners;
    private final ShardGroup shardGroup;
    private final AtomicBoolean changingPeer;
    private final AtomicBoolean snapshotFlag;
    private final Object leaderChangedEvent = "leaderChangedEvent";
    /**
     * Default value size threshold to decide whether it will be stored in segments or rocksdb,
     * default is 4K.
     * When the value size is less than 4K, it will be stored in rocksdb directly.
     */
    private final int DEFAULT_VALUE_SIZE_THRESHOLD = SystemPropertyUtil.getInt(
            "jraft.log_storage.segment.value.threshold.bytes", 4 * 1024);
    /**
     * Default checkpoint interval in milliseconds.
     */
    private final int DEFAULT_CHECKPOINT_INTERVAL_MS = SystemPropertyUtil.getInt(
            "jraft.log_storage.segment.checkpoint.interval.ms", 5000);

    private PartitionEngineOptions options;
    private HgStoreStateMachine stateMachine;
    private RaftGroupService raftGroupService;
    private TaskManager taskManager;
    private Node raftNode;
    private boolean started;

    public PartitionEngine(HgStoreEngine storeEngine, ShardGroup shardGroup) {
        this.storeEngine = storeEngine;
        this.shardGroup = shardGroup;
        this.changingPeer = new AtomicBoolean(false);
        this.snapshotFlag = new AtomicBoolean(false);
        partitionManager = storeEngine.getPartitionManager();
        stateListeners = Collections.synchronizedList(new ArrayList());
    }
//    public static ThreadPoolExecutor getRaftLogWriteExecutor() {
//        if (raftLogWriteExecutor == null) {
//            synchronized (PartitionEngine.class) {
//                if (raftLogWriteExecutor == null)
//                    raftLogWriteExecutor = RocksDBSegmentLogStorage.createDefaultWriteExecutor();
//            }
//        }
//        return raftLogWriteExecutor;
//    }

    /**
     * 记录使用本raft的分区信息
     */

    public synchronized void loadPartitionFromSnapshot(Partition partition) {
        partitionManager.loadPartitionFromSnapshot(partition);
    }

    public List<Metapb.Partition> loadPartitionsFromLocalDb() {
        return partitionManager.loadPartitionsFromDb(getGroupId());
    }

    public void removePartition(String graphName) {
        partitionManager.removePartition(graphName, options.getGroupId());
    }

    public boolean hasPartition(String graphName) {
        return partitionManager.hasPartition(graphName, getGroupId());
    }

    public Integer getGroupId() {
        return options.getGroupId();
    }

    /**
     * Initialize the raft engine
     *
     * @return
     */
    @Override
    public synchronized boolean init(PartitionEngineOptions opts) {
        this.options = opts;
        if (this.started) {
            log.info("PartitionEngine: {} already started.", this.options.getGroupId());
            return true;
        }

        log.info("PartitionEngine starting: {}", this);
        this.taskManager = new TaskManager(storeEngine.getBusinessHandler(), opts.getGroupId());
        HgSnapshotHandler snapshotHandler = new HgSnapshotHandler(this);
        this.stateMachine = new HgStoreStateMachine(opts.getGroupId(), snapshotHandler);
        // probably null in test case
        if (opts.getTaskHandler() != null) {
            this.stateMachine.addTaskHandler(opts.getTaskHandler());
        }
        this.stateMachine.addTaskHandler(new TaskHandler());

        // 监听分组leader是否发生改变
        this.stateMachine.addStateListener(this);

        new File(options.getRaftDataPath()).mkdirs();

        Configuration initConf = opts.getConf();
        if (initConf == null) {
            String peersList = StringUtils.join(options.getPeerList(), ",");
            initConf = new Configuration();
            initConf.parse(peersList);
        }

        // 设置Node参数，包括日志存储路径和状态机实例
        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setFsm(stateMachine);
        nodeOptions.setEnableMetrics(true);
        // 日志路径
        nodeOptions.setLogUri(options.getRaftDataPath() + "/log");
        // raft元数据路径
        nodeOptions.setRaftMetaUri(options.getRaftDataPath() + "/meta");
        // 快照路径
        nodeOptions.setSnapshotUri(options.getRaftSnapShotPath() + "/snapshot");
        nodeOptions.setSharedTimerPool(true);
        nodeOptions.setSharedElectionTimer(true);
        nodeOptions.setSharedSnapshotTimer(true);
        nodeOptions.setSharedStepDownTimer(true);
        nodeOptions.setSharedVoteTimer(true);
        nodeOptions.setFilterBeforeCopyRemote(true);

        nodeOptions.setServiceFactory(new DefaultJRaftServiceFactory() {
            @Override
            public LogStorage createLogStorage(final String uri, final RaftOptions raftOptions) {
                if (options.getRaftOptions().isUseRocksDBSegmentLogStorage()) {
                    return new RocksDBSegmentLogStorage(uri, raftOptions);
                } else {
                    return new RocksDBLogStorage(uri, raftOptions);
                }
            }
        });
        // 初始集群
        nodeOptions.setInitialConf(initConf);
        // 快照时间间隔
        nodeOptions.setSnapshotIntervalSecs(options.getRaftOptions().getSnapshotIntervalSecs());

        //nodeOptions.setSnapshotLogIndexMargin(options.getRaftOptions()
        // .getSnapshotLogIndexMargin());

        nodeOptions.setRpcConnectTimeoutMs(options.getRaftOptions().getRpcConnectTimeoutMs());
        nodeOptions.setRpcDefaultTimeout(options.getRaftOptions().getRpcDefaultTimeout());
        nodeOptions.setRpcInstallSnapshotTimeout(
                options.getRaftOptions().getRpcInstallSnapshotTimeout());
        nodeOptions.setElectionTimeoutMs(options.getRaftOptions().getElectionTimeoutMs());
        // 设置raft配置
        RaftOptions raftOptions = nodeOptions.getRaftOptions();
        raftOptions.setDisruptorBufferSize(options.getRaftOptions().getDisruptorBufferSize());
        raftOptions.setMaxEntriesSize(options.getRaftOptions().getMaxEntriesSize());
        raftOptions.setMaxReplicatorInflightMsgs(
                options.getRaftOptions().getMaxReplicatorInflightMsgs());
        raftOptions.setMaxByteCountPerRpc(1024 * 1024);
        nodeOptions.setEnableMetrics(true);

        final PeerId serverId = JRaftUtils.getPeerId(options.getRaftAddress());

        // 构建raft组并启动raft
        this.raftGroupService = new RaftGroupService(raftPrefix + options.getGroupId(),
                                                     serverId, nodeOptions,
                                                     storeEngine.getRaftRpcServer(), true);
        this.raftNode = raftGroupService.start(false);
        this.raftNode.addReplicatorStateListener(new ReplicatorStateListener());

        // 检查pd返回的peers是否和本地一致，如果不一致，重置peerlist
        if (this.raftNode != null) {
            //TODO 检查peer列表，如果peer发生改变，进行重置
            started = true;
        }

        log.info("PartitionEngine start successfully: id = {}, peers list = {}",
                 options.getGroupId(), nodeOptions.getInitialConf().getPeers());
        return this.started;
    }

    public HgStoreEngine getStoreEngine() {
        return this.storeEngine;
    }

    public ShardGroup getShardGroup() {
        return shardGroup;
    }

    /**
     * 1、收到PD发送的分区迁移指令，向状态机添加迁移任务，状态为新建
     * 2、执行状态机消息，添加到任务队列，并执行任务
     * 3、比较新旧peer，查找出新增和删除的peer
     * 4、如果存在新增的peer
     * 4.1、对于新增的peer，通知peer创建raft状态机
     * 4.2、以learner方式加入raft group
     * 4.3、监听snapshot同步事件，重复执行步骤3
     * 5、不存在新增的peer
     * 5.1、移除learner，并等待返回
     * 5.2、修改learner为peer，加入raft group
     * 6、存在被删除的peer
     * 6.1、通知peer，删除状态机并删除数据
     *
     * @param peers
     * @param done
     * @return true表示完成，false表示未完成
     */
    public Status changePeers(List<String> peers, final Closure done) {
        if (ListUtils.isEqualList(peers, RaftUtils.getPeerEndpoints(raftNode))) {
            return Status.OK();
        }

        Status result = HgRaftError.TASK_CONTINUE.toStatus();
        List<String> oldPeers = RaftUtils.getAllEndpoints(raftNode);
        log.info("Raft {} changePeers start, old peer is {}, new peer is {}",
                 getGroupId(), oldPeers, peers);
        // 检查需要新增的peer。
        List<String> addPeers = ListUtils.removeAll(peers, oldPeers);
        // 需要删除的learner。可能peer发生改变
        List<String> removedPeers = ListUtils.removeAll(RaftUtils.getLearnerEndpoints(raftNode),
                                                        peers);

        HgCmdClient rpcClient = storeEngine.getHgCmdClient();
        // 生成新的Configuration对象
        Configuration oldConf = getCurrentConf();
        Configuration conf = oldConf.copy();
        if (!addPeers.isEmpty()) {
            addPeers.forEach(peer -> {
                conf.addLearner(JRaftUtils.getPeerId(peer));
            });

            doSnapshot((RaftClosure) status -> {
                log.info("Raft {} snapshot before add learner, result:{}", getGroupId(), status);
            });

            FutureClosure closure = new FutureClosure(addPeers.size());
            addPeers.forEach(peer -> Utils.runInThread(() -> {
                // 1. 创建新peer的raft对象
                rpcClient.createRaftNode(peer, partitionManager.getPartitionList(getGroupId()),
                                         conf, status -> {
                            closure.run(status);
                            if (!status.isOk()) {
                                log.error("Raft {} add node {} error {}",
                                          options.getGroupId(), peer, status);
                            }
                        });
            }));
            closure.get();
        } else {
            // 3. 检查learner是否完成快照同步
            boolean snapshotOk = true;
            for (PeerId peerId : raftNode.listLearners()) {
                Replicator.State state = getReplicatorState(peerId);
                if (state == null || state != Replicator.State.Replicate) {
                    snapshotOk = false;
                    break;
                }
                log.info("Raft {} {} getReplicatorState {}", getGroupId(), peerId, state);
            }
            if (snapshotOk && !conf.listLearners().isEmpty()) {
                // 4. 删除learner，以peer重新加入
                FutureClosure closure = new FutureClosure();
                raftNode.removeLearners(conf.listLearners(), closure);
                if (closure.get().isOk()) {
                    conf.listLearners().forEach(peerId -> {
                        conf.addPeer(peerId);
                        conf.removeLearner(peerId);
                    });
                    result = Status.OK();
                } else {
                    // 失败了重试
                    result = HgRaftError.TASK_ERROR.toStatus();
                }
            } else if (snapshotOk) {
                result = Status.OK();   // 没有learner，说明只做删除操作
            }
        }
        if (result.isOk()) {
            // 同步完成，删除旧peer
            removedPeers.addAll(ListUtils.removeAll(oldPeers, peers));
            // 检查leader是否被删除，如果是，先进行leader迁移
            if (removedPeers.contains(
                    this.getRaftNode().getNodeId().getPeerId().getEndpoint().toString())) {

                log.info("Raft {} leader is removed, needs to transfer leader {}, conf: {}",
                         getGroupId(), peers, conf);
                // only one (that's leader self), should add peer first
                if (raftNode.listPeers().size() == 1) {
                    FutureClosure closure = new FutureClosure();
                    raftNode.changePeers(conf, closure);
                    log.info("Raft {} change peer result:{}", getGroupId(), closure.get());
                }

                var status = this.raftNode.transferLeadershipTo(PeerId.ANY_PEER);
                log.info("Raft {} transfer leader status : {}", getGroupId(), status);
                // 需要重新发送指令给新leader
                return HgRaftError.TASK_ERROR.toStatus();
            }
        }

        if (!removedPeers.isEmpty()) {
            removedPeers.forEach(peer -> {
                conf.removeLearner(JRaftUtils.getPeerId(peer));
                conf.removePeer(JRaftUtils.getPeerId(peer));
            });
        }

        if (!RaftUtils.configurationEquals(oldConf, conf)) {
            // 2. 新的peer以learner身份加入
            // 5. peer切换，增加新的peer，删除旧的peer
            FutureClosure closure = new FutureClosure();
            raftNode.changePeers(conf, closure);
            if (closure.get().isOk()) {
                if (!removedPeers.isEmpty()) {
                    removedPeers.forEach(peer -> Utils.runInThread(() -> {
                        // 6. 停止已被删除的peer
                        rpcClient.destroyRaftNode(peer,
                                                  partitionManager.getPartitionList(getGroupId()),
                                                  status -> {
                                                      if (!status.isOk()) {
                                                          // TODO 失败了怎么办？
                                                          log.error("Raft {} destroy node {}" +
                                                                    " error {}",
                                                                    options.getGroupId(), peer,
                                                                    status);
                                                      }
                                                  });
                    }));
                }
            } else {
                // 失败了重试
                result = HgRaftError.TASK_ERROR.toStatus();
            }
            log.info("Raft {} changePeers result {}, conf is {}",
                     getRaftNode().getGroupId(), closure.get(), conf);
        }
        log.info("Raft {} changePeers end. {}, result is {}", getGroupId(), peers, result);
        return result;
    }

    public void addRaftTask(RaftOperation operation, RaftClosure closure) {
        if (!isLeader()) {
            closure.run(new Status(HgRaftError.NOT_LEADER.getNumber(), "Not leader"));
            return;
        }
        final Task task = new Task();
        task.setData(ByteBuffer.wrap(operation.getValues()));
        task.setDone(new HgStoreStateMachine.RaftClosureAdapter(operation, closure));
        this.raftNode.apply(task);
    }

    @Override
    public void shutdown() {
        if (!this.started) {
            return;
        }

        partitionManager.updateShardGroup(shardGroup);

        if (this.raftGroupService != null) {
            this.raftGroupService.shutdown();
            try {
                this.raftGroupService.join();
            } catch (final InterruptedException e) {
                ThrowUtil.throwException(e);
            }
        }
        this.started = false;
        log.info("PartitionEngine shutdown successfully: {}.", this);
    }

    /**
     * 重启raft引擎
     */
    public void restartRaftNode() {
        shutdown();
        log.error("Raft {} is restarting !!!", getGroupId());
        this.init(this.options);
    }

    /**
     * 检查是否活跃，如果不活跃，则重启
     */
    public void checkActivity() {
        Utils.runInThread(() -> {
            if (!this.raftNode.getNodeState().isActive()) {
                log.error("Raft {} is not activity state is {} ",
                          this.getGroupId(), raftNode.getNodeState());
                restartRaftNode();
            }
        });
    }

    /**
     * raft peer被销毁，删除日志和数据
     */
    public void destroy() {
        shutdown();
        try {
            FileUtils.deleteDirectory(new File(this.options.getRaftDataPath()));
            if (!Objects.equals(this.options.getRaftDataPath(),
                                this.options.getRaftSnapShotPath())) {
                FileUtils.deleteDirectory(new File(this.options.getRaftSnapShotPath()));
            }
        } catch (IOException e) {
            log.error("Raft {} destroy exception {}", this.options.getGroupId(), e);
        }
    }

    public boolean isLeader() {
        return this.raftNode != null && this.raftNode.isLeader(false);
    }

    public Endpoint getLeader() {
        PeerId peerId = this.raftNode.getLeaderId();
        return peerId != null ? peerId.getEndpoint() : null;
    }

    public void addStateListener(PartitionStateListener listener) {
        this.stateListeners.add(listener);
    }

    /**
     * 返回所有活跃的peer
     *
     * @return
     */
    public Map<Long, PeerId> getAlivePeers() {
        Map<Long, PeerId> peers = new HashMap<>();
        raftNode.listAlivePeers().forEach(peerId -> {
            Shard shard = partitionManager.getShardByRaftEndpoint(shardGroup,
                                                                  peerId.getEndpoint().toString());
            if (shard != null) {
                peers.put(shard.getStoreId(), peerId);
            }
        });
        return peers;
    }

    public Node getRaftNode() {
        return raftNode;
    }

    /**
     * 等待Leader被选举
     *
     * @param timeOut
     * @return
     */
    public Endpoint waitForLeader(long timeOut) {
        Endpoint leader = getLeader();
        if (leader != null) {
            return leader;
        }

        synchronized (leaderChangedEvent) {
            leader = getLeader();
            if (leader != null) {
                return leader;
            }

            long start = System.currentTimeMillis();
            while ((System.currentTimeMillis() - start < timeOut) && (leader == null)) {
                try {
                    leaderChangedEvent.wait(timeOut);
                } catch (InterruptedException e) {
                    log.error("Raft {} wait for leader exception", this.options.getGroupId(), e);
                }
                leader = getLeader();
                if (leader == null) {
                    if (partitionManager.isLocalPartition(this.options.getGroupId())) {
                        log.error("Raft {} leader not found, try to repair!",
                                  this.options.getGroupId());
                        // TODO 判断raft是否本机，如果是，尝试修复Leader，包括检查配置是否正确
                        storeEngine.createPartitionGroups(
                                partitionManager.getPartitionList(getGroupId()).get(0));
                    }
                } else {
                    log.info("Raft {} wait for leader success, latency time {}",
                             this.options.getGroupId(), System.currentTimeMillis() - start);
                }
            }
            return leader;
        }
    }

    public Map<String, Partition> getPartitions() {
        return partitionManager.getPartitions(getGroupId());
    }

    public Partition getPartition(String graphName) {
        return partitionManager.getPartition(graphName, getGroupId());
    }

    public PartitionEngineOptions getOptions() {
        return options;
    }

    @Override
    public String toString() {
        return "PartitionEngine{" + "groupId =" + this.options.getGroupId() + ", isLeader=" +
               isLeader()
               + ", options=" + this.options + '}';
    }

    /**
     * update partition leader
     *
     * @param newTerm the new term
     */
    @Override
    public void onLeaderStart(long newTerm) {
        log.info("Raft {} onLeaderStart newTerm is {}", getGroupId(), newTerm);
        // 更新shard group对象
        shardGroup.changeLeader(partitionManager.getStore().getId());

        onConfigurationCommitted(getCurrentConf());
        synchronized (leaderChangedEvent) {
            leaderChangedEvent.notifyAll();
        }

    }

    @Override
    public void onStartFollowing(final PeerId newLeaderId, final long newTerm) {
        onConfigurationCommitted(getCurrentConf());
        synchronized (leaderChangedEvent) {
            leaderChangedEvent.notifyAll();
        }
    }

    /**
     * update partition shardList
     *
     * @param conf committed configuration
     */
    @Override
    public void onConfigurationCommitted(Configuration conf) {

        try {
            //更新shardlist
            log.info("Raft {} onConfigurationCommitted, conf is {}", getGroupId(), conf.toString());
            // 根据raft endpoint查找storeId
            List<Long> peerIds = new ArrayList<>();
            for (String peer : RaftUtils.getPeerEndpoints(conf)) {
                Store store = getStoreByEndpoint(peer);
                if (store != null) {
                    peerIds.add(store.getId());
                } else {
                    log.error("Raft {} GetStoreInfo failure, {}", getGroupId(), peer);
                }
            }
            List<Long> learners = new ArrayList<>();
            for (String learner : RaftUtils.getLearnerEndpoints(conf)) {
                Store store = getStoreByEndpoint(learner);
                if (store != null) {
                    learners.add(store.getId());
                } else {
                    log.error("Raft {} GetStoreInfo failure, {}", getGroupId(), learner);
                }
            }

            shardGroup.changeShardList(peerIds, learners, partitionManager.getStore().getId());
            partitionManager.updateShardGroup(shardGroup);

            if (isLeader()) {
                // partitionManager.getPartitionList(getGroupId()).forEach(partition -> {
                //    partitionManager.changeShards(partition, shardGroup.getMetaPbShard());
                // });
                try {
                    var pdGroup = storeEngine.getPdProvider().getShardGroup(getGroupId());
                    List<String> peers = partitionManager.shards2Peers(pdGroup.getShardsList());

                    if (!ListUtils.isEqualList(peers, RaftUtils.getPeerEndpoints(raftNode))) {
                        partitionManager.getPdProvider().updateShardGroup(shardGroup.getProtoObj());
                    }

                } catch (PDException e) {
                    throw new RuntimeException(e);
                }
            }
            log.info("Raft {} onConfigurationCommitted, shardGroup {}", getGroupId(),
                     shardGroup.getMetaPbShard());

        } catch (Exception e) {
            log.error("Raft {} onConfigurationCommitted exception {}", getGroupId(), e);
        }

    }

    private Store getStoreByEndpoint(String endpoint) {
        Store store = partitionManager.getStoreByRaftEndpoint(getShardGroup(), endpoint);
        if (store == null || store.getId() == 0) {
            store = this.storeEngine.getHgCmdClient().getStoreInfo(endpoint);
        }
        return store;
    }

    @Override
    public void onDataCommitted(long index) {

    }

    @Override
    public void onError(RaftException e) {
        this.restartRaftNode();
    }

    public NodeMetrics getNodeMetrics() {
        return this.raftNode.getNodeMetrics();
    }

    public long getCommittedIndex() {
        return stateMachine.getCommittedIndex();
    }

    public long getLeaderTerm() {
        return stateMachine.getLeaderTerm();
    }

    public TaskManager getTaskManager() {
        return this.taskManager;
    }

    /**
     * 收到PD发送的leader转移指令
     *
     * @param graphName
     * @param shard
     * @return
     */
    public Status transferLeader(String graphName, Metapb.Shard shard) {
        if (!isLeader()) {
            return new Status(HgRaftError.NOT_LEADER.getNumber(), "Not leader");
        }
        String address = partitionManager.getStore(shard.getStoreId()).getRaftAddress();
        return raftNode.transferLeadershipTo(JRaftUtils.getPeerId(address));
    }

    /**
     * 接收到pd发来的修改副本指令
     * 1. 比较新旧peer，查找出新增和删除的peer
     * 2. 对于新增的peer，以learner方式加入
     * 3. 监听快照同步事件
     * 4. 快照同步完成后，调用changePeers，修改learner为follower，删除旧的peer
     */
    public void doChangeShard(final MetaTask.Task task, Closure done) {
        if (!isLeader()) {
            return;
        }

        log.info("Raft {} doChangeShard task is {}", getGroupId(), task);
        // 如果相同的分区有相同的任务在执行，忽略任务执行
        if (taskManager.partitionTaskRepeat(task.getPartition().getId(),
                                            task.getPartition().getGraphName(),
                                            task.getType().name())) {
            log.error("Raft {} doChangeShard task repeat, type:{}", getGroupId(), task.getType());
            return;
        }
        // 任务未完成，重复执行
        if (task.getState().getNumber() < MetaTask.TaskState.Task_Stop_VALUE && isLeader()) {
            Utils.runInThread(() -> {
                try {
                    // 不能在状态机中changePeers
                    List<String> peers =
                            partitionManager.shards2Peers(task.getChangeShard().getShardList());
                    HashSet<String> hashSet = new HashSet<>(peers);
                    // 任务中有相同的peers，说明任务本身有错误，任务忽略
                    if (peers.size() != hashSet.size()) {
                        log.info("Raft {} doChangeShard peer is repeat， peers：{}", getGroupId(),
                                 peers);
                    }
                    Status result;
                    if (changingPeer.compareAndSet(false, true)) {
                        result = this.changePeers(peers, done);
                    } else {
                        result = HgRaftError.TASK_ERROR.toStatus();
                    }

                    if (result.getCode() != HgRaftError.TASK_CONTINUE.getNumber()) {
                        log.info("Raft {} doChangeShard is finished, status is {}", getGroupId(),
                                 result);
                        // 任务完成，同步任务状态
                        MetaTask.Task newTask;
                        if (result.isOk()) {
                            newTask = task.toBuilder().setState(MetaTask.TaskState.Task_Success)
                                          .build();
                        } else {
                            log.warn(
                                    "Raft {} doChangeShard is failure, need to retry, status is {}",
                                    getGroupId(), result);
                            try {
                                // 减少发送次数
                                Thread.sleep(1000);
                            } catch (Exception e) {
                                log.error("wait 1s to resend retry task. got error:{}",
                                          e.getMessage());
                            }
                            newTask = task.toBuilder().setState(MetaTask.TaskState.Task_Ready)
                                          .build();
                        }
                        try {
                            // 等待过程中，可能已经shut down了
                            if (isLeader()) {
                                storeEngine.addRaftTask(newTask.getPartition().getGraphName(),
                                                        newTask.getPartition().getId(),
                                                        RaftOperation.create(
                                                                RaftOperation.SYNC_PARTITION_TASK,
                                                                newTask),
                                                        status -> {
                                                            if (!status.isOk()) {
                                                                log.error(
                                                                        "Raft {} addRaftTask " +
                                                                        "error, status is {}",
                                                                        newTask.getPartition()
                                                                               .getId(), status);
                                                            }
                                                        }
                                );
                            }
                        } catch (Exception e) {
                            log.error("Partition {}-{} update task state exception {}",
                                      task.getPartition().getGraphName(),
                                      task.getPartition().getId(), e);
                        }
                        // db 可能被销毁了，就不要更新了
                        if (this.started) {
                            taskManager.updateTask(newTask);
                        }
                    } else {
                        log.info("Raft {} doChangeShard not finished", getGroupId());
                    }
                } catch (Exception e) {
                    log.error("Raft {} doChangeShard exception {}", getGroupId(), e);
                } finally {
                    changingPeer.set(false);
                }
            });
        } else {
            // 返回消息是否被处理
            if (done != null) {
                done.run(Status.OK());
            }
        }
    }

    /**
     * 收到PD发送的分区之间转移数据
     * 1. 通知目标机器，创建raft
     * 2. 从源机器拷贝数据到目标机器
     * 3. 迁移成功后，通知PD修改分区信息
     * 4. 删除源分区
     *
     * @return
     */
    public Status moveData(MetaTask.Task task) {

        synchronized (this) {
            if (taskManager.taskExists(task.getPartition().getId(),
                                       task.getPartition().getGraphName(),
                                       task.getType().name())) {
                log.info("task : {}-{}-{} repeat", task.getPartition().getGraphName(),
                         task.getPartition().getId(),
                         task.getType().name());
                return Status.OK();
            }
            taskManager.updateTask(
                    task.toBuilder().setState(MetaTask.TaskState.Task_Doing).build());
        }

        Status status = Status.OK();

        switch (task.getType()) {
            case Split_Partition:
                status = handleSplitTask(task);
                break;
            case Move_Partition:
                status = handleMoveTask(task);
                break;
            default:
                break;
        }
        log.info("moveData {}-{}-{} result:{}", task.getPartition().getGraphName(),
                 task.getPartition().getId(),
                 task.getType().name(), status);

        if (status.isOk()) {
            // 向PD汇报任务执行结果
            partitionManager.reportTask(
                    task.toBuilder().setState(MetaTask.TaskState.Task_Success).build());
            // 更新本地任务状态
            taskManager.updateTask(
                    task.toBuilder().setState(MetaTask.TaskState.Task_Success).build());
        } else {
            partitionManager.reportTask(task.toBuilder()
                                            .setState(MetaTask.TaskState.Task_Failure)
                                            .setMessage(status.getErrorMsg()).build());
            // 更新本地任务状态
            taskManager.updateTask(task.toBuilder()
                                       .setState(MetaTask.TaskState.Task_Failure)
                                       .setMessage(status.getErrorMsg()).build());
        }

        return status;
    }

    /**
     * 对应分区分裂的任务
     *
     * @param task split partition task
     * @return task execution result
     */
    private Status handleSplitTask(MetaTask.Task task) {

        log.info("Partition {}-{} moveData {}", task.getPartition().getGraphName(),
                 task.getPartition().getId(), task);

        Status status;

        List<Metapb.Partition> targets = task.getSplitPartition().getNewPartitionList();
        List<Metapb.Partition> newPartitions = targets.subList(1, targets.size());
        try {
            for (int i = 0; i < newPartitions.size(); i++) {
                storeEngine.createPartitionGroups(new Partition(newPartitions.get(i)));
            }
            // 从源机器拷贝数据到目标机器
            status = storeEngine.getDataMover().moveData(task.getPartition(), newPartitions);

            if (status.isOk()) {
                var source = Metapb.Partition.newBuilder(targets.get(0))
                                             .setState(Metapb.PartitionState.PState_Normal)
                                             .build();
                // 更新本地key range，并同步follower
                partitionManager.updatePartition(source, true);
                storeEngine.getDataMover().updatePartitionRange(source,
                                                                (int) source.getStartKey(),
                                                                (int) source.getEndKey());
            }

            if (!status.isOk()) {
                throw new Exception(status.getErrorMsg());
            }
        } catch (Exception e) {
            log.error("Partition {}-{} moveData exception {}",
                      task.getPartition().getGraphName(), task.getPartition().getId(), e);
            status = new Status(-1, e.getMessage());
        }

        return status;
    }

    /**
     * 对应分区数据移动的task
     *
     * @param task move partition task
     * @return task execution result
     */
    private Status handleMoveTask(MetaTask.Task task) {
        Status status;
        try {
            log.info("handleMoveTask: start to copy {} data from {} to {}",
                     task.getPartition().getGraphName(),
                     task.getPartition().getId(),
                     task.getMovePartition().getTargetPartition().getId());
            status = storeEngine.getDataMover().moveData(task.getPartition(),
                                                         task.getMovePartition()
                                                             .getTargetPartition());
        } catch (Exception e) {
            log.error("handleMoveTask got exception: ", e);
            status = new Status(-1, e.getMessage());
        }
        return status;
    }

    /**
     * 对于整个图删除的清空，删除分区， 如果没有其他的图，销毁raft group.
     * 需要放到调用move data 之后
     *
     * @param graphName   graph name
     * @param partitionId partition id
     * @param keyStart    key start 用于验证
     * @param keyEnd      key end 用于验证
     * @param isLeader    是否leader，避免leader漂移，采取move data时候的leader状态
     */
    private synchronized void destroyPartitionIfGraphsNull(String graphName, int partitionId,
                                                           long keyStart, long keyEnd,
                                                           boolean isLeader) {
        Partition partition = partitionManager.getPartition(graphName, partitionId);

        // key range 校验
        if (partition != null && partition.getEndKey() == keyEnd &&
            partition.getStartKey() == keyStart) {
            log.info("remove partition id :{}, graph:{}", partition.getId(),
                     partition.getGraphName());
            storeEngine.deletePartition(partitionId, graphName);
        }

        // 没有partition engine的情况
        if (isLeader && partition == null) {
            partitionManager.deletePartition(graphName, partitionId);
        }

        if (isLeader && partition != null) {
            if (partitionManager.getPartitions(partitionId).size() == 0) {
                log.info("destroyPartitionIfGraphsNull, destroy raft group id:{}",
                         partition.getId());
                storeEngine.destroyPartitionGroups(partition);
                try {
                    // delete shard group from pd
                    partitionManager.getPdProvider().deleteShardGroup(partitionId);
                } catch (PDException e) {
                    log.error("delete shard group failed, status:{}", e.getMessage());
                }
            }
        }
    }

    public void snapshot() {
        log.info("Raft {} send snapshot command. ", this.getGroupId());
        // 空指令，占位
        this.addRaftTask(
                RaftOperation.create(RaftOperation.BLANK_TASK), status -> {
                    // 生成快照指令
                    this.addRaftTask(
                            RaftOperation.create(RaftOperation.DO_SNAPSHOT), status2 -> {
                            });
                });

    }

    protected void doSnapshot(Closure done) {
        log.info("Raft {} doSnapshot. ", this.getGroupId());

        if (this.snapshotFlag.compareAndSet(false, true)) {

            raftNode.snapshot(status -> {
                log.info("Raft {}  snapshot OK. ", this.getGroupId());
                if (done != null) {
                    done.run(status);
                }
            });
        }

        this.snapshotFlag.set(false);
    }

    public void addBlankRaftTask() {
        // 空指令，占位
        this.addRaftTask(
                RaftOperation.create(RaftOperation.BLANK_TASK), status -> {
                });
    }

    private void handleCleanOp(CleanDataRequest request) {
        // 避免清理数据过程中的leader漂移
        boolean isLeader = isLeader();
        var partition =
                partitionManager.getPartition(request.getGraphName(), request.getPartitionId());

        if (partition != null) {
            storeEngine.getDataMover().doCleanData(request);
            storeEngine.getBusinessHandler()
                       .dbCompaction(partition.getGraphName(), partition.getId());

            if (request.isDeletePartition()) {
                destroyPartitionIfGraphsNull(request.getGraphName(), request.getPartitionId(),
                                             request.getKeyStart(), request.getKeyEnd(), isLeader);
            }
        } else {
            log.info("handleCleanOp, partition is null. {}-{}", request.getGraphName(),
                     request.getPartitionId());
            partition = new Partition() {{
                setId(request.getPartitionId());
                setGraphName(request.getGraphName());
                setVersion(0);
                setStartKey(0);
                setEndKey(0);
                setWorkState(Metapb.PartitionState.PState_Normal);
            }};
        }

        // report task
        if (isLeader) {
            MetaTask.Task task = MetaTask.Task.newBuilder()
                                              .setType(Clean_Partition)
                                              .setId(request.getTaskId())
                                              .setPartition(partition.getProtoObj())
                                              .setCleanPartition(
                                                      CleanDataRequest.toCleanPartitionTask(
                                                              request))
                                              .setState(MetaTask.TaskState.Task_Success)
                                              .build();
            partitionManager.reportTask(task);
        }
    }

    public Configuration getCurrentConf() {
        return new Configuration(this.raftNode.listPeers(), this.raftNode.listLearners());
    }

    private Replicator.State getReplicatorState(PeerId peerId) {
        var replicateGroup = getReplicatorGroup();
        if (replicateGroup == null) {
            return null;
        }

        ThreadId threadId = replicateGroup.getReplicator(peerId);
        if (threadId == null) {
            return null;
        } else {
            Replicator r = (Replicator) threadId.lock();
            if (r == null) {
                return Replicator.State.Probe;
            }
            Replicator.State result = getState(r);
            threadId.unlock();
            return result;
        }
    }

    private ReplicatorGroup getReplicatorGroup() {
        var clz = this.raftNode.getClass();
        try {
            var f = clz.getDeclaredField("replicatorGroup");
            f.setAccessible(true);
            var group = (ReplicatorGroup) f.get(this.raftNode);
            f.setAccessible(false);
            return group;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            log.info("getReplicatorGroup: error {}", e.getMessage());
            return null;
        }
    }

    private Replicator.State getState(Replicator r) {
        var clz = r.getClass();
        try {
            var f = clz.getDeclaredField("state");
            f.setAccessible(true);
            var state = (Replicator.State) f.get(this.raftNode);
            f.setAccessible(false);
            return state;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            log.info("getReplicatorGroup: error {}", e.getMessage());
            return null;
        }
    }

    class ReplicatorStateListener implements Replicator.ReplicatorStateListener {

        @Override
        public void onCreated(PeerId peer) {
            log.info("Raft {} Replicator onCreated {}", getGroupId(), peer);
        }

        @Override
        public void onError(PeerId peer, Status status) {
            //  log.info("Raft {} Replicator onError {} {}", getGroupId(), peer, status);
        }

        @Override
        public void onDestroyed(PeerId peer) {

        }

        /**
         * 监听replicator状态改变，判断快照是否同步完成
         * 检查是否存在changeShard任务，如果存在，调用changeShard
         */
        @Override
        public void stateChanged(final PeerId peer, final ReplicatorState newState) {
            log.info("Raft {} Replicator stateChanged {} {}", getGroupId(), peer, newState);
            if (newState == ReplicatorState.ONLINE) {
                MetaTask.Task task = taskManager.getOneTask(getGroupId(),
                                                            MetaTask.TaskType.Change_Shard.name());
                if (task != null) {
                    doChangeShard(task, status -> {
                        log.info("Raft {} (replicator state changed) changeShard task {}, " +
                                 "status is {}", getGroupId(), task, status);
                    });
                }
            }
        }
    }

    class TaskHandler implements RaftTaskHandler {

        @Override
        public boolean invoke(final int groupId, byte[] request,
                              RaftClosure response) throws HgStoreException {
            try {
                CodedInputStream input = CodedInputStream.newInstance(request);
                byte methodId = input.readRawByte();
                switch (methodId) {
                    case RaftOperation.SYNC_PARTITION_TASK:
                        invoke(groupId, methodId, MetaTask.Task.parseFrom(input), response);
                        break;
                    case RaftOperation.SYNC_PARTITION:
                        invoke(groupId, methodId, Metapb.Partition.parseFrom(input), response);
                        break;
                    case RaftOperation.DO_SNAPSHOT:
                    case RaftOperation.BLANK_TASK:
                        invoke(groupId, methodId, null, response);
                        break;
                    case RaftOperation.IN_WRITE_OP:
                    case RaftOperation.RAFT_UPDATE_PARTITION:
                    case RaftOperation.IN_CLEAN_OP:
                    case RaftOperation.DB_COMPACTION:
                        invoke(groupId, methodId, RaftOperation.toObject(request, 0), response);
                        break;
                    default:
                        return false;
                }
            } catch (IOException e) {
                log.error("raft task exception ", e);
            }
            return true;
        }

        @Override
        public boolean invoke(final int groupId, byte methodId, Object req,
                              RaftClosure response) throws HgStoreException {
            switch (methodId) {
                case RaftOperation.SYNC_PARTITION_TASK: {
                    MetaTask.Task task = (MetaTask.Task) req;
                    taskManager.updateTask(task);
                    if (task.getType() == MetaTask.TaskType.Change_Shard) {
                        log.info("change shard task: id {}, change shard:{} ",
                                 task.getId(), task.getChangeShard());
                        doChangeShard(task, response);
                    }
                }
                break;
                case RaftOperation.SYNC_PARTITION:
                    log.info("receive sync partition {}", req);
                    if (!isLeader()) {
                        partitionManager.updatePartition((Metapb.Partition) req, true);
                    }
                    break;
                case RaftOperation.BLANK_TASK:
                    break;
                case RaftOperation.DO_SNAPSHOT:
                    doSnapshot(response);
                    break;
                case RaftOperation.IN_WRITE_OP:
                    storeEngine.getDataMover().doWriteData((BatchPutRequest) (req));
                    break;
                case RaftOperation.IN_CLEAN_OP:
                    handleCleanOp((CleanDataRequest) req);
                    break;
                case RaftOperation.RAFT_UPDATE_PARTITION:
                    log.info("Raft {}, receive raft updatePartitionRangeOrState {}",
                             getGroupId(), req);
                    partitionManager.updatePartitionRangeOrState((UpdatePartitionRequest) (req));
                    break;
                case RaftOperation.DB_COMPACTION:
                    DbCompactionRequest dbCompactionRequest = (DbCompactionRequest) (req);
                    storeEngine.getBusinessHandler()
                               .dbCompaction(dbCompactionRequest.getGraphName(),
                                             dbCompactionRequest.getPartitionId(),
                                             dbCompactionRequest.getTableName());
                    break;
                default:
                    return false;
            }
            return true;
        }
    }

}
