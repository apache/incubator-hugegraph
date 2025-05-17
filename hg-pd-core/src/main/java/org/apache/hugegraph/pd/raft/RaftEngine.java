package org.apache.hugegraph.pd.raft;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.common.ErrorType;
import org.apache.hugegraph.pd.grpc.common.Errors;
import org.apache.hugegraph.pd.raft.RaftRpcProcessor.GetMemberResponse;

import lombok.extern.slf4j.Slf4j;
import org.apache.hugegraph.pd.config.PDConfig.Raft;

@Slf4j
public class RaftEngine {
    private volatile static RaftEngine instance = new RaftEngine();

    public static RaftEngine getInstance() {
        return instance;
    }

    private String groupId = "pd_raft";
    private Raft config;
    private RaftStateMachine stateMachine;
    private RaftGroupService raftGroupService;
    private RpcServer rpcServer;
    private Node raftNode;
    private RaftRpcClient raftRpcClient;
    private static ConcurrentMap<String, String> grpcAddresses = new ConcurrentHashMap();

    public RaftEngine(){
        this.stateMachine = new RaftStateMachine();
    }

    public synchronized boolean init(Raft config) {
        if (this.raftNode != null) return false;
        this.config = config;

        raftRpcClient = new RaftRpcClient();
        raftRpcClient.init(new RpcOptions());

        String raftPath = config.getDataPath() + "/" + groupId;
        new File(raftPath).mkdirs();

        new File(config.getDataPath()).mkdirs();
        Configuration initConf = new Configuration();
        initConf.parse(config.getPeersList());
        if (config.isEnable() && config.getPeersList().length() < 3) {
            log.error(
                    "The RaftEngine parameter is incorrect." +
                    " When RAFT is enabled, the number of peers " +
                    "cannot be less than 3");
        }
        // 设置Node参数，包括日志存储路径和状态机实例
        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setFsm(stateMachine);
        nodeOptions.setEnableMetrics(true);
        // 日志路径
        nodeOptions.setLogUri(raftPath + "/log");
        // raft元数据路径
        nodeOptions.setRaftMetaUri(raftPath + "/meta");
        // 快照路径
        nodeOptions.setSnapshotUri(raftPath + "/snapshot");
        // 初始集群
        nodeOptions.setInitialConf(initConf);
        // 快照时间间隔
        nodeOptions.setSnapshotIntervalSecs(config.getSnapshotInterval());

        nodeOptions.setRpcConnectTimeoutMs(config.getRpcTimeout());
        nodeOptions.setRpcDefaultTimeout(config.getRpcTimeout());
        nodeOptions.setRpcInstallSnapshotTimeout(config.getRpcTimeout());
        // 设置raft配置
        RaftOptions raftOptions = nodeOptions.getRaftOptions();

        nodeOptions.setEnableMetrics(true);

        final PeerId serverId = JRaftUtils.getPeerId(config.getAddress());

        rpcServer = createRaftRpcServer(config.getAddress());
        // 构建raft组并启动raft
        this.raftGroupService = new RaftGroupService(groupId, serverId,
                nodeOptions, rpcServer, true);
        this.raftNode = raftGroupService.start(false);
        log.info("RaftEngine start successfully: id = {}, peers list = {}", groupId, nodeOptions.getInitialConf().getPeers());
        return this.raftNode != null;
    }

    /**
     * 创建raft rpc server，用于pd之间通讯
     */
    private RpcServer createRaftRpcServer(String raftAddr) {
        Endpoint endpoint = JRaftUtils.getEndPoint(raftAddr);
        RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(endpoint);
        RaftRpcProcessor.registerProcessor(rpcServer, this);
        rpcServer.init(null);
        return rpcServer;
    }

    public void shutDown() {
        if (this.raftGroupService != null) {
            this.raftGroupService.shutdown();
            try {
                this.raftGroupService.join();
            } catch (final InterruptedException e) {
                this.raftNode = null;
                ThrowUtil.throwException(e);
            }
            this.raftGroupService = null;
        }
        if (this.rpcServer != null){
            this.rpcServer.shutdown();
            this.rpcServer = null;
        }
        if (this.raftNode != null) {
            this.raftNode.shutdown();
        }
        this.raftNode = null;
    }

    public boolean isLeader() {
        return  this.raftNode.isLeader(true);
    }

    /**
     * 添加Raft任务，grpc通过该接口给raft发送数据
     */
    public void addTask(Task task) {
        if (!isLeader()) {
            KVStoreClosure closure = (KVStoreClosure) task.getDone();
            closure.setError(Errors.newBuilder().setType(ErrorType.NOT_LEADER).build());
            closure.run(new Status(RaftError.EPERM, "Not leader"));
            return;
        }
        this.raftNode.apply(task);
    }

    public void addStateListener(RaftStateListener listener){
        this.stateMachine.addStateListener(listener);
    }

    public void addTaskHandler(RaftTaskHandler handler){
        this.stateMachine.addTaskHandler(handler);
    }
    public Raft getConfig() {
        return this.config;
    }

    public PeerId getLeader(){
        return raftNode.getLeaderId();
    }

    /**
     * 向leader发消息，获取grpc地址；
     */
    public String getLeaderGrpcAddress() throws PDException {
        return getLeaderGrpcAddress(true);
    }

    /**
     * 获取leader grpc地址；
     */
    public String getLeaderGrpcAddress(boolean blocking) throws PDException {
        try{
            if (isLeader()) {
                return config.getGrpcAddress();
            }
            PeerId leaderId = raftNode.getLeaderId();
            if (leaderId == null) {
                if (blocking) {
                    leaderId = waitingForLeader(10000);
                    if (leaderId == null) {
                        return "";
                    }
                } else {
                    return "";
                }
            }
            String raftAddress = leaderId.getEndpoint().toString();
            String grpcAddress = grpcAddresses.get(raftAddress);
            if (!StringUtils.isEmpty(grpcAddress)) {
                return grpcAddress;
            }
            grpcAddress = raftRpcClient.getGrpcAddress(raftAddress).get().getGrpcAddress();
            grpcAddresses.put(raftAddress, grpcAddress);
            return grpcAddress;
        } catch (Exception e) {
            throw new PDException(ErrorType.ERROR, e);
        }
    }

    /**
     * 清空 gRPC 地址列表，用于极端情况下修改了Grpc地址而不重启的场景
     */
    public void clearGrpcAddresses() {
        grpcAddresses.clear();
    }

    /**
     * 获取本地成员信息
     *
     * @return 本地成员信息对象 {@link Metapb.Member} 的构建器
     */
    public Metapb.Member getLocalMember(){
        Metapb.Member.Builder builder = Metapb.Member.newBuilder();
        builder.setClusterId(config.getClusterId());
        builder.setRaftUrl(config.getAddress());
        builder.setDataPath(config.getDataPath());
        builder.setGrpcUrl(config.getGrpcAddress());
        builder.setRestUrl(config.getHost() + ":" + config.getPort());
        builder.setState(Metapb.StoreState.Up);
        return builder.build();
    }

    public List<Metapb.Member> getMembers() throws ExecutionException, InterruptedException {
        List<Metapb.Member> members = new ArrayList<>();

        List<PeerId> peers = raftNode.listPeers();
        peers.addAll(raftNode.listLearners());
        var learners = new HashSet<>(raftNode.listLearners());

        for(PeerId peerId : peers){
            Metapb.Member.Builder builder = Metapb.Member.newBuilder();
            builder.setClusterId(config.getClusterId());
            CompletableFuture<GetMemberResponse> future =
                    raftRpcClient.getGrpcAddress(peerId.getEndpoint().toString());

            Metapb.ShardRole role = Metapb.ShardRole.Follower;
            if (PeerUtil.isPeerEquals(peerId, raftNode.getLeaderId())) {
                role = Metapb.ShardRole.Leader;
            } else if (learners.contains(peerId)) {
                role = Metapb.ShardRole.Learner;
                var state = raftNode.getReplicatorState(peerId);
                if (state != null) {
                    builder.setReplicatorState(state.name());
                }
            }

            builder.setRole(role);

            try {
                if (future.isCompletedExceptionally()) {
                    log.error("failed to getGrpcAddress of {}",
                              peerId.getEndpoint().toString());
                    builder.setState(Metapb.StoreState.Offline);
                    builder.setRaftUrl(peerId.getEndpoint().toString());
                    members.add(builder.build());
                } else {
                    GetMemberResponse response = future.get();
                    builder.setState(Metapb.StoreState.Up);
                    builder.setRaftUrl(response.getRaftAddress());
                    builder.setDataPath(response.getDatePath());
                    builder.setGrpcUrl(response.getGrpcAddress());
                    builder.setRestUrl(response.getRestAddress());
                    members.add(builder.build());
                }
            } catch (Exception e) {
                log.error("failed to getGrpcAddress of {}. {}",
                          peerId.getEndpoint().toString(), e);
                builder.setState(Metapb.StoreState.Offline);
                builder.setRaftUrl(peerId.getEndpoint().toString());
                members.add(builder.build());
            }

        }
        return members;
    }

    public Status changePeerList(String peerList) {
        AtomicReference<Status> result = new AtomicReference<>();
        try {
            String[] peers = peerList.split(",", -1);
            if ((peers.length & 1) != 1) {
                throw new PDException(-1, "the number of peer list must be odd.");
            }
            Configuration newPeers = new Configuration();
            newPeers.parse(peerList);
            CountDownLatch latch = new CountDownLatch(1);
            this.raftNode.changePeers(newPeers, status -> {
                result.set(status);
                latch.countDown();
            });
            latch.await();
        } catch (Exception e) {
            log.error("failed to changePeerList to {},{}", peerList, e);
            result.set(new Status(-1, e.getMessage()));
        }
        return result.get();
    }

    public PeerId waitingForLeader(long timeOut){
        PeerId leader = getLeader();
        if ( leader != null ) {
            return leader;
        }

        synchronized (this) {
            leader = getLeader();
            long start = System.currentTimeMillis();
            while ((System.currentTimeMillis() - start < timeOut) && (leader == null)) {
                try {
                    this.wait(1000);
                } catch (InterruptedException e) {
                    log.error("Raft wait for leader exception", e);
                }
                leader = getLeader();
            }
            return leader != null ? leader : null;
        }

    }

    public Node getRaftNode() {
        return raftNode;
    }

    public List<String> getPeerGrpcAddresses() throws PDException {
        try {
            List<PeerId> peers = raftNode.listPeers();
            peers.addAll(raftNode.listLearners());
            ArrayList<String> addresses = new ArrayList<>(peers.size());
            for (PeerId id : peers) {
                CompletableFuture<GetMemberResponse> future =
                        raftRpcClient.getGrpcAddress(id.getEndpoint().toString());
                try {
                    String grpcAddress = future.get().getGrpcAddress();
                    if (!StringUtils.isEmpty(grpcAddress)) {
                        addresses.add(grpcAddress);
                    }
                } catch (Exception e) {
                    log.warn("get grpc address of peer: {} with error:", id, e);
                }
            }
            return addresses;
        } catch (Exception e) {
            throw new PDException(ErrorType.ERROR, e);
        }
    }

    public List<String> getPeerGrpcAddressesByCache() throws PDException {
        try {
            List<PeerId> peers = raftNode.listPeers();
            peers.addAll(raftNode.listLearners());
            ArrayList<String> addresses = new ArrayList<>(peers.size());
            String grpcAddress;
            for (PeerId id : peers) {
                String raftAddress = id.getEndpoint().toString();
                grpcAddress = grpcAddresses.get(raftAddress);
                if (grpcAddress != null) {
                    addresses.add(grpcAddress);
                } else {
                    CompletableFuture<GetMemberResponse> future = raftRpcClient.getGrpcAddress(raftAddress);
                    try {
                        grpcAddress = future.get().getGrpcAddress();
                        if (!StringUtils.isEmpty(grpcAddress)) {
                            grpcAddresses.put(raftAddress, grpcAddress);
                            addresses.add(grpcAddress);
                        }
                    } catch (Exception e) {
                        log.warn("get grpc address of peer: {} with error:", id, e);
                    }
                }
            }
            return addresses;
        } catch (Exception e) {
            throw new PDException(ErrorType.ERROR, e);
        }
    }
}
