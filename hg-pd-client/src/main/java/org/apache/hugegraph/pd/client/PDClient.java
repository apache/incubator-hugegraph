package org.apache.hugegraph.pd.client;

import static org.apache.hugegraph.pd.common.Consts.DEFAULT_STORE_GROUP_ID;

import java.util.List;

import org.apache.hugegraph.pd.client.impl.PDApi;
import org.apache.hugegraph.pd.client.listener.PDEventListener;
import org.apache.hugegraph.pd.client.rpc.ConnectionManager;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.grpc.ClusterOp;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Metapb.Partition;
import org.apache.hugegraph.pd.grpc.Metapb.Shard;
import org.apache.hugegraph.pd.grpc.Metapb.ShardGroup;
import org.apache.hugegraph.pd.grpc.PDGrpc;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.Pdpb.CachePartitionResponse;
import org.apache.hugegraph.pd.grpc.Pdpb.CacheResponse;
import org.apache.hugegraph.pd.grpc.Pdpb.GraphStatsResponse;
import org.apache.hugegraph.pd.pulse.Pulse;
import org.apache.hugegraph.pd.watch.PDEventRaiser;
import org.apache.hugegraph.pd.watch.Watcher;

import io.grpc.ManagedChannel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * PD客户端实现类
 *
 * @author yanjinbing
 */
@Slf4j
public class PDClient extends BaseClient {

    private final PDConfig config;
    @Getter
    private final ClientCache cache;
    private final PulseClient pulse;
    private final PDEventRaiser events;
    @Getter
    private final Watcher pdWatch;
    private final PDApi pdApi;
    private final ConnectionManager cm;

    PDClient(PDConfig config) {
        super(config, PDGrpc::newStub, PDGrpc::newBlockingStub);
        this.config = config;
        this.cm = getCm();
        this.pulse = this.cm.getPulseClient();
        this.pdWatch = this.cm.getWatcher();
        this.cache = this.cm.getCache();
        this.pdApi = new PDApi(this, this.cache);
        this.cache.setPdApi(this.pdApi);
        this.events = new PDEventRaiser(this.pdWatch);
    }

    /**
     * 创建PDClient对象，并初始化stub
     *
     * @param config
     * @return
     */
    public static PDClient create(PDConfig config) {
        PDClient client = new PDClient(config);
        return client;
    }


    @Deprecated
    public static void setChannel(ManagedChannel mc) {
        log.warn("[PDClient] Invoking a deprecated method [ PDClient::setChannel ].");
    }

    /**
     * Return the local PD config.
     *
     * @return
     */
    public PDConfig getClientConfig() {
        return this.config;
    }


    /**
     * Return the PD pulse client.
     *
     * @return
     */
    public Pulse getPulse() {
        return this.pulse;
    }

    public Pulse getPulse(long storeId) {
        this.pulse.setObserverId(storeId);
        return this.pulse;
    }

    /**
     * Force a reconnection to the PD leader, regardless of whether the current connection is alive or not.
     */
    public void forceReconnect() {
        getCm().reconnect();
    }

    /**
     * Begin watching with the leader address.
     *
     * @param leader
     */
    @Deprecated
    public void startWatch(String leader) {
        log.warn("[PDClient] Invoking a deprecated method [ PDClient::startWatch ],");
    }

    public String getLeaderIp() {
        return getCm().getLeader();
    }

    /**
     * Store注册，返回storeID，初次注册会返回新ID
     *
     * @param store
     * @return
     */
    public long registerStore(Metapb.Store store) throws PDException {
        return this.pdApi.registerStore(store);
    }

    /**
     * 根据storeId返回Store对象
     *
     * @param storeId
     * @return
     * @throws PDException
     */
    public Metapb.Store getStore(long storeId) throws PDException {
        return this.pdApi.getStore(storeId);
    }

    /**
     * 更新Store信息，包括上下线等
     *
     * @param store
     * @return
     */
    public Metapb.Store updateStore(Metapb.Store store) throws PDException {
        return this.pdApi.updateStore(store);
    }

    /**
     * 返回活跃的Store
     *
     * @param graphName
     * @return
     */
    public List<Metapb.Store> getActiveStores(String graphName) throws PDException {
        return this.pdApi.getActiveStores(graphName);
    }

    public List<Metapb.Store> getActiveStores() throws PDException {
        return this.pdApi.getActiveStores();
    }

    /**
     * 返回活跃的Store
     *
     * @param graphName
     * @return
     */
    public List<Metapb.Store> getAllStores(String graphName) throws PDException {
        return this.pdApi.getAllStores(graphName);
    }

    /**
     * Store心跳，定期调用，保持在线状态
     *
     * @param stats
     * @throws PDException
     */
    public Metapb.ClusterStats storeHeartbeat(Metapb.StoreStats stats) throws PDException {
        return this.pdApi.storeHeartbeat(stats);
    }

    /**
     * 查询Key所属分区信息
     *
     * @param graphName
     * @param key
     * @return
     * @throws PDException
     */
    public KVPair<Partition, Shard> getPartition(String graphName, byte[] key) throws PDException {
        return this.pdApi.getPartition(graphName, key);
    }

    public KVPair<Partition, Shard> getPartition(String graphName, byte[] key, int code) throws PDException {
        return this.pdApi.getPartition(graphName, key, code);
    }

    /**
     * 根据hashcode查询所属分区信息
     *
     * @param graphName
     * @param hashCode
     * @return
     * @throws PDException
     */
    public KVPair<Partition, Shard> getPartitionByCode(String graphName, long hashCode)
            throws PDException {
        return this.pdApi.getPartitionByCode(graphName, hashCode);
    }

    /**
     * 获取Key的哈希值
     */
    public int keyToCode(String graphName, byte[] key) {
        return PartitionUtils.calcHashcode(key);
    }

    /**
     * 根据分区id返回分区信息, RPC请求
     *
     * @param graphName
     * @param partId
     * @return
     * @throws PDException
     */
    public KVPair<Partition, Shard> getPartitionById(String graphName, int partId) throws PDException {
        return this.pdApi.getPartitionById(graphName, partId);
    }

    public ShardGroup getShardGroup(int partId) throws PDException {
        return this.pdApi.getShardGroup(partId);
    }

    public ShardGroup getShardGroupDirect(int partId) throws PDException {
        return this.pdApi.getShardGroupDirect(partId);
    }

    public void updateShardGroup(ShardGroup shardGroup) throws PDException {
        this.pdApi.updateShardGroup(shardGroup);
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
    public List<KVPair<Partition, Shard>> scanPartitions(String graphName, byte[] startKey,
                                                         byte[] endKey) throws PDException {
        return this.pdApi.scanPartitions(graphName, startKey, endKey);
    }

    /**
     * 根据条件查询分区信息
     *
     * @return
     * @throws PDException
     */
    public List<Partition> getPartitionsByStore(long storeId) throws PDException {

        return this.pdApi.getPartitionsByStore(storeId);
    }

    /**
     * 查找指定store上的指定partitionId
     *
     * @return
     * @throws PDException
     */
    public List<Partition> queryPartitions(long storeId, int partitionId) throws PDException {
        return this.pdApi.queryPartitions(storeId, partitionId);
    }

    public List<Partition> getPartitions(long storeId, String graphName) throws PDException {

        return this.pdApi.getPartitions(storeId, graphName);

    }

    /**
     * create a graph, requires the graph wouldn't exist before
     *
     * @param graph graph
     * @return graph that created
     * @throws PDException error occurs
     */
    public Metapb.Graph createGraph(Metapb.Graph graph) throws PDException {
        return this.pdApi.createGraph(graph);
    }

    /**
     * update graph, update graph name if exists, otherwise create a new graph
     *
     * @param graph the new graph
     * @return graph that updated
     * @throws PDException error occurs
     */
    public Metapb.Graph setGraph(Metapb.Graph graph) throws PDException {
        return this.pdApi.setGraph(graph);
    }

    public Metapb.Graph getGraph(String graphName) throws PDException {
        return this.pdApi.getGraph(graphName);
    }

    public Metapb.Graph getGraphWithOutException(String graphName) throws
                                                                   PDException {
        return this.pdApi.getGraphWithOutException(graphName);
    }

    public Metapb.Graph delGraph(String graphName) throws PDException {
        return this.pdApi.delGraph(graphName);
    }

    public List<Partition> updatePartition(List<Partition> partitions) throws PDException {
        return this.pdApi.updatePartition(partitions);

    }

    public Partition delPartition(String graphName, int partitionId) throws PDException {
        return this.pdApi.delPartition(graphName, partitionId);
    }

    /**
     * 删除分区缓存
     */
    public void invalidPartitionCache(String graphName, int partitionId) {
        this.pdApi.invalidPartitionCache(graphName, partitionId);
    }

    /**
     * 删除分区缓存
     */
    public void invalidPartitionCache() {
        // 检查是否存在缓存
        cache.removePartitions();
    }

    /**
     * 删除分区缓存
     */
    public void invalidStoreCache(long storeId) {
        cache.removeStore(storeId);
    }

    /**
     * Hugegraph server 调用，Leader发生改变，更新缓存
     */
    public void updatePartitionLeader(String graphName, int partId, long leaderStoreId) {
        this.pdApi.updatePartitionLeader(graphName, partId, leaderStoreId);
    }

    /**
     * Hugegraph-store调用，更新缓存
     *
     * @param partition
     */
    public void updatePartitionCache(Partition partition, Shard leader) {
        this.pdApi.updatePartitionCache(partition, leader);
    }

    public Pdpb.GetIdResponse getIdByKey(String key, int delta) throws PDException {
        return this.pdApi.getIdByKey(key, delta);
    }

    public Pdpb.ResetIdResponse resetIdByKey(String key) throws PDException {
        return this.pdApi.resetIdByKey(key);
    }

    public Metapb.Member getLeader() throws PDException {
        return this.pdApi.getLeader();
    }

    public Pdpb.GetMembersResponse getMembers() throws PDException {
        return this.pdApi.getMembers();
    }

    public Metapb.ClusterStats getClusterStats() throws PDException {
        return this.pdApi.getClusterStats(DEFAULT_STORE_GROUP_ID);
    }

    public Metapb.ClusterStats getClusterStats(long storeId) throws PDException {
        return this.pdApi.getClusterStats(storeId);
    }

    public Metapb.ClusterStats getClusterStats(int storeGroupId) throws PDException {
        return this.pdApi.getClusterStats(storeGroupId);
    }

    public void addEventListener(PDEventListener listener) {
        this.events.addListener(listener);
    }

    public Watcher getWatchClient() {
        return this.pdWatch;
    }

    /**
     * 返回Store状态信息
     */
    public List<Metapb.Store> getStoreStatus(boolean offlineExcluded) throws PDException {
        return this.pdApi.getStoreStatus(offlineExcluded);
    }

    public void setGraphSpace(String graphSpaceName, long storageLimit) throws PDException {
        this.pdApi.setGraphSpace(graphSpaceName, storageLimit);
    }

    public List<Metapb.GraphSpace> getGraphSpace(String graphSpaceName) throws
                                                                        PDException {
        return this.pdApi.getGraphSpace(graphSpaceName);
    }

    @Deprecated
    public void setPDConfig(int partitionCount, String peerList, int shardCount, long version) throws
                                                                                               PDException {
        this.pdApi.setPDConfig(partitionCount, peerList, shardCount, version);
    }

    public void setPDConfig(String peerList, int shardCount, long version) throws PDException {
        this.pdApi.setPDConfig(0, peerList, shardCount, version);
    }

    public void setPDConfig(Metapb.PDConfig pdConfig) throws PDException {
        this.pdApi.setPDConfig(pdConfig);
    }

    public Metapb.PDConfig getPDConfig() throws PDException {
        return this.pdApi.getPDConfig();
    }

    public Metapb.PDConfig getPDConfig(long version) throws PDException {
        return this.pdApi.getPDConfig(version);
    }

    public void changePeerList(String peerList) throws PDException {
        this.pdApi.changePeerList(peerList);
    }

    /**
     * 工作模式
     * Auto：自动分裂，每个Store上分区数达到最大值, 需要指定store group id. store group id 为0， 针对默认分区
     * 建议使用 splitData(ClusterOp.OperationMode mode, int storeGroupId, List<ClusterOp.SplitDataParam> params)
     * mode = Auto 指定 storeGroupId, params 为空
     *
     * @throws PDException
     */
    @Deprecated
    public void splitData() throws PDException {
        this.pdApi.splitData(ClusterOp.OperationMode.Auto, 0, List.of());
    }

    /**
     * 工作模式
     * Auto：自动分裂，每个Store上分区数达到最大值, 需要指定store group id
     * Expert:专家模式，需要指定splitParams, 限制 SplitDataParam 在同一个store group中
     *
     * @param mode
     * @param params
     * @throws PDException
     */
    public void splitData(ClusterOp.OperationMode mode, int storeGroupId,
                          List<ClusterOp.SplitDataParam> params)
            throws PDException {
        this.pdApi.splitData(mode, storeGroupId, params);
    }

    /**
     * 针对单个graph的分裂，会扩充partition，造成整体分区数的不一致.
     * 建议：针对整个store group做分裂. 大小图可以根据分组放到不同的分区中
     *
     * @param graphName
     * @param toCount
     * @throws PDException
     */
    @Deprecated
    public void splitGraphData(String graphName, int toCount) throws PDException {
        this.pdApi.splitGraphData(graphName, toCount);
    }

    /**
     * 自动转移，达到每个Store上分区数量相同, 建议使用 balancePartition(int storeGroupId), 指定 storeGroupId
     *
     * @throws PDException
     */
    @Deprecated
    public void balancePartition() throws PDException {
        this.pdApi.balancePartition(ClusterOp.OperationMode.Auto, DEFAULT_STORE_GROUP_ID, List.of());
    }

    public void balancePartition(int storeGroupId) throws PDException {
        this.pdApi.balancePartition(ClusterOp.OperationMode.Auto, storeGroupId, List.of());
    }

    /**
     * 迁移分区 手动模式
     * //工作模式
     * //  Auto：自动转移，达到每个Store上分区数量相同
     * //  Expert:专家模式，需要指定transferParams
     *
     * @param params 指定transferParams, expert 模式， 要求 source store / target store在同一个store group
     * @throws PDException
     */
    public void movePartition(ClusterOp.OperationMode mode, List<ClusterOp.MovePartitionParam> params) throws
                                                                                                       PDException {
        this.pdApi.balancePartition(ClusterOp.OperationMode.Expert, DEFAULT_STORE_GROUP_ID, params);
    }

    public void reportTask(MetaTask.Task task) throws PDException {
        this.pdApi.reportTask(task);
    }

    public Metapb.PartitionStats getPartitionsStats(String graph, int partId) throws PDException {
        return this.pdApi.getPartitionsStats(graph, partId);
    }

    /**
     * 平衡不同store中leader的数量
     */
    public void balanceLeaders() throws PDException {
        this.pdApi.balanceLeaders();
    }

    /**
     * 从pd中删除store
     */
    public Metapb.Store delStore(long storeId) throws PDException {
        return this.pdApi.delStore(storeId);
    }

    /**
     * 对rocksdb整体进行compaction
     *
     * @throws PDException
     */
    public void dbCompaction() throws PDException {
        this.pdApi.dbCompaction();
    }

    /**
     * 对rocksdb指定表进行compaction
     *
     * @param tableName
     * @throws PDException
     */
    public void dbCompaction(String tableName) throws PDException {
        this.pdApi.dbCompaction(tableName);
    }

    /**
     * 分区合并，把当前的分区缩容至toCount个
     *
     * @param toCount 缩容到分区的个数
     * @throws PDException
     */
    @Deprecated
    public void combineCluster(int toCount) throws PDException {
        this.pdApi.combineCluster(DEFAULT_STORE_GROUP_ID, toCount);
    }

    public void combineCluster(int shardGroupId, int toCount) throws PDException {
        this.pdApi.combineCluster(shardGroupId, toCount);
    }

    /**
     * 将单图缩容到 toCount个, 与分裂类似，要保证同个store group中的分区数量一样。
     * 如果有特殊需求，可以考虑迁移到其他的分组中
     *
     * @param graphName graph name
     * @param toCount   target count
     * @throws PDException
     */
    @Deprecated
    public void combineGraph(String graphName, int toCount) throws PDException {
        this.pdApi.combineGraph(graphName, toCount);
    }

    public void deleteShardGroup(int groupId) throws PDException {
        this.pdApi.deleteShardGroup(groupId);
    }

    /**
     * 用于 store的 shard list重建
     *
     * @param groupId shard group id
     * @param shards  shard list，delete when shards size is 0
     */
    public void updateShardGroupOp(int groupId, List<Shard> shards) throws PDException {
        this.pdApi.updateShardGroupOp(groupId, shards);
    }

    /**
     * invoke fireChangeShard command
     *
     * @param groupId shard group id
     * @param shards  shard list
     */
    public void changeShard(int groupId, List<Shard> shards) throws PDException {
        this.pdApi.changeShard(groupId, shards);
    }

    public CacheResponse getClientCache() throws PDException {
        return this.pdApi.getClientCache();
    }

    public CachePartitionResponse getPartitionCache(String graph) throws PDException {
        return this.pdApi.getPartitionCache(graph);
    }

    public void updatePdRaft(String raftConfig) throws PDException {
        this.pdApi.updatePdRaft(raftConfig);
    }

    public long submitBuildIndexTask(Metapb.BuildIndexParam param) throws PDException {
        return this.pdApi.submitBuildIndexTask(param);
    }

    public long submitBackupGraphTask(String sourceGraph, String targetGraph) throws PDException {
        return this.pdApi.submitBackupGraphTask(sourceGraph, targetGraph);
    }

    @Deprecated
    public Pdpb.TaskQueryResponse queryBuildIndexTaskStatus(long taskId) throws PDException {
        return this.queryTaskStatus(taskId);
    }

    public Pdpb.TaskQueryResponse queryTaskStatus(long taskId) throws PDException {
        return this.pdApi.queryBuildIndexTaskStatus(taskId);
    }

    @Deprecated
    public Pdpb.TaskQueryResponse retryBuildIndexTask(long taskId) throws PDException {
        return retryTask(taskId);
    }

    public Pdpb.TaskQueryResponse retryTask(long taskId) throws PDException {
        return this.pdApi.retryTask(taskId);
    }

    public GraphStatsResponse getGraphStats(String graphName) throws PDException {
        return this.pdApi.getGraphStats(graphName);
    }

    public Metapb.StoreGroup createStoreGroup(int groupId, String name, int partitionCount) throws
                                                                                            PDException {
        return this.pdApi.createStoreGroup(groupId, name, partitionCount);
    }

    public Metapb.StoreGroup getStoreGroup(int groupId) throws PDException {
        return this.pdApi.getStoreGroup(groupId);
    }

    public List<Metapb.StoreGroup> getAllStoreGroups() throws PDException {
        return this.pdApi.getAllStoreGroups();
    }

    public Metapb.StoreGroup updateStoreGroup(int groupId, String name) throws PDException {
        return this.pdApi.updateStoreGroup(groupId, name);
    }

    public List<Metapb.Store> getStoresByStoreGroup(int groupId) throws PDException {
        return this.pdApi.getStoresByStoreGroup(groupId);
    }

    public boolean updateStoreGroupRelation(long storeId, int groupId) throws PDException {
        return this.pdApi.updateStoreGroupRelation(storeId, groupId);
    }

    public void onLeaderChanged(String leader) {
    }

    public void close() {
        super.close();
    }
}
