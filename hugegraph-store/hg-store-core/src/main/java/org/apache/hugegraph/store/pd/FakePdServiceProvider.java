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

package org.apache.hugegraph.store.pd;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.store.meta.GraphManager;
import org.apache.hugegraph.store.meta.Partition;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;

import lombok.extern.slf4j.Slf4j;

/**
 * 内置PD服务，用于单机部署或开发调试
 */
@Slf4j
public class FakePdServiceProvider implements PdProvider {

    private final Map<Long, Store> stores;
    private final int shardCount = 0;
    private final Map<String, Metapb.Partition> partitions = new ConcurrentHashMap<>();
    private int partitionCount = 0;
    private GraphManager graphManager = null;

    public FakePdServiceProvider(HgStoreEngineOptions.FakePdOptions options) {
        stores = new LinkedHashMap<>();
        if (options != null) {
            String[] storeList = options.getStoreList().split(",");
            String[] peersList = options.getPeersList().split(",");
            for (int i = 0; i < storeList.length; i++) {
                if (!storeList[i].isEmpty()) {
                    addStore(storeList[i], peersList[i]);
                }
            }
        }
        this.partitionCount = options.getPartitionCount();
    }

    public static long makeStoreId(String storeAddress) {
        return storeAddress.hashCode();
    }

    /**
     * For unit test
     *
     * @return
     */
    public static Store getDefaultStore() {
        Store store = new Store();
        store.setId(1);
        store.setStoreAddress("127.0.0.1:8501");
        store.setRaftAddress("127.0.0.1:8511");
        store.setPartitionCount(1);
        return store;
    }

    private void addStore(String storeAddr, String raftAddr) {
        Store store = new Store() {{
            setId(makeStoreId(storeAddr));
            setRaftAddress(raftAddr);
            setStoreAddress(storeAddr);
        }};
        stores.put(store.getId(), store);
    }

    public void addStore(Store store) {
        stores.put(store.getId(), store);
    }

    @Override
    public long registerStore(Store store) throws PDException {
        log.info("registerStore storeId:{}, storeAddress:{}", store.getId(),
                 store.getStoreAddress());

        // id 不匹配，禁止登录
        if (store.getId() != 0 && store.getId() != makeStoreId(store.getStoreAddress())) {
            throw new PDException(Pdpb.ErrorType.STORE_ID_NOT_EXIST_VALUE,
                                  "Store id does not matched");
        }

        if (!stores.containsKey(makeStoreId(store.getStoreAddress()))) {
            store.setId(makeStoreId(store.getStoreAddress()));
            stores.put(store.getId(), store);
        }
        Store s = stores.get(makeStoreId(store.getStoreAddress()));
        store.setId(s.getId());

        return store.getId();
    }

    @Override
    public Partition getPartitionByID(String graph, int partId) {
        List<Store> storeList = new ArrayList(stores.values());
        int shardCount = this.shardCount;
        if (shardCount == 0 || shardCount >= stores.size()) {
            shardCount = stores.size();
        }

        int storeIdx = partId % storeList.size();
        List<Metapb.Shard> shards = new ArrayList<>();
        for (int i = 0; i < shardCount; i++) {
            Metapb.Shard shard =
                    Metapb.Shard.newBuilder().setStoreId(storeList.get(storeIdx).getId())
                                .setRole(i == 0 ? Metapb.ShardRole.Leader :
                                         Metapb.ShardRole.Follower) //
                                .build();
            shards.add(shard);
            storeIdx = (storeIdx + 1) >= storeList.size() ? 0 : ++storeIdx; // 顺序选择
        }

        int partLength = getPartitionLength();
        Metapb.Partition partition = Metapb.Partition.newBuilder()
                                                     .setGraphName(graph)
                                                     .setId(partId)
                                                     .setStartKey(partLength * partId)
                                                     .setEndKey(partLength * (partId + 1))
                                                     //.addAllShards(shards)
                                                     .build();
        return new Partition(partition);
    }

    @Override
    public Metapb.Shard getPartitionLeader(String graph, int partId) {
        return null;
    }

    private int getPartitionLength() {
        return PartitionUtils.MAX_VALUE / (partitionCount == 0 ? stores.size() : partitionCount) +
               1;
    }

    @Override
    public Metapb.Partition getPartitionByCode(String graph, int code) {
        int partId = code / getPartitionLength();
        return getPartitionByID(graph, partId).getProtoObj();
    }

    @Override
    public Partition delPartition(String graph, int partId) {
        return null;
    }

    @Override
    public List<Metapb.Partition> updatePartition(List<Metapb.Partition> partitions) {
        return partitions;
    }

    @Override
    public List<Partition> getPartitionsByStore(long storeId) throws PDException {
        return new ArrayList<>();
    }

    @Override
    public void updatePartitionCache(Partition partition, Boolean changeLeader) {

    }

    @Override
    public void invalidPartitionCache(String graph, int partId) {

    }

    @Override
    public boolean startHeartbeatStream(Consumer<Throwable> onError) {
        return false;
    }

    @Override
    public boolean addPartitionInstructionListener(PartitionInstructionListener listener) {
        return false;
    }

    @Override
    public boolean partitionHeartbeat(List<Metapb.PartitionStats> statsList) {
        return true;
    }

    @Override
    public boolean isLocalPartition(long storeId, int partitionId) {
        return true;
    }

    @Override
    public Metapb.Graph getGraph(String graphName) {
        return Metapb.Graph.newBuilder().setGraphName(graphName)
                           //.setId(PartitionUtils.calcHashcode(graphName.getBytes()))
                           .build();
    }

    @Override
    public void reportTask(MetaTask.Task task) throws PDException {

    }

    @Override
    public PDClient getPDClient() {
        return null;
    }

    @Override
    public Store getStoreByID(Long storeId) {
        return stores.get(storeId);
    }

    @Override
    public Metapb.ClusterStats getClusterStats() {
        return Metapb.ClusterStats.newBuilder()
                                  .setState(Metapb.ClusterState.Cluster_OK).build();
    }

    @Override
    public Metapb.ClusterStats storeHeartbeat(Store node) {

        return getClusterStats();
    }

    @Override
    public boolean updatePartitionLeader(String graphName, int partId, long leaderStoreId) {
        return false;

    }

    @Override
    public GraphManager getGraphManager() {
        return graphManager;
    }

    @Override
    public void setGraphManager(GraphManager graphManager) {
        this.graphManager = graphManager;
    }

    @Override
    public void deleteShardGroup(int groupId) {

    }
}
