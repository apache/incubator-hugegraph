<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDRestService.java
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
========
package org.apache.hugegraph.pd.service;

import static org.apache.hugegraph.pd.common.Consts.DEFAULT_STORE_GROUP_ID;
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDRestService.java

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.LogService;
import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.StoreMonitorDataService;
import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.TaskScheduleService;
import org.apache.hugegraph.pd.boot.ShutdownHook;
import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.common.ErrorType;
import org.apache.hugegraph.pd.grpc.common.Errors;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.grpc.discovery.RegisterInfo;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.model.RegistryRestRequest;
import org.apache.hugegraph.pd.model.RegistryRestResponse;
import org.apache.hugegraph.pd.raft.RaftEngine;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.LogService;
import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.StoreMonitorDataService;
import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.TaskScheduleService;
import org.apache.hugegraph.pd.common.HgAssert;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.grpc.discovery.RegisterInfo;
import org.apache.hugegraph.pd.model.RegistryRestRequest;
import org.apache.hugegraph.pd.model.RegistryRestResponse;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class PDRestService implements InitializingBean {

    private static final String EMPTY_STRING = "";
    @Autowired
    PDService pdService;
    @Autowired
    DiscoveryService discoveryService;
    private StoreNodeService storeNodeService;
    private PartitionService partitionService;
    private TaskScheduleService monitorService;
    private ConfigService configService;
    private LogService logService;
    private StoreMonitorDataService storeMonitorDataService;
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDRestService.java
========
    private static final int WAIT_TIMEOUT = 45;
    @Autowired
    PDService pdService;
    @Autowired
    DiscoveryService discoveryService;
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/service/PDRestService.java

    /**
     * initialize
     *
     * @throws Exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        storeNodeService = pdService.getStoreNodeService();
        partitionService = pdService.getPartitionService();
        monitorService = pdService.getTaskService();
        configService = pdService.getConfigService();
        logService = pdService.getLogService();
        storeMonitorDataService = pdService.getStoreMonitorDataService();
        HgAssert.isNotNull(storeNodeService, "storeNodeService does not initialize");
        HgAssert.isNotNull(partitionService, "partitionService does not initialize");
    }

    public List<Metapb.Store> getStores(String graphName) throws PDException {
        return storeNodeService.getStores(graphName);
    }

    public Metapb.Store getStore(long storeId) throws PDException {
        return storeNodeService.getStore(storeId);
    }

    public List<Metapb.ShardGroup> getShardGroups() throws PDException {
        return storeNodeService.getShardGroups();
    }

    public Metapb.Store updateStore(Metapb.Store store) throws PDException {
        logService.insertLog(LogService.NODE_CHANGE, LogService.REST, store);
        return storeNodeService.updateStore(store);
    }

    public boolean removeStore(Long storeId) throws PDException {
        if (storeId == null) {
            return false;
        }
        return 0 != storeNodeService.removeStore(storeId);
    }

    public Metapb.GraphSpace setGraphSpace(Metapb.GraphSpace graphSpace) throws PDException {
        return configService.setGraphSpace(graphSpace);
    }

    public List<Metapb.GraphSpace> getGraphSpaces() throws PDException {
        return configService.getGraphSpace(EMPTY_STRING);
    }

    public Metapb.GraphSpace getGraphSpace(String graphSpaceName) throws PDException {
        return configService.getGraphSpace(graphSpaceName).get(0);
    }

    public List<Metapb.Graph> getGraphs() throws PDException {
        return partitionService.getGraphs();
    }

    public Metapb.Graph getGraph(String graphName) throws PDException {
        return partitionService.getGraph(graphName);
    }

    public Metapb.Graph updateGraph(Metapb.Graph graph) throws PDException {
        return partitionService.updateGraphName(graph);
    }

    public List<Metapb.Partition> getPartitions(String graphName) {
        return partitionService.getPartitions(graphName);
    }

    public Map<Integer, Metapb.ShardGroup> getShardGroupCache() {
        return partitionService.getShardGroupCache();
    }

    public List<Metapb.Store> patrolStores() throws PDException {
        return monitorService.patrolStores();
    }

    public List<Metapb.Partition> patrolPartitions() throws PDException {
        return monitorService.patrolPartitions();
    }

    public Metapb.PartitionStats getPartitionStats(String graphName, int partitionId) throws
                                                                                      PDException {
        return partitionService.getPartitionStats(graphName, partitionId);
    }

    public List<Metapb.PartitionStats> getPartitionStatus(String graphName) throws PDException {
        return partitionService.getPartitionStatus(graphName);
    }

    public Map<Integer, KVPair<Long, Long>> balancePartitions() throws PDException {
        return monitorService.balancePartitionShard(DEFAULT_STORE_GROUP_ID);
    }

    public List<Metapb.Partition> splitPartitions() throws PDException {
        return monitorService.autoSplitPartition(DEFAULT_STORE_GROUP_ID);
    }

    public List<Metapb.Store> getStoreStats(boolean isActive) throws PDException {
        return storeNodeService.getStoreStatus(isActive);
    }

    public List<Map<String, Long>> getMonitorData(long storeId) throws PDException {
        return storeMonitorDataService.getStoreMonitorData(storeId);
    }

    public String getMonitorDataText(long storeId) throws PDException {
        return storeMonitorDataService.getStoreMonitorDataText(storeId);
    }

    public RegistryRestResponse register(NodeInfo nodeInfo) throws PDException {
        CountDownLatch latch = new CountDownLatch(1);
        final RegisterInfo[] info = {null};
        RegistryRestResponse response = new RegistryRestResponse();
        try {
            StreamObserver<RegisterInfo> observer = new StreamObserver<RegisterInfo>() {
                @Override
                public void onNext(RegisterInfo value) {
                    info[0] = value;
                    latch.countDown();
                }

                @Override
                public void onError(Throwable t) {
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            };
            this.discoveryService.register(nodeInfo, observer);
            latch.await(WAIT_TIMEOUT, TimeUnit.SECONDS);
            Errors error = info[0].getHeader().getError();
            response.setErrorType(error.getType());
            response.setMessage(error.getMessage());
        } catch (Exception e) {
            response.setErrorType(ErrorType.UNRECOGNIZED);
            response.setMessage(e.getMessage());
        }
        return response;
    }

    public ArrayList<RegistryRestRequest> getNodeInfo(Query request) throws PDException {
        CountDownLatch latch = new CountDownLatch(1);
        final NodeInfos[] info = {null};
        RegistryRestResponse response = new RegistryRestResponse();
        ArrayList<RegistryRestRequest> registryRestRequests = null;
        try {
            StreamObserver<NodeInfos> observer = new StreamObserver<NodeInfos>() {
                @Override
                public void onNext(NodeInfos value) {
                    info[0] = value;
                    latch.countDown();
                }

                @Override
                public void onError(Throwable t) {
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            };
            this.discoveryService.getNodes(request, observer);
            latch.await(WAIT_TIMEOUT, TimeUnit.SECONDS);
            List<NodeInfo> infoList = info[0].getInfoList();
            registryRestRequests = new ArrayList(infoList.size());
            for (int i = 0; i < infoList.size(); i++) {
                NodeInfo element = infoList.get(i);
                RegistryRestRequest registryRestRequest = new RegistryRestRequest();
                registryRestRequest.setAddress(element.getAddress());
                registryRestRequest.setAppName(element.getAppName());
                registryRestRequest.setVersion(element.getVersion());
                registryRestRequest.setInterval(String.valueOf(element.getInterval()));
                HashMap<String, String> labels = new HashMap<>();
                labels.putAll(element.getLabelsMap());
                registryRestRequest.setLabels(labels);
                registryRestRequests.add(registryRestRequest);
            }
        } catch (Exception e) {
            response.setErrorType(ErrorType.UNRECOGNIZED);
            response.setMessage(e.getMessage());
        }
        return registryRestRequests;
    }

    public List<Metapb.LogRecord> getStoreStatusLog(Long start, Long end) throws PDException {
        return logService.getLog(LogService.NODE_CHANGE, start, end);
    }

    public List<Metapb.LogRecord> getPartitionLog(Long start, Long end) throws PDException {
        return logService.getLog(LogService.PARTITION_CHANGE, start, end);
    }

    public Map<Integer, Long> balancePartitionLeader() throws PDException {
        return monitorService.balancePartitionLeader(true);
    }

    public void dbCompaction() throws PDException {
        monitorService.dbCompaction("");
    }

    public List<Metapb.Shard> getShardList(int partitionId) throws PDException {
        return storeNodeService.getShardList(partitionId);
    }

    public void resetPartitionState(Metapb.Partition partition) throws PDException {
        partitionService.updatePartitionState(partition.getGraphName(), partition.getId(),
                Metapb.PartitionState.PState_Normal);
    }

    public int getStoreGroupId(long storeId) throws PDException {
        return storeNodeService.getStoreGroupByStore(storeId);
    }

    @PreDestroy
    public void shutdown(){
        try {
            log.info("shutdown RaftEngine....");
            RaftEngine engine = RaftEngine.getInstance();
            int count = 0;
            while (count++ < 3) {
                Node raftNode = engine.getRaftNode();
                if (raftNode.isLeader(true)) {
                    Status status = raftNode.transferLeadershipTo(PeerId.ANY_PEER);
                    if (status.isOk()) {
                        raftNode.disableVote();
                        break;
                    } else {
                        log.warn("transfer leader with warning: {}", status);
                        synchronized (ShutdownHook.class) {
                            ShutdownHook.class.wait(1000);
                        }
                    }
                } else {
                    break;
                }
            }
            engine.shutDown();
            log.info("RaftEngine shutdown and start to shutdown db....");
            MetadataFactory.closeStore();
            log.info("db shutdown");
            log.info("all resources have been closed");
        } catch (Exception e) {
            log.warn("shutdown with error:", e);
        }
    }
}
