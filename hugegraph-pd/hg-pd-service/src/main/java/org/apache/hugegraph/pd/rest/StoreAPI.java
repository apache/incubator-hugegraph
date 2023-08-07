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

package org.apache.hugegraph.pd.rest;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.model.RestApiResponse;
import org.apache.hugegraph.pd.model.StoreRestRequest;
import org.apache.hugegraph.pd.model.TimeRangeRequest;
import org.apache.hugegraph.pd.service.PDRestService;
import org.apache.hugegraph.pd.util.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.protobuf.util.JsonFormat;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/v1")
public class StoreAPI extends API {

    @Autowired
    PDRestService pdRestService;

    @GetMapping(value = "/stores", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getStores() {
        List<StoreStatistics> storeStatsList = new ArrayList<>();
        try {
            HashMap<String, Object> dataMap = new HashMap<>();
            Map<String, Integer> stateCountMap = new HashMap<>();
            for (Metapb.Store store : pdRestService.getStores("")) {
                String stateKey = store.getState().name();
                stateCountMap.put(stateKey, stateCountMap.getOrDefault(stateKey, 0) + 1);
                storeStatsList.add(new StoreStatistics(store));
            }
            storeStatsList.sort((o1, o2) -> o1.address.compareTo(o2.address));
            dataMap.put("stores", storeStatsList);
            dataMap.put("numOfService", storeStatsList.size());
            dataMap.put("numOfNormalService",
                        stateCountMap.getOrDefault(Metapb.StoreState.Up.name(), 0));
            dataMap.put("stateCountMap", stateCountMap);
            return new RestApiResponse(dataMap, Pdpb.ErrorType.OK, Pdpb.ErrorType.OK.name());
        } catch (PDException e) {
            log.error("PDException", e);
            return new RestApiResponse(null, e.getErrorCode(), e.getMessage());
        }
    }

    // 仅支持通过该接口修改 storeState
    @PostMapping(value = "/store/{storeId}", consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String setStore(@PathVariable long storeId, @RequestBody StoreRestRequest request) {
        try {
            Metapb.Store lastStore = pdRestService.getStore(storeId);
            if (lastStore != null) {
                Metapb.Store.Builder builder = Metapb.Store.newBuilder(lastStore);
                Metapb.StoreState storeState = Metapb.StoreState.valueOf(request.getStoreState());
                builder.setState(storeState);
                Metapb.Store newStore = pdRestService.updateStore(builder.build());
                return toJSON(newStore, "store");
            } else {
                return toJSON(new PDException(Pdpb.ErrorType.NOT_FOUND_VALUE, "error"));
            }
        } catch (PDException e) {
            return toJSON(e);
        }
    }

    @GetMapping(value = "/shardGroups", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String getShardGroups() {
        try {
            return toJSON(pdRestService.getShardGroups(), "shardGroups");
        } catch (PDException e) {
            return toJSON(e);
        }
    }

    /**
     * 返回每个store上的leader
     *
     * @return
     */
    @GetMapping(value = "/shardLeaders")
    public Map<String, List<Integer>> shardLeaders() throws PDException {
        Map<String, List<Integer>> leaders = new HashMap<>();
        try {

            List<Metapb.ShardGroup> groups = pdRestService.getShardGroups();
            groups.forEach(group -> {
                group.getShardsList().forEach(shard -> {
                    if (shard.getRole() == Metapb.ShardRole.Leader) {
                        try {
                            String ip = pdRestService.getStore(shard.getStoreId()).getRaftAddress();
                            if (!leaders.containsKey(ip)) {
                                leaders.put(ip, new ArrayList<>());
                            }
                            leaders.get(ip).add(group.getId());
                        } catch (PDException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            });
        } catch (PDException e) {
            throw e;
        }
        return leaders;
    }

    @GetMapping(value = "/balanceLeaders")
    public Map<Integer, Long> balanceLeaders() throws PDException {
        return pdRestService.balancePartitionLeader();
    }

    @DeleteMapping(value = "/store/{storeId}")
    public String removeStore(@PathVariable(value = "storeId") Long storeId) {
        try {
            pdRestService.removeStore(storeId);
        } catch (PDException e) {
            return e.getStackTrace().toString();
        }
        return "OK";
    }

    @PostMapping(value = "/store/log", consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String getStoreLog(@RequestBody TimeRangeRequest request) {
        try {
            Date dateStart = DateUtil.getDate(request.getStartTime());
            Date dateEnd = DateUtil.getDate(request.getEndTime());
            List<Metapb.LogRecord> changedStore =
                    pdRestService.getStoreStatusLog(dateStart.getTime(),
                                                    dateEnd.getTime());
            if (changedStore != null) {
                JsonFormat.TypeRegistry registry = JsonFormat.TypeRegistry
                        .newBuilder().add(Metapb.Store.getDescriptor()).build();
                return toJSON(changedStore, registry);
            } else {
                return toJSON(new PDException(Pdpb.ErrorType.NOT_FOUND_VALUE, "error"));
            }
        } catch (PDException e) {
            return toJSON(e);
        }
    }


    @GetMapping(value = "store/{storeId}", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getStore(@PathVariable long storeId) {
        //获取store的统计信息
        Metapb.Store store = null;
        try {
            store = pdRestService.getStore(storeId);
        } catch (PDException e) {
            return new RestApiResponse(null, e.getErrorCode(), e.getMessage());
        }
        if (store != null) {
            StoreStatistics resultStoreStats = resultStoreStats = new StoreStatistics(store);
            return new RestApiResponse(resultStoreStats, Pdpb.ErrorType.OK,
                                       Pdpb.ErrorType.OK.name());
        } else {
            return new RestApiResponse(null, Pdpb.ErrorType.STORE_ID_NOT_EXIST,
                                       Pdpb.ErrorType.STORE_ID_NOT_EXIST.name());
        }
    }

    @GetMapping(value = "storesAndStats", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String getStoresAndStats() {
        //for debug use
        try {
            List<Metapb.Store> stores = pdRestService.getStores("");
            return toJSON(stores, "stores");
        } catch (PDException e) {
            log.error("PD exception:" + e);
            return toJSON(e);
        }
    }

    @GetMapping(value = "store_monitor/json/{storeId}", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getStoreMonitorData(@PathVariable long storeId) {
        try {
            List<Map<String, Long>> result = pdRestService.getMonitorData(storeId);
            return new RestApiResponse(result, Pdpb.ErrorType.OK, Pdpb.ErrorType.OK.name());
        } catch (PDException e) {
            return new RestApiResponse(null, e.getErrorCode(), e.getMessage());
        }
    }

    @GetMapping(value = "store_monitor/{storeId}")
    @ResponseBody
    public String getStoreMonitorDataText(@PathVariable long storeId) {
        try {
            return pdRestService.getMonitorDataText(storeId);
        } catch (PDException e) {
            return "error:" + e.getErrorCode() + e.getMessage();
        }
    }

    @Data
    class Partition {
        //分区信息
        int partitionId;
        String graphName;
        String role; // shard role
        String workState;
        long dataSize; // 占用的存储空间

        Partition() {
        }

        Partition(Metapb.GraphStats graphStats) {
            partitionId = graphStats.getPartitionId();
            graphName = graphStats.getGraphName();
            final int postfixLength = 2;
            graphName = graphName.substring(0, graphName.length() - postfixLength);
            role = String.valueOf(graphStats.getRole());
            workState = String.valueOf(graphStats.getWorkState());
            dataSize = graphStats.getApproximateSize();
        }
    }

    @Data
    class StoreStatistics {
        //store的统计信息
        long storeId;
        String address;
        String raftAddress;
        String version;
        String state;
        String deployPath;
        String dataPath; // 数据存储路径
        long startTimeStamp;
        long registedTimeStamp; // 暂时取第一次心跳时间作为注册时间
        long lastHeartBeat; // 上一次心跳时间
        long capacity;
        long available;
        int partitionCount;
        int graphSize;
        long keyCount;
        long leaderCount; // shard role = 'Leader'的分区数量
        String serviceName;
        String serviceVersion;
        long serviceCreatedTimeStamp; // 服务创建时间
        List<Partition> partitions;

        StoreStatistics(Metapb.Store store) {
            if (store != null) {
                storeId = store.getId();
                address = store.getAddress();
                raftAddress = store.getRaftAddress();
                state = String.valueOf(store.getState());
                version = store.getVersion();
                deployPath = store.getDeployPath();
                final String prefix = "file:";
                if ((deployPath != null) && (deployPath.startsWith(prefix))) {
                    // 去掉前缀
                    deployPath = deployPath.substring(prefix.length());
                }
                if ((deployPath != null) && (deployPath.contains(".jar"))) {
                    // 去掉jar包之后的信息
                    deployPath = deployPath.substring(0, deployPath.indexOf(".jar") + 4);
                }
                dataPath = store.getDataPath();
                startTimeStamp = store.getStartTimestamp();
                try {
                    serviceCreatedTimeStamp = pdRestService.getStore(store.getId())
                                                           .getStats().getStartTime(); // 实例时间
                    final int base = 1000;
                    serviceCreatedTimeStamp *= base; // 转化为毫秒
                } catch (PDException e) {
                    e.printStackTrace();
                    serviceCreatedTimeStamp = store.getStartTimestamp();
                }
                registedTimeStamp = store.getStartTimestamp(); // 注册时间
                lastHeartBeat = store.getLastHeartbeat();
                capacity = store.getStats().getCapacity();
                available = store.getStats().getAvailable();
                partitionCount = store.getStats().getPartitionCount();
                serviceName = address + "-store";
                serviceVersion = store.getVersion();
                List<Metapb.GraphStats> graphStatsList = store.getStats().getGraphStatsList();
                List<Partition> partitionStatsList = new ArrayList<>(); // 保存分区信息
                HashSet<String> graphNameSet = new HashSet<>(); // 用于统计图的数量
                HashSet<Integer> leaderPartitionIds = new HashSet<Integer>(); // 统计leader的分区数量
                // 构造分区信息(store中存储的图信息)
                Map<Integer, Long> partition2KeyCount = new HashMap<>();
                for (Metapb.GraphStats graphStats : graphStatsList) {
                    String graphName = graphStats.getGraphName();
                    // 图名只保留/g /m /s前面的部分
                    final int postfixLength = 2;
                    graphNameSet.add(graphName.substring(0, graphName.length() - postfixLength));
                    if ((graphStats.getGraphName() != null) &&
                        (graphStats.getGraphName().endsWith("/g"))) {
                        Partition pt = new Partition(graphStats);
                        partitionStatsList.add(pt);
                    }
                    // 统计每个分区的keyCount
                    partition2KeyCount.put(graphStats.getPartitionId(),
                                           graphStats.getApproximateKeys());
                    if (graphStats.getRole() == Metapb.ShardRole.Leader) {
                        leaderPartitionIds.add(graphStats.getPartitionId());
                    }
                }
                for (Map.Entry<Integer, Long> entry : partition2KeyCount.entrySet()) {
                    keyCount += entry.getValue();
                }
                partitions = partitionStatsList;
                graphSize = graphNameSet.size();
                leaderCount = leaderPartitionIds.size();
            }

        }
    }

}
