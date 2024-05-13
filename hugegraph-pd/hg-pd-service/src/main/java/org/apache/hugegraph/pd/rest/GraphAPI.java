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

package org.apache.hugegraph.pd.rest;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.model.GraphRestRequest;
import org.apache.hugegraph.pd.model.RestApiResponse;
import org.apache.hugegraph.pd.service.PDRestService;
import org.apache.hugegraph.pd.service.PDService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/v1")
public class GraphAPI extends API {

    @Autowired
    PDRestService pdRestService;
    @Autowired
    PDService pdService;

    @GetMapping(value = "/graph/partitionSizeRange", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getPartitionSizeRange() {
        try {
            int minPartitionSize = 1;
            int maxPartitionSize = pdService.getStoreNodeService().getShardGroups().size();
            Map<String, Integer> dataMap = new HashMap<>();
            dataMap.put("minPartitionSize", minPartitionSize);
            dataMap.put("maxPartitionSize", maxPartitionSize);
            return new RestApiResponse(dataMap, Pdpb.ErrorType.OK, Pdpb.ErrorType.OK.name());
        } catch (PDException e) {
            log.error("PDException:", e);
            return new RestApiResponse(null, e.getErrorCode(), e.getMessage());
        }
    }

    @GetMapping(value = "/graphs", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getGraphs() {
        RestApiResponse response = new RestApiResponse();
        try {
            List<Metapb.Graph> graphs = pdRestService.getGraphs();
            List<GraphStatistics> resultGraphs = new ArrayList<>();
            for (Metapb.Graph graph : graphs) {
                if ((graph.getGraphName() != null) && (graph.getGraphName().endsWith("/g"))) {
                    resultGraphs.add(new GraphStatistics(graph));
                }
            }
            HashMap<String, Object> dataMap = new HashMap<>();
            dataMap.put("graphs", resultGraphs);
            response.setData(dataMap);
            response.setStatus(Pdpb.ErrorType.OK.getNumber());
            response.setMessage(Pdpb.ErrorType.OK.name());

        } catch (PDException e) {
            log.error("PDException: ", e);
            response.setData(new HashMap<String, Object>());
            response.setStatus(e.getErrorCode());
            response.setMessage(e.getMessage());
        }
        return response;
    }

    @PostMapping(value = "/graph/**", consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String setGraph(@RequestBody GraphRestRequest body, HttpServletRequest request) {
        try {
            String requestURL = request.getRequestURL().toString();
            final String prefix = "/graph/";
            final int limit = 2;
            String graphName = requestURL.split(prefix, limit)[1];
            graphName = URLDecoder.decode(graphName, StandardCharsets.UTF_8);
            Metapb.Graph curGraph = pdRestService.getGraph(graphName);
            Metapb.Graph.Builder builder = Metapb.Graph.newBuilder(
                    curGraph == null ? Metapb.Graph.getDefaultInstance() : curGraph);
            builder.setGraphName(graphName);
            if (body.getPartitionCount() > 0) {
                builder.setPartitionCount(body.getPartitionCount());
            }

            Metapb.Graph newGraph = pdRestService.updateGraph(builder.build());
            return toJSON(newGraph, "graph");
        } catch (PDException exception) {
            return toJSON(exception);
        } catch (Exception e) {
            return toJSON(e);
        }
    }

    @GetMapping(value = "/graph/**", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getGraph(HttpServletRequest request) throws
                                                                UnsupportedEncodingException {
        RestApiResponse response = new RestApiResponse();
        GraphStatistics statistics = null;
        String requestURL = request.getRequestURL().toString();
        final String prefix = "/graph/";
        final int limit = 2;
        String graphName = requestURL.split(prefix, limit)[1];
        graphName = URLDecoder.decode(graphName, StandardCharsets.UTF_8);
        try {
            Metapb.Graph graph = pdRestService.getGraph(graphName);
            if (graph != null) {
                statistics = new GraphStatistics(graph);
                response.setData(statistics);
            } else {
                response.setData(new HashMap<String, Object>());
            }
            response.setStatus(Pdpb.ErrorType.OK.getNumber());
            response.setMessage(Pdpb.ErrorType.OK.name());
        } catch (PDException e) {
            log.error(e.getMessage());
            response.setData(new HashMap<String, Object>());
            response.setStatus(Pdpb.ErrorType.UNKNOWN.getNumber());
            response.setMessage(e.getMessage());
        }
        return response;
    }

    @Data
    class Shard {

        long partitionId;
        long storeId;
        String state;
        String role;
        int progress;

        public Shard(Metapb.ShardStats shardStats, long partitionId) {
            this.role = String.valueOf(shardStats.getRole());
            this.storeId = shardStats.getStoreId();
            this.state = String.valueOf(shardStats.getState());
            this.partitionId = partitionId;
            this.progress = shardStats.getProgress();
        }

        public Shard(Metapb.Shard shard, long partitionId) {
            this.role = String.valueOf(shard.getRole());
            this.storeId = shard.getStoreId();
            this.state = Metapb.ShardState.SState_Normal.name();
            this.progress = 0;
            this.partitionId = partitionId;
        }

    }

    @Data
    class Partition {

        int partitionId;
        String graphName;
        String workState;
        long startKey;
        long endKey;
        List<Shard> shards;
        long dataSize;

        public Partition(Metapb.Partition pt, Metapb.PartitionStats partitionStats) {
            if (pt != null) {
                partitionId = pt.getId();
                startKey = pt.getStartKey();
                endKey = pt.getEndKey();
                workState = String.valueOf(pt.getState());
                graphName = pt.getGraphName();
                final int postfixLength = 2;
                graphName = graphName.substring(0, graphName.length() - postfixLength);
                if (partitionStats != null) {
                    List<Metapb.ShardStats> shardStatsList = partitionStats.getShardStatsList();
                    List<Shard> shardsList = new ArrayList<>();
                    for (Metapb.ShardStats shardStats : shardStatsList) {
                        Shard shard = new Shard(shardStats, partitionId);
                        shardsList.add(shard);
                    }
                    this.shards = shardsList;
                } else {
                    List<Shard> shardsList = new ArrayList<>();
                    try {
                        var shardGroup = pdService.getStoreNodeService().getShardGroup(pt.getId());
                        if (shardGroup != null) {
                            for (Metapb.Shard shard1 : shardGroup.getShardsList()) {
                                shardsList.add(new Shard(shard1, partitionId));
                            }
                        } else {
                            log.error("GraphAPI.Partition(), get shard group: {} returns null",
                                      pt.getId());
                        }
                    } catch (PDException e) {
                        log.error("Partition init failed, error: {}", e.getMessage());
                    }
                    this.shards = shardsList;
                }

            }
        }
    }

    @Data
    class GraphStatistics {

        // Graph statistics
        String graphName;
        long partitionCount;
        String state;
        List<Partition> partitions;
        long dataSize;
        //todo
        int nodeCount;
        int edgeCount;
        long keyCount;

        public GraphStatistics(Metapb.Graph graph) throws PDException {
            if (graph == null) {
                return;
            }
            Map<Integer, Long> partition2DataSize = new HashMap<>();
            graphName = graph.getGraphName();
            partitionCount = graph.getPartitionCount();
            state = String.valueOf(graph.getState());
            // The amount of data and the number of keys
            List<Metapb.Store> stores = pdRestService.getStores(graphName);
            for (Metapb.Store store : stores) {
                List<Metapb.GraphStats> graphStatsList = store.getStats().getGraphStatsList();
                for (Metapb.GraphStats graphStats : graphStatsList) {
                    if ((graphName.equals(graphStats.getGraphName()))
                        && (Metapb.ShardRole.Leader.equals(graphStats.getRole()))) {
                        keyCount += graphStats.getApproximateKeys();
                        dataSize += graphStats.getApproximateSize();
                        partition2DataSize.put(graphStats.getPartitionId(),
                                               graphStats.getApproximateSize());
                    }
                }
            }
            List<Partition> resultPartitionList = new ArrayList<>();
            List<Metapb.Partition> tmpPartitions = pdRestService.getPartitions(graphName);
            if ((tmpPartitions != null) && (!tmpPartitions.isEmpty())) {
                // The partition information to be returned
                for (Metapb.Partition partition : tmpPartitions) {
                    Metapb.PartitionStats partitionStats = pdRestService
                            .getPartitionStats(graphName, partition.getId());
                    Partition pt = new Partition(partition, partitionStats);
                    pt.dataSize = partition2DataSize.getOrDefault(partition.getId(), 0L);
                    resultPartitionList.add(pt);
                }
            }
            partitions = resultPartitionList;
            // Hide /g /m /s after the title of the graph
            final int postfixLength = 2;
            graphName = graphName.substring(0, graphName.length() - postfixLength);
        }
    }
}
