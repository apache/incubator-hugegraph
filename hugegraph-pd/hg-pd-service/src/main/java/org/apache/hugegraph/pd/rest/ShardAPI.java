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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.model.RestApiResponse;
import org.apache.hugegraph.pd.service.PDRestService;
import org.apache.hugegraph.pd.service.PDService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/v1")
public class ShardAPI extends API {

    @Autowired
    PDRestService pdRestService;
    @Autowired
    PDService pdService;

    @GetMapping(value = "/shards", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getShards() {
        try {
            List<Shard> resultShardList = new ArrayList<>();
            List<Metapb.Graph> graphs = pdRestService.getGraphs();
            for (Metapb.Graph graph : graphs) {
                String graphName = graph.getGraphName();
                List<Metapb.Partition> partitions = pdRestService.getPartitions(graphName);
                for (Metapb.Partition pt : partitions) {
                    Metapb.PartitionStats partitionStats =
                            pdRestService.getPartitionStats(graphName, pt.getId());
                    if (partitionStats != null) {
                        List<Metapb.ShardStats> shardStatsList = partitionStats.getShardStatsList();
                        for (Metapb.ShardStats shardStats : shardStatsList) {
                            Shard resultShard = new Shard();
                            resultShard.storeId = shardStats.getStoreId();
                            resultShard.partitionId = pt.getId();
                            resultShard.role = String.valueOf(shardStats.getRole());
                            resultShard.state = String.valueOf(shardStats.getState());
                            resultShard.graphName = graphName;
                            resultShard.progress = shardStats.getProgress();
                            resultShardList.add(resultShard);
                        }
                    } else {
                        List<Metapb.Shard> shardList = new ArrayList<>();
                        var shardGroup = pdService.getStoreNodeService().getShardGroup(pt.getId());
                        if (shardGroup != null) {
                            shardList = shardGroup.getShardsList();
                        } else {
                            log.error(
                                    "ShardAPI.getShards(), get shards of group id: {} returns " +
                                    "null.",
                                    pt.getId());
                        }

                        for (Metapb.Shard shard : shardList) {
                            Shard resultShard = new Shard();
                            resultShard.storeId = shard.getStoreId();
                            resultShard.partitionId = pt.getId();
                            resultShard.role = String.valueOf(shard.getRole());
                            resultShard.state = String.valueOf(Metapb.ShardState.SState_Normal);
                            resultShard.graphName = graphName;
                            resultShard.progress = 0;
                            resultShardList.add(resultShard);
                        }
                    }
                }
            }
            HashMap<String, Object> dataMap = new HashMap<>();
            dataMap.put("shards", resultShardList);
            return new RestApiResponse(dataMap, Pdpb.ErrorType.OK, Pdpb.ErrorType.OK.name());
        } catch (PDException e) {
            log.error("PDException: ", e);
            return new RestApiResponse(null, e.getErrorCode(), e.getMessage());
        }
    }

    @Data
    class Shard {

        long storeId;
        long partitionId;
        String role;
        String state;
        String graphName;
        int progress;
    }
}
