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

package org.apache.hugegraph.store.node.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.store.node.grpc.HgStoreNodeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.util.Endpoint;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.meta.Partition;
import com.baidu.hugegraph.store.metric.HgStoreMetric;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/")
public class IndexAPI {
    @Autowired
    HgStoreNodeService nodeService;

    @GetMapping(value = "/", produces = "application/json")
    public StoreInfo index() {
        StoreInfo info = new StoreInfo();
        info.leaderCount = nodeService.getStoreEngine().getLeaderPartition().size();
        info.partitionCount = nodeService.getStoreEngine().getPartitionEngines().size();
        return info;
    }

    @Data
    class StoreInfo {
        private int leaderCount;
        private int partitionCount;
    }

    @Data
    public class Raft {
        private int groupId;
        private String role;
        private String conf;
        private Endpoint leader;
        private long logIndex;
        private List<PeerId> peers;
        private List<PeerId> learners;
        private final List<PartitionInfo> partitions = new ArrayList<>();
    }

    @Data
    public class PartitionInfo {
        private final int id;                                             // region id
        private final String graphName;
        // Region key range [startKey, endKey)
        private final long startKey;
        private final long endKey;
        private HgStoreMetric.Partition metric;
        private final String version;
        private final Metapb.PartitionState workState;

        private String leader;


        public PartitionInfo(Partition pt) {
            id = pt.getId();
            graphName = pt.getGraphName();
            startKey = pt.getStartKey();
            endKey = pt.getEndKey();

            workState = pt.getWorkState();
            version = String.valueOf(pt.getVersion());

        }
    }

    public Map<String, Object> okMap(String k, Object v) {
        Map<String, Object> map = new HashMap<>();
        map.put("status", 0);
        map.put(k, v);
        return map;
    }
}

