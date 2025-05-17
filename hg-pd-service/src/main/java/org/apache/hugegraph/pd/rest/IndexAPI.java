<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/rest/IndexAPI.java
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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
========
package org.apache.hugegraph.pd.rest;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.common.ErrorType;
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/rest/IndexAPI.java
import org.apache.hugegraph.pd.model.RestApiResponse;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.rest.MemberAPI.CallStreamObserverWrap;
import org.apache.hugegraph.pd.service.PDRestService;
import org.apache.hugegraph.pd.service.PDService;
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/rest/IndexAPI.java
========
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/rest/IndexAPI.java
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/rest/IndexAPI.java
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
========
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/rest/IndexAPI.java

import static org.apache.hugegraph.pd.common.Consts.DEFAULT_STORE_GROUP_ID;

@RestController
@Slf4j
@RequestMapping("/")
public class IndexAPI extends API {

    @Autowired
    PDService pdService;
    @Autowired
    PDRestService pdRestService;

    @GetMapping(value = "/", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public BriefStatistics index() throws PDException, ExecutionException, InterruptedException {

        BriefStatistics statistics = new BriefStatistics();
        statistics.leader = RaftEngine.getInstance().getLeaderGrpcAddress();
        statistics.state = getClusterState();
        statistics.storeSize = pdService.getStoreNodeService().getActiveStores().size();
        statistics.graphSize = pdService.getPartitionService().getGraphs().size();
        statistics.partitionSize = pdService.getStoreNodeService().getShardGroups().size();
        return statistics;

    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/rest/IndexAPI.java
========
    @Data
    class BriefStatistics {
        Map<Integer, String> state;
        String leader;
        int memberSize;
        int storeSize;
        int graphSize;
        int partitionSize;
    }

>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/rest/IndexAPI.java
    @GetMapping(value = "/v1/cluster", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse cluster() throws InterruptedException, ExecutionException {
        Statistics statistics = new Statistics();
        try {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/rest/IndexAPI.java
            statistics.state =
                    String.valueOf(pdService.getStoreNodeService().getClusterStats().getState());
            String leaderGrpcAddress = RaftEngine.getInstance().getLeaderGrpcAddress();
            CallStreamObserverWrap<Pdpb.GetMembersResponse> response =
                    new CallStreamObserverWrap<>();
========
            statistics.states = getClusterState();
            statistics.state = statistics.getStates().get(DEFAULT_STORE_GROUP_ID);
            String leaderGrpcAddress = RaftEngine.getInstance().getLeaderGrpcAddress();
            CallStreamObserverWrap<Pdpb.GetMembersResponse> response = new CallStreamObserverWrap<>();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/rest/IndexAPI.java
            pdService.getMembers(Pdpb.GetMembersRequest.newBuilder().build(), response);
            List<Member> pdList = new ArrayList<>();
            for (Metapb.Member member : response.get().get(0).getMembersList()) {
                Member member1 = new Member(member);
                if ((leaderGrpcAddress != null) &&
                    (leaderGrpcAddress.equals(member.getGrpcUrl()))) {
                    member1.role = "Leader";
                    statistics.pdLeader = member1;
                } else {
                    member1.role = "Follower";
                }
                pdList.add(member1);
            }
            statistics.pdList = pdList;
            statistics.memberSize = pdList.size();
            List<Store> stores = new ArrayList<>();
            for (Metapb.Store store : pdService.getStoreNodeService().getStores()) {
                stores.add(new Store(store));
            }
            statistics.stores = stores;
            statistics.storeSize = statistics.stores.size();
            statistics.onlineStoreSize = pdService.getStoreNodeService().getActiveStores().size();
            statistics.offlineStoreSize = statistics.storeSize - statistics.onlineStoreSize;
            List<Metapb.Graph> graphs = pdRestService.getGraphs();
            statistics.graphSize = graphs.stream().filter((g) -> (g.getGraphName() != null)
                                                                 &&
                                                                 (g.getGraphName().endsWith("/g")))
                                         .count();
            statistics.partitionSize = pdService.getStoreNodeService().getShardGroups().size();
            statistics.shardCount = pdService.getConfigService().getPDConfig().getShardCount();
            for (Metapb.Store store : pdService.getStoreNodeService().getStores()) {
                List<Metapb.GraphStats> graphStatsList = store.getStats().getGraphStatsList();
                for (Metapb.GraphStats graphStats : graphStatsList) {
                    statistics.keyCount += graphStats.getApproximateKeys();
                    statistics.dataSize += graphStats.getApproximateSize();
                }
            }
            // Data status: The data status is deduced based on the state of the graph, the
            // larger the enumeration value, the more serious the problem, and the default is the
            // normal state
            Metapb.PartitionState dataState = Metapb.PartitionState.PState_Normal;
            for (Metapb.Graph graph : pdRestService.getGraphs()) {
                if (graph.getState() == Metapb.PartitionState.UNRECOGNIZED) {
                    // If it is not recognized, it will not participate in the
                    // comparison, otherwise an exception will be thrown
                    continue;
                }
                if ((graph.getState() != null) &&
                    (graph.getState().getNumber() > dataState.getNumber())) {
                    dataState = graph.getState();
                }
            }
            statistics.dataState = dataState.name();
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/rest/IndexAPI.java
            return new RestApiResponse(statistics, Pdpb.ErrorType.OK, Pdpb.ErrorType.OK.name());
        } catch (PDException e) {
========
            return new RestApiResponse(statistics, ErrorType.OK, ErrorType.OK.name());
        } catch (PDException e){
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/rest/IndexAPI.java
            log.error("PD Exception: ", e);
            return new RestApiResponse(null, e.getErrorCode(), e.getMessage());
        }
    }

    private Map<Integer, String> getClusterState() {
        Map<Integer, String> state = new HashMap<>();
        for (var entry : pdService.getStoreNodeService().getAllClusterStats().entrySet())  {
            state.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
        return state;
    }

    @Data
    class BriefStatistics {

        String state;
        String leader;
        int memberSize;
        int storeSize;
        int graphSize;
        int partitionSize;
    }

    @Data
    class Store {

        long storeId;
        String address;
        String raftAddress;
        String version;
        String state;
        long startTimeStamp;

        public Store(Metapb.Store store) {
            if (store != null) {
                storeId = store.getId();
                address = store.getAddress();
                raftAddress = store.getRaftAddress();
                version = store.getVersion();
                state = String.valueOf(store.getState());
                startTimeStamp = store.getStartTimestamp();
            }

        }
    }

    @Data
    class Member {

        String raftUrl;
        String grpcUrl;
        String restUrl;
        String state;
        String dataPath;
        String role;
        String serviceName; // Service name, custom attributes
        String serviceVersion; // Static definitions
        long startTimeStamp; // The time when the process started

        public Member(Metapb.Member member) {
            if (member != null) {
                raftUrl = member.getRaftUrl();
                grpcUrl = member.getGrpcUrl();
                restUrl = member.getRestUrl();
                state = String.valueOf(member.getState());
                dataPath = member.getDataPath();
                serviceName = grpcUrl + "-PD";
                serviceVersion = VERSION;
                startTimeStamp = ManagementFactory.getRuntimeMXBean().getStartTime();
            }
        }

        public Member() {

        }
    }

    @Data
    class Statistics {

        /**
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/rest/IndexAPI.java
         * Cluster status, default of the cluster
         */
        String state;
        /**
         * Data status
========
         * 集群状态, 默认集群的
         */
        String state;
        /**
         * 集群状态
         */
        Map<Integer, String> states;
        /**
         * 数据状态
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/rest/IndexAPI.java
         */
        String dataState;
        /**
         * pd Cluster members
         */
        List<Member> pdList;
        /**
         * pd The leader of the cluster
         */
        Member pdLeader;
        /**
         * pd The size of the cluster
         */
        int memberSize;
        /**
         * stores list
         */
        List<Store> stores;
        /**
         * store quantity
         */
        int storeSize;
        /**
         * onlineStore
         */
        int onlineStoreSize;
        /**
         * The number of stores that are offline
         */
        int offlineStoreSize;
        /**
         * The number of graphs
         */
        long graphSize;
        /**
         * The number of partitions
         */
        int partitionSize;
        /**
         * Number of partition replicas
         */
        int shardCount;
        /**
         * The number of keys
         */
        long keyCount;
        /**
         * Amount of data
         */
        long dataSize;

    }
}
