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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.model.RestApiResponse;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.rest.MemberAPI.CallStreamObserverWrap;
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
        statistics.state = pdService.getStoreNodeService().getClusterStats().getState().toString();
        statistics.storeSize = pdService.getStoreNodeService().getActiveStores().size();
        statistics.graphSize = pdService.getPartitionService().getGraphs().size();
        statistics.partitionSize = pdService.getStoreNodeService().getShardGroups().size();
        return statistics;

    }

    @GetMapping(value = "/v1/cluster", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse cluster() throws InterruptedException, ExecutionException {
        Statistics statistics = new Statistics();
        try {
            statistics.state =
                    String.valueOf(pdService.getStoreNodeService().getClusterStats().getState());
            String leaderGrpcAddress = RaftEngine.getInstance().getLeaderGrpcAddress();
            CallStreamObserverWrap<Pdpb.GetMembersResponse> response =
                    new CallStreamObserverWrap<>();
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
            // 图的数量，只统计/g
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
            // 数据状态：根据图的状态推出数据状态,枚举值越大，问题越严重， 默认为正常状态
            Metapb.PartitionState dataState = Metapb.PartitionState.PState_Normal;
            for (Metapb.Graph graph : pdRestService.getGraphs()) {
                if (graph.getState() == Metapb.PartitionState.UNRECOGNIZED) {
                    continue; // 未识别不参与对比，不然会抛出异常
                }
                if ((graph.getState() != null) &&
                    (graph.getState().getNumber() > dataState.getNumber())) {
                    dataState = graph.getState();
                }
            }
            statistics.dataState = dataState.name();
            return new RestApiResponse(statistics, Pdpb.ErrorType.OK, Pdpb.ErrorType.OK.name());
        } catch (PDException e) {
            log.error("PD Exception: ", e);
            return new RestApiResponse(null, e.getErrorCode(), e.getMessage());
        }
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
        String serviceName; //服务名称，自定义属性
        String serviceVersion; //静态定义
        long startTimeStamp; //进程启动时间

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
         * 集群状态
         */
        String state;
        /**
         * 数据状态
         */
        String dataState;
        /**
         * pd集群成员
         */
        List<Member> pdList;
        /**
         * pd集群的leader
         */
        Member pdLeader;
        /**
         * pd集群的大小
         */
        int memberSize;
        /**
         * stores列表
         */
        List<Store> stores;
        /**
         * store的数量
         */
        int storeSize;
        /**
         * onlineStore
         */
        int onlineStoreSize;
        /**
         * 离线的store的数量
         */
        int offlineStoreSize;
        /**
         * 图的数量
         */
        long graphSize;
        /**
         * 分区的数量
         */
        int partitionSize;
        /**
         * 分区副本数
         */
        int shardCount;
        /**
         * key的数量
         */
        long keyCount;
        /**
         * 数据量
         */
        long dataSize;

    }
}

