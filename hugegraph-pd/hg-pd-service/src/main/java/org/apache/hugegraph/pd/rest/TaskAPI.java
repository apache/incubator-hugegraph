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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.pd.common.HdfsUtils;
import org.apache.hugegraph.pd.common.KVPair;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.model.BulkloadRestRequest;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.service.PDRestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

import org.springframework.web.client.RestTemplate;

@RestController
@Slf4j
@RequestMapping("/v1/task")
public class TaskAPI extends API {

    @Autowired
    PDRestService pdRestService;

    @Autowired
    private RestTemplate restTemplate;

    public boolean isLeader() {
        return RaftEngine.getInstance().isLeader();
    }

    @GetMapping(value = "/patrolStores", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String patrolStores() {
        try {
            List<Metapb.Store> stores = pdRestService.patrolStores();
            return toJSON(stores, "stores");
        } catch (PDException e) {
            e.printStackTrace();
            return toJSON(e);
        }
    }

    @GetMapping(value = "/patrolPartitions", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String patrolPartitions() {
        try {
            List<Metapb.Partition> partitions = pdRestService.patrolPartitions();
            return toJSON(partitions, "partitions");
        } catch (PDException e) {
            e.printStackTrace();
            return toJSON(e);
        }
    }

    @GetMapping(value = "/balancePartitions", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Map<Integer, KVPair<Long, Long>> balancePartitions() {
        try {
            Map<Integer, KVPair<Long, Long>> partitions = pdRestService.balancePartitions();
            return partitions;
        } catch (PDException e) {
            e.printStackTrace();
            return null;
        }
    }

    @GetMapping(value = "/splitPartitions", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String splitPartitions() {
        try {
            List<Metapb.Partition> partitions = pdRestService.splitPartitions();
            return toJSON(partitions, "partitions");
        } catch (PDException e) {
            e.printStackTrace();
            return toJSON(e);
        }
    }

    @PostMapping(value = "/bulkload")
    public Map<String,String> bulkload(@RequestBody BulkloadRestRequest request) throws PDException {
        if (isLeader()) {
            // 当前节点是Leader，处理请求
            return handleBulkload(request);
        } else {
            // 当前节点不是Leader，转发请求到Leader
            String leaderAddress = RaftEngine.getInstance().getLeader().getIp();
            String url = "http://" + leaderAddress+":8620" + "/v1/task/bulkload";
            ResponseEntity<Map> mapResponseEntity = restTemplate.postForEntity(url, request, Map.class);
            return mapResponseEntity.getBody();
        }
    }

    private Map<String,String> handleBulkload(BulkloadRestRequest request)  {
        Map<Integer, String> parseHdfsPathMap = null;
        Map<String,String> resMap = new HashMap<>();
        try (HdfsUtils hdfsUtils = new HdfsUtils(request.getHdfsPath())) {
            parseHdfsPathMap = hdfsUtils.parseHdfsPath(request.getHdfsPath());
            boolean result = pdRestService.bulkload(request.getGraphName(),
                                                    request.getTableName(), parseHdfsPathMap,
                                                    request.getMaxDownloadRate());
            resMap.put("status", result?"success":"failed");
        } catch (Exception e) {
            log.info("bulkload failed,error:{}", toJSON(e));
            resMap.put("message", e.getMessage());
        }
        return resMap;
    }

    @GetMapping(value = "/balanceLeaders")
    public Map<Integer, Long> balanceLeaders() throws PDException {
        return pdRestService.balancePartitionLeader();
    }

    @GetMapping(value = "/compact")
    public String dbCompaction() throws PDException {
        pdRestService.dbCompaction();
        return "compact ok";
    }
}
