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

package org.apache.hugegraph.ct.config;

import static org.apache.hugegraph.ct.base.ClusterConstant.APPLICATION_FILE;
import static org.apache.hugegraph.ct.base.ClusterConstant.CT_PACKAGE_PATH;
import static org.apache.hugegraph.ct.base.ClusterConstant.PD_TEMPLATE_FILE;
import static org.apache.hugegraph.ct.base.EnvUtil.getAvailablePort;

import java.util.List;
import java.util.stream.Collectors;

import lombok.Getter;

public class PDConfig extends AbstractConfig {

    @Getter
    private final int raftPort, grpcPort, restPort;
    @Getter
    private String grpcHost, dataPath, raftHost;

    public PDConfig() {
        readTemplate(CT_PACKAGE_PATH + PD_TEMPLATE_FILE);
        this.fileName = APPLICATION_FILE;
        this.grpcHost = "127.0.0.1";
        this.raftHost = "127.0.0.1";
        this.dataPath = "./pd_data";
        this.raftPort = getAvailablePort();
        this.grpcPort = getAvailablePort();
        this.restPort = getAvailablePort();
        properties.put("GRPC_HOST", grpcHost);
        properties.put("GRPC_PORT", String.valueOf(grpcPort));
        properties.put("REST_PORT", String.valueOf(restPort));
        properties.put("PD_DATA_PATH", dataPath);
        properties.put("RAFT_ADDRESS", raftHost + ":"
                                       + raftPort);
    }

    public void setGrpcHost(String grpcHost) {
        this.grpcHost = grpcHost;
        setProperty("GRPC_HOST", grpcHost);
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
        setProperty("PD_DATA_PATH", dataPath);
    }

    public void setRaftHost(String raftHost) {
        this.raftHost = raftHost;
        setProperty("RAFT_ADDRESS", raftHost + ":" + raftPort);
    }

    public void setRaftPeerList(List<String> raftPeerList) {
        String raftPeers = raftPeerList.stream()
                                       .collect(Collectors.joining(","));
        setProperty("RAFT_PEERS_LIST", raftPeers);
    }

    public void setStoreCount(int storeCount) {
        setProperty("STORE_COUNT", String.valueOf(storeCount));
    }

    public void setStoreGrpcList(List<String> storeGrpcList) {
        String storeGrpcLists = storeGrpcList.stream()
                                             .collect(Collectors.joining(","));
        setProperty("STORE_GRPC_LIST", storeGrpcLists);
    }

    public String getRaftAddress() {
        return raftHost + ":" + raftPort;
    }

    public String getGrpcAddress() {
        return grpcHost + ":" + grpcPort;
    }
}