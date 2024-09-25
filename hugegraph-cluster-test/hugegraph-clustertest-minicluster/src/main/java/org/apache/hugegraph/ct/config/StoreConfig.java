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
import static org.apache.hugegraph.ct.base.ClusterConstant.STORE_TEMPLATE_FILE;
import static org.apache.hugegraph.ct.base.EnvUtil.getAvailablePort;

import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import lombok.Getter;

public class StoreConfig extends AbstractConfig {

    @Getter
    private int raftPort;
    @Getter
    private int grpcPort;
    @Getter
    private int restPort;

    public StoreConfig() {
        readTemplate(
                Paths.get(CT_PACKAGE_PATH + STORE_TEMPLATE_FILE));
        this.fileName = APPLICATION_FILE;
        this.raftPort = getAvailablePort();
        this.grpcPort = getAvailablePort();
        this.restPort = getAvailablePort();
        properties.put("GRPC_PORT", String.valueOf(this.grpcPort));
        properties.put("REST_PORT", String.valueOf(this.restPort));
        properties.put("RAFT_ADDRESS", "127.0.0.1:" + this.raftPort);
    }

    public void setPDServerList(List<String> pdServerList) {
        String pdServers = pdServerList.stream()
                                       .collect(Collectors.joining(","));
        setProperty("PD_SERVER_ADDRESS", pdServers);
    }

    public String getGrpcAddress() {
        return "127.0.0.1:" + this.grpcPort;
    }
}
