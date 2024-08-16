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

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.ct.base.HGTestLogger;
import org.slf4j.Logger;

public class ClusterConf {

    protected static final Logger LOG = HGTestLogger.LOG;
    protected List<PDConfig> pdConfigs;
    protected List<StoreConfig> storeConfigs;
    protected List<ServerConfig> serverConfigs;

    protected List<String> pdGrpcList, pdRaftList, storeGrpcList;

    public ClusterConf(int pdCnt, int storeCnt, int serverCnt) {
        pdConfigs = new ArrayList<>();
        storeConfigs = new ArrayList<>();
        serverConfigs = new ArrayList<>();

        for (int i = 0; i < pdCnt; i++) {
            PDConfig pdConfig = new PDConfig();
            pdConfig.setStoreCount(storeCnt);
            pdConfigs.add(pdConfig);
            pdGrpcList.add(pdConfig.getGrpcAddress());
            pdRaftList.add(pdConfig.getRaftAddress());
        }

        for (int i = 0; i < storeCnt; i++) {
            StoreConfig storeConfig = new StoreConfig();
            storeConfig.setPDServerList(pdGrpcList);
            storeConfigs.add(storeConfig);
            storeGrpcList.add(storeConfig.getGrpcAddress());
        }

        for (int i = 0; i < serverCnt; i++) {
            ServerConfig serverConfig = new ServerConfig();
            serverConfigs.add(serverConfig);
        }

        for (int i = 0; i < pdCnt; i++) {
            PDConfig pdConfig = pdConfigs.get(i);
            pdConfig.setRaftPeerList(pdGrpcList);
            pdConfig.setStoreGrpcList(storeGrpcList);
        }
    }

    public PDConfig getPDConfig(int i) {
        return pdConfigs.get(i);
    }

    public StoreConfig getStoreConfig(int i) {
        return storeConfigs.get(i);
    }

    public ServerConfig getServerConfig(int i) {
        return serverConfigs.get(i);
    }
}
