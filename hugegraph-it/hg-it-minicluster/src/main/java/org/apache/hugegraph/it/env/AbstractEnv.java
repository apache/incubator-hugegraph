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

package org.apache.hugegraph.it.env;

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.it.base.HGTestLogger;
import org.apache.hugegraph.it.config.ClusterConf;
import org.apache.hugegraph.it.config.ClusterConfImpl;
import org.apache.hugegraph.it.node.PDNodeWrapper;
import org.apache.hugegraph.it.node.ServerNodeWrapper;
import org.apache.hugegraph.it.node.StoreNodeWrapper;
import org.slf4j.Logger;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractEnv implements BaseEnv {

    private static final Logger LOG = HGTestLogger.LOG;
    private final ClusterConf clusterConf;
    protected List<PDNodeWrapper> pdNodeWrappers;
    protected List<ServerNodeWrapper> serverNodeWrappers;
    protected List<StoreNodeWrapper> storeNodeWrappers;
    protected int cluster_id = 0;

    public AbstractEnv() {
        this.clusterConf = new ClusterConfImpl();
        this.pdNodeWrappers = new ArrayList<>();
        this.serverNodeWrappers = new ArrayList<>();
        this.storeNodeWrappers = new ArrayList<>();
    }

    protected void addPDNode(PDNodeWrapper pdNodeWrapper) {
        this.pdNodeWrappers.add(pdNodeWrapper);
    }

    protected void addStoreNode(StoreNodeWrapper storeNodeWrapper) {
        this.storeNodeWrappers.add(storeNodeWrapper);
    }

    protected void addServerNode(ServerNodeWrapper serverNodeWrapper) {
        this.serverNodeWrappers.add(serverNodeWrapper);
    }

    @Override
    public void initCluster() {
        for (PDNodeWrapper pdNodeWrapper : pdNodeWrappers) {
            pdNodeWrapper.start();
        }
        for (StoreNodeWrapper storeNodeWrapper : storeNodeWrappers) {
            storeNodeWrapper.start();
        }
        for (ServerNodeWrapper serverNodeWrapper : serverNodeWrappers) {
            serverNodeWrapper.start();
        }
    }

    @Override
    public void clearCluster() {
        for (PDNodeWrapper pdNodeWrapper : pdNodeWrappers) {
            pdNodeWrapper.stop();
        }
        for (StoreNodeWrapper storeNodeWrapper : storeNodeWrappers) {
            storeNodeWrapper.stop();
        }
        for (ServerNodeWrapper serverNodeWrapper : serverNodeWrappers) {
            serverNodeWrapper.stop();
        }
    }

    @Override
    public ClusterConf getConf() {
        return this.clusterConf;
    }

}
