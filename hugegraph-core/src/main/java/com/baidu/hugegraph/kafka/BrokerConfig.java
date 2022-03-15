/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.kafka;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import com.alipay.sofa.jraft.util.concurrent.ConcurrentHashSet;
import com.baidu.hugegraph.meta.MetaManager;

/**
 * BrokerConfig used to init producer and consumer
 * @author Scorpiour
 * @since 2022-01-18
 */
public final class BrokerConfig {

    private final MetaManager manager;
    private final String SYNC_BROKER_KEY;
    private final String SYNC_STORAGE_KEY;
    private final String FILTER_GRAPH_KEY;
    private final String FILTER_GRAPH_SPACE_KEY;

    private volatile boolean needSyncBroker = false;
    private volatile boolean needSyncStorage = false;

    private final Set<String> filteredGraph = new ConcurrentHashSet<>();
    private final Set<String> filteredGraphSpace = new ConcurrentHashSet<>();

    private static class ConfigHolder {
        public final static BrokerConfig instance = new BrokerConfig();
        public final static HugeGraphClusterRole clusterRole = ConfigHolder.getClusterRole();
        public final static String brokerHost = ConfigHolder.getKafkaHost();
        public final static String brokerPort = ConfigHolder.getKafkaPort();
        public final static int partitionCount = 1;


        private static HugeGraphClusterRole getClusterRole() {
            MetaManager manager = MetaManager.instance();
            String val = manager.getHugeGraphClusterRole();
            HugeGraphClusterRole role = HugeGraphClusterRole.fromName(val);
            return role;
        }

        private static String getKafkaHost() {
            String result =MetaManager.instance().getKafkaBrokerHost();
            return result;
        }

        private static String getKafkaPort() {
            String result = MetaManager.instance().getKafkaBrokerPort();
            return result;
        }
    }

    private BrokerConfig() {
        this.manager = MetaManager.instance();
        this.SYNC_BROKER_KEY = manager.kafkaSyncBrokerKey();
        this.SYNC_STORAGE_KEY = manager.kafkaSyncStorageKey();
        this.FILTER_GRAPH_KEY = manager.kafkaFilterGraphKey();
        this.FILTER_GRAPH_SPACE_KEY = manager.kafkaFilterGraphspaceKey();
        
        this.updateNeedSyncBroker();
        this.updateNeedSyncStorage();

        manager.listenKafkaConfig(this::kafkaConfigEventHandler);
    }

    private <T> void kafkaConfigEventHandler(T response) {
        Map<String, String> events = manager.extractKVFromResponse(response);
        for(Map.Entry<String, String> entry : events.entrySet()) {
            String key = entry.getKey();
            if (this.SYNC_BROKER_KEY.equals(key)) {
                this.needSyncBroker = "1".equals(entry.getValue());
            } else if (this.SYNC_STORAGE_KEY.equals(key)) {
                this.needSyncStorage = "1".equals(entry.getValue());
            } else if (this.FILTER_GRAPH_KEY.equals(key)) {
                String[] graphs = entry.getValue().split(",");
                this.filteredGraph.clear();
                this.filteredGraph.addAll(Arrays.asList(graphs));
            } else if (this.FILTER_GRAPH_SPACE_KEY.equals(key)) {
                String[] graphSpaces = entry.getValue().split(",");
                this.filteredGraphSpace.clear();
                this.filteredGraphSpace.addAll(Arrays.asList(graphSpaces));
            }
        }
    }

    private void updateNeedSyncBroker() {
        String res = manager.getRaw(this.SYNC_BROKER_KEY);
        this.needSyncBroker = "1".equals(res);
    }

    private void updateNeedSyncStorage() {
        String res = manager.getRaw(this.SYNC_STORAGE_KEY);
        this.needSyncStorage = "1".equals(res);
    }


        /**
     * Indicates when if need sync data between hugegraph-server & broker
     * Should be functioned dynamically
     * If returns true, both Master and slave will be produce topics to broker
     * @return
     */
    public boolean needKafkaSyncBroker() {
        return this.needSyncBroker;
    }

    /**
     * Indicates when if need sync data between hugegraph-server & storage
     * Should be functioned dynamically
     * If returns true, Master's consumer will consume data from broker, then push to slave,
     * while Slave will consume data from broker, then commit them to storage
     * @return
     */
    public boolean needKafkaSyncStorage() {
        return this.needSyncStorage;
    }

    public static BrokerConfig getInstance() {
        return ConfigHolder.instance;
    }

    public HugeGraphClusterRole getClusterRole() {
        return ConfigHolder.clusterRole;
    }

    public int getPartitionCount() {
        return ConfigHolder.partitionCount;
    }

    public String getKafkaHost() {
        return ConfigHolder.brokerHost;
    }

    public String getKafkaPort() {
        return ConfigHolder.brokerPort;
    }

    public Boolean isMaster() {
        return HugeGraphClusterRole.MASTER.equals(ConfigHolder.clusterRole);
    }

    public Boolean isSlave() {
        return HugeGraphClusterRole.SLAVE.equals(ConfigHolder.clusterRole);
    }

    public String getSyncGroupId() {
        return  "hugegraph-sync-consumer-group";
    }

    public String getSyncGroupInstanceId() {
        return "hugegraph-sync-consumer-instance-1";
    }

    public String getMutateGroupId() {
        return  "hugegraph-mutate-consumer-group";
    }

    public String getMutateGroupInstanceId() {
        return "hugegraph-mutate-consumer-instance-1";
    }

    public String getConfGroupId() {
        return "hugegraph-conf-consumer-group";
    }

    public String getConfGroupInstanceId() {
        return "hugegraph-conf-consumer-instance-1";
    }

    public String getConfPrefix() {
        return "GLOBAL-";
    }

    public boolean graphSpaceFiltered(String graphSpace) {
        return filteredGraphSpace.contains(graphSpace);
    }

    public boolean graphFiltered(String graphSpace, String graph) {
        return this.filteredGraph.contains(graph);
    }
}
