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

import java.util.Map;

import com.baidu.hugegraph.meta.MetaManager;
import com.baidu.hugegraph.util.Log;

import org.slf4j.Logger;

/**
 * BrokerConfig used to init producer and consumer
 * @author Scorpiour
 * @since 2022-01-18
 */
public final class BrokerConfig {

    private static final Logger log = Log.logger(BrokerConfig.class);
    private final MetaManager manager;
    private final String SYNC_BROKER_KEY;
    private final String SYNC_STORAGE_KEY;

    private volatile boolean needSyncBroker = false;
    private volatile boolean needSyncStorage = false;

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
            log.info("====> Scorpiour: Get cluster role is {}", role);
            return role;
        }

        private static String getKafkaHost() {
            String result =MetaManager.instance().getKafkaBrokerHost();
            log.info("====> Scorpiour: get Kafka Host {}", result);
            return result;
        }

        private static String getKafkaPort() {
            String result = MetaManager.instance().getKafkaBrokerPort();
            log.info("====> Scorpiour: get Kafka port {}", result);
            return result;
        }
    }

    private BrokerConfig() {
        this.manager = MetaManager.instance();
        this.SYNC_BROKER_KEY = manager.kafkaSyncBrokerKey();
        this.SYNC_STORAGE_KEY = manager.kafkaSyncStorageKey();
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
            }
        }
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
        log.info("====> Scorpiour: Check is master now");
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
}
