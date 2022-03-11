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
            return MetaManager.instance().getKafkaBrokerHost();
        }

        private static String getKafkaPort() {
            return MetaManager.instance().getKafkaBrokerPort();
        }
    }

    private BrokerConfig() {

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
