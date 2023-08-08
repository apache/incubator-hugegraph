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

package org.apache.hugegraph.pd.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.IdService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import lombok.Data;


/**
 * PD配置文件
 */
@Data
@Component
public class PDConfig {

    @Value("${pd.cluster_id:1}")
    private long clusterId;   // 集群ID

    @Value("${pd.patrol-interval:300}")
    private long patrolInterval = 300;  //巡查任务时间间隔
    @Value("${pd.data-path}")
    private String dataPath;
    @Value("${pd.initial-store-count:3}")
    private int minStoreCount;

    // 初始store列表，该列表内的store自动激活
    @Value("${pd.initial-store-list: ''}")
    private String initialStoreList;
    @Value("${grpc.host}")
    private String host;

    @Value("${license.verify-path}")
    private String verifyPath;
    @Value("${license.license-path}")
    private String licensePath;
    @Autowired
    private ThreadPoolGrpc threadPoolGrpc;
    @Autowired
    private Raft raft;
    @Autowired
    private Store store;
    @Autowired
    private Partition partition;
    @Autowired
    private Discovery discovery;
    private Map<String, String> initialStoreMap = null;
    private ConfigService configService;
    private IdService idService;

    public Map<String, String> getInitialStoreMap() {
        if (initialStoreMap == null) {
            initialStoreMap = new HashMap<>();
            Arrays.asList(initialStoreList.split(",")).forEach(s -> {
                initialStoreMap.put(s, s);
            });
        }
        return initialStoreMap;
    }

    /**
     * 初始分区数量
     * Store数量 * 每Store最大副本数 /每分区副本数
     *
     * @return
     */
    public int getInitialPartitionCount() {
        return getInitialStoreMap().size() * partition.getMaxShardsPerStore()
               / partition.getShardCount();
    }

    public ConfigService getConfigService() {
        return configService;
    }

    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    public IdService getIdService() {
        return idService;
    }

    public void setIdService(IdService idService) {
        this.idService = idService;
    }

    @Data
    @Configuration
    public class ThreadPoolGrpc {
        @Value("${thread.pool.grpc.core:600}")
        private int core;
        @Value("${thread.pool.grpc.max:1000}")
        private int max;
        @Value("${thread.pool.grpc.queue:" + Integer.MAX_VALUE + "}")
        private int queue;
    }

    @Data
    @Configuration
    public class Raft {
        @Value("${raft.enable:true }")
        private boolean enable;
        @Value("${raft.address}")
        private String address;
        @Value("${pd.data-path}")
        private String dataPath;
        @Value("${raft.peers-list}")
        private String peersList;
        @Value("${raft.snapshotInterval: 300}")
        private int snapshotInterval;
        @Value("${raft.rpc-timeout:10000}")
        private int rpcTimeout;
        @Value("${grpc.host}")
        private String host;
        @Value("${server.port}")
        private int port;

        @Value("${pd.cluster_id:1}")
        private long clusterId;   // 集群ID
        @Value("${grpc.port}")
        private int grpcPort;

        public String getGrpcAddress() {
            return host + ":" + grpcPort;
        }
    }

    @Data
    @Configuration
    public class Store {
        // store 心跳超时时间
        @Value("${store.keepAlive-timeout:300}")
        private long keepAliveTimeout = 300;
        @Value("${store.max-down-time:1800}")
        private long maxDownTime = 1800;

        @Value("${store.monitor_data_enabled:true}")
        private boolean monitorDataEnabled = true;

        @Value("${store.monitor_data_interval: 1 minute}")
        private String monitorDataInterval = "1 minute";

        @Value("${store.monitor_data_retention: 1 day}")
        private String monitorDataRetention = "1 day";

        /**
         * interval -> seconds.
         * minimum value is 1 seconds.
         *
         * @return the seconds of the interval
         */
        public Long getMonitorInterval() {
            return parseTimeExpression(this.monitorDataInterval);
        }

        /**
         * the monitor data that saved in rocksdb, will be deleted
         * out of period
         *
         * @return the period of the monitor data should keep
         */
        public Long getRetentionPeriod() {
            return parseTimeExpression(this.monitorDataRetention);
        }

        /**
         * parse time expression , support pattern:
         * [1-9][ ](second, minute, hour, day, month, year)
         * unit could not be null, the number part is 1 by default.
         *
         * @param exp
         * @return seconds value of the expression. 1 will return by illegal expression
         */
        private Long parseTimeExpression(String exp) {
            if (exp != null) {
                Pattern pattern = Pattern.compile(
                    "(?<n>(\\d+)*)(\\s)*(?<unit>(second|minute|hour|day|month|year)$)");
                Matcher matcher = pattern.matcher(exp.trim());
                if (matcher.find()) {
                    String n = matcher.group("n");
                    String unit = matcher.group("unit");

                    if (null == n || n.length() == 0) {
                        n = "1";
                    }

                    Long interval;
                    switch (unit) {
                        case "minute":
                            interval = 60L;
                            break;
                        case "hour":
                            interval = 3600L;
                            break;
                        case "day":
                            interval = 86400L;
                            break;
                        case "month":
                            interval = 86400L * 30;
                            break;
                        case "year":
                            interval = 86400L * 365;
                            break;
                        case "second":
                        default:
                            interval = 1L;
                    }
                    // avoid n == '0'
                    return Math.max(1L, interval * Integer.parseInt(n));
                }
            }
            return 1L;
        }

    }

    @Data
    @Configuration
    public class Partition {
        private int totalCount = 0;

        // 每个Store最大副本数
        @Value("${partition.store-max-shard-count:24}")
        private int maxShardsPerStore = 24;

        // 默认分副本数量
        @Value("${partition.default-shard-count:3}")
        private int shardCount = 3;

        public int getTotalCount() {
            if (totalCount == 0) {
                totalCount = getInitialPartitionCount();
            }
            return totalCount;
        }

        public void setTotalCount(int totalCount) {
            this.totalCount = totalCount;
        }
    }

    @Data
    @Configuration
    public class Discovery {
        // 客户端注册后，无心跳最长次数，超过后，之前的注册信息会被删除
        @Value("${discovery.heartbeat-try-count:3}")
        private int heartbeatOutTimes = 3;
    }

}
