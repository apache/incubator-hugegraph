<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java
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

package org.apache.hugegraph.pd.config;
========
package org.apache.hugegraph.pd.config;

import static org.apache.hugegraph.pd.common.Consts.DEFAULT_STORE_GROUP_ID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java

import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.IdService;

import lombok.Data;
import lombok.Getter;

import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.IdService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import lombok.Data;

/**
 * PD profile
 */
@Data
@Component
public class PDConfig {

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java
    // cluster ID
========
    private static String[] storeInfo = {"store",
                                         "$2a$04$9ZGBULe2vc73DMj7r/iBKeQB1SagtUXPrDbMmNswRkTwlWQURE/Jy",
                                         "E3UnnQa605go"};
    private static String[] serverInfo = {"hg",
                                          "$2a$04$i10KooNg6wLvIPVDh909n.RBYlZ/4pJo978nFK86nrqQiGIKV4UGS",
                                          "qRyYhxVAWDb5"};
    private static String[] hubbleInfo = {"hubble",
                                          "$2a$04$pSGkohaywGgFrJLr6VOPm.IK2WtOjlNLcZN8gct5uIKEDO1I61DGa",
                                          "iMjHnUl5Pprx"};
    private static String[] vermeer = {"vermeer",
                                       "$2a$04$N89qHe0v5jqNJKhQZHnTdOFSGmiNoiA2B2fdWpV2BwrtJK72dXYD.",
                                       "FqU8BOvTpteT"};
    private static String[][] infos = new String[][]{storeInfo, serverInfo, hubbleInfo, vermeer};

    @Getter
    private static List<Server> defaultServers;

    static {
        defaultServers = new ArrayList<>(infos.length);
        for (String[] info : infos) {
            defaultServers.add(new Server(info[0], info[1], info[2]));
        }
    }

>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java
    @Value("${pd.cluster_id:1}")
    private long clusterId;

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java
    // The patrol task interval
========
    // 巡查任务时间间隔
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java
    @Value("${pd.patrol-interval:300}")
    private long patrolInterval = 300;
    @Value("${pd.data-path}")
    private String dataPath;
    @Value("${pd.initial-store-count:3}")
    private int minStoreCount;

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java
    // The initial store list, within which the store is automatically activated
========
    // 初始store列表，该列表内的store自动激活
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java
    // format: store_addresss, store_address, store_address/group_id, store_address/group_id
    @Value("${pd.initial-store-list: ''}")
    private String initialStoreList;
    @Value("${grpc.host}")
    private String host;

    @Value("${license.verify-path}")
    private String verifyPath;
    @Value("${license.license-path}")
    private String licensePath;
    @Autowired
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java
    private ThreadPoolGrpc threadPoolGrpc;
========
    private JobConfig jobConfig;

    @Autowired
    private ThreadPoolGrpc threadPoolGrpc;

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

    @Value("${auth.secret-key: 'FXQXbJtbCLxODc6tGci732pkH1cyf8Qg'}")
    private String secretKey;

>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java
    @Autowired
    private Raft raft;
    @Autowired
    private Store store;
    @Autowired
    private Partition partition;
    @Autowired
    private Discovery discovery;
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java
    private Map<String, String> initialStoreMap = null;
    private ConfigService configService;
    private IdService idService;
========

    private volatile Map<String, String> initialStoreMap = null;
    private volatile Map<String, Integer> initialStoreGroupMap = null;
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java

    public Map<String, String> getInitialStoreMap() {
        if (initialStoreMap == null) {
            initialStoreMap = new ConcurrentHashMap<>();
            Arrays.asList(initialStoreList.split(",")).forEach(s -> {
                String[] arr = s.split("/");
                initialStoreMap.put(arr[0], arr[0]);
            });
        }
        return initialStoreMap;
    }

    public int getInitialStoreGroup(String address) {
        if (initialStoreGroupMap == null) {
            synchronized (this) {
                if (initialStoreGroupMap == null) {
                    initialStoreGroupMap = new ConcurrentHashMap<>();
                    Arrays.asList(initialStoreList.split(",")).forEach(s -> {
                        String[] arr = s.split("/");
                        if (arr.length == 2) {
                            initialStoreGroupMap.put(arr[0], Integer.parseInt(arr[1]));
                        } else {
                            initialStoreGroupMap.put(arr[0], DEFAULT_STORE_GROUP_ID);
                        }
                    });
                }
            }
        }
        return initialStoreGroupMap.getOrDefault(address, DEFAULT_STORE_GROUP_ID);
    }

    /**
     * The initial number of partitions
     * Number of Stores * Maximum number of replicas per Store / Number of replicas per partition
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
        private long clusterId;
        @Value("${grpc.port}")
        private int grpcPort;

        public String getGrpcAddress() {
            return host + ":" + grpcPort;
        }
    }

    @Data
    @Configuration
    public class Store {

        // store Heartbeat timeout
        @Value("${store.keepAlive-timeout:300}")
        private long keepAliveTimeout = 300;
        @Value("${store.max-down-time:1800}")
        private long maxDownTime = 1800;

        @Value("${store.monitor_data_enabled:false}")
        private boolean monitorDataEnabled = false;

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
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java
    public class Partition {

        private int totalCount = 0;
========
    public class Partition{
//        private int totalCount = 0;
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java

        // Maximum number of replicas per Store
        @Value("${partition.store-max-shard-count:24}")
        private int maxShardsPerStore = 24;

        @Value("${partition.default-shard-count:3}")
        private int shardCount = 3;

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java
        public int getTotalCount() {
            if (totalCount == 0) {
                totalCount = getInitialPartitionCount();
            }
            return totalCount;
        }

        public void setTotalCount(int totalCount) {
            this.totalCount = totalCount;
        }
========
//        public void setTotalCount(int totalCount){
//            this.totalCount = totalCount;
//        }
//
//        public int getTotalCount() {
//            if ( totalCount == 0 ) {
//                totalCount = getInitialPartitionCount();
//            }
//            return totalCount;
//        }
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java
    }

    @Data
    @Configuration
    public class Discovery {

        // After the client registers, the maximum number of heartbeats is not reached, and after
        // that, the previous registration information will be deleted
        @Value("${discovery.heartbeat-try-count:3}")
        private int heartbeatOutTimes = 3;
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java
========
    @Data
    @Configuration
    public class JobConfig {
        @Value("${job.interruptableThreadPool.core:0}")
        private int core;
        @Value("${job.interruptableThreadPool.max:256}")
        private int max;
        @Value("${job.interruptableThreadPool.queue:" + Integer.MAX_VALUE + "}")
        private int queueSize;
        @Value("${job.start-time:19}")
        private int startTime;
        @Value("${job.uninterruptibleThreadPool.core:0}")
        private int uninterruptibleCore;
        @Value("${job.uninterruptibleThreadPool.max:256}")
        private int uninterruptibleMax;
        @Value("${job.uninterruptibleThreadPool.queue:" + Integer.MAX_VALUE + "}")
        private int uninterruptibleQueueSize;
    }


    @Data
    @Configuration
    @ConfigurationProperties(prefix = "pd")
    public class Servers {
        List<Server> servers;

        public List<Server> getServers() {
            if (CollectionUtils.isEmpty(servers)) {
                return defaultServers;
            }
            return servers;
        }
    }

    @Value("${pd.allows-address-acquisition: false}")
    private boolean allowsAddressAcquisition = false;

    @Getter
    private ConfigService configService;

    @Getter
    private IdService   idService;

    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    public void setIdService(IdService idService) {
        this.idService = idService;
    }

>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/config/PDConfig.java
}
