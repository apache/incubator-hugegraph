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

package org.apache.hugegraph.store.options;

import java.util.Map;

import org.apache.hugegraph.store.business.DataMover;
import org.apache.hugegraph.store.pd.PdProvider;
import org.apache.hugegraph.store.raft.RaftTaskHandler;

import com.alipay.sofa.jraft.util.Utils;

import lombok.Data;

/**
 * Storage engine configuration
 */
@Data
public class HgStoreEngineOptions {
    public static String Raft_Path_Prefix = "raft";
    public static String DB_Path_Prefix = "db";
    public static String Snapshot_Path_Prefix = "snapshot";
    // pd 服务器地址
    private String pdAddress;
    // 对外服务地址
    private String grpcAddress;
    // Raft 对外服务地址
    private String raftAddress;
    // 存储路径，支持多个位置，逗号分割
    private String dataPath;

    private String raftPath;

    private Map<String, Object> rocksdbConfig;
    // store心跳间隔，单位秒
    private final int storeHBInterval = 30;
    // 分区心跳间隔，单位秒
    private final int partitionHBInterval = 5;
    // 等待leader超时时间，单位秒
    private final int waitLeaderTimeout = 30;
    // 没有PD模式，用于开发调试使用
    private final boolean fakePD = false;

    private final int raftRpcThreadPoolSize = Utils.cpus() * 6;
    // 自定义的标签，传给pd
    private Map<String, String> labels;
    // fakePd配置项
    private final FakePdOptions fakePdOptions = new FakePdOptions();
    private final RaftOptions raftOptions = new RaftOptions();

    // Raft任务处理器
    private RaftTaskHandler taskHandler;

    private PdProvider pdProvider;

    // 数据迁移服务
    private DataMover dataTransfer;

    @Data
    public static class FakePdOptions {
        private String storeList;
        private String peersList;
        private final int partitionCount = 0;
        private final int shardCount = 0;
    }

    @Data
    public static class RaftOptions {

        /*
         * Rpc connect timeout in milliseconds
         * The time should be less than electionTimeoutMs, otherwise the election will timeout
         */
        private final int rpcConnectTimeoutMs = 1000;

        /**
         * RPC request default timeout in milliseconds
         */
        private final int rpcDefaultTimeout = 5000;

        // A follower would become a candidate if it doesn't receive any message
        // from the leader in |election_timeout_ms| milliseconds

        private final int electionTimeoutMs = 3000;
        /**
         * Install snapshot RPC request default timeout in milliseconds
         */
        private final int rpcInstallSnapshotTimeout = 60 * 60 * 1000;
        // A snapshot saving would be triggered every |snapshot_interval_s| seconds
        // if this was reset as a positive number
        // If |snapshot_interval_s| <= 0, the time based snapshot would be disabled.
        //
        // Default: 3600 (1 hour)
        private final int snapshotIntervalSecs = 3600;
        // A snapshot saving would be triggered every |snapshot_interval_s| seconds,
        // and at this moment when state machine's lastAppliedIndex value
        // minus lastSnapshotId value is greater than snapshotLogIndexMargin value,
        // the snapshot action will be done really.
        // If |snapshotLogIndexMargin| <= 0, the distance based snapshot would be disable.
        //
        // Default: 0
        private final int snapshotLogIndexMargin = 1024;
        private final boolean metrics = true;
        // 等待leader超时时间，单位秒
        private final int waitLeaderTimeout = 30;
        /**
         * Internal disruptor buffers size for Node/FSMCaller/LogManager etc.
         */
        private final int disruptorBufferSize = 4096;
        /**
         * The maximum number of entries in AppendEntriesRequest
         */
        private final int maxEntriesSize = 256;
        /**
         * The maximum replicator pipeline in-flight requests/responses, only valid when enable
         * replicator pipeline.
         */
        private final int maxReplicatorInflightMsgs = 256;
        /**
         * Raft集群发生数据积压后，限速等待时间 单位毫秒
         **/
        private final int overloadRateLimit = 100;

        /**
         * The maximum byte size of log allowed by user.
         */
        private final long maxLogSize = 100 * 1024 * 1024;
        /**
         * The ratio of exponential approximation for average size of log entry.
         */
        private final double aveLogEntrySizeRatio = 0.95;

        private final boolean useRocksDBSegmentLogStorage = true;
        private final int maxSegmentFileSize = 64 * 1024 * 1024;
        private final int keepInMemorySegmentCount = 2;
        private final int preAllocateSegmentCount = 1;
        private final int splitPartitionLogIndexMargin = 10;
    }
}
