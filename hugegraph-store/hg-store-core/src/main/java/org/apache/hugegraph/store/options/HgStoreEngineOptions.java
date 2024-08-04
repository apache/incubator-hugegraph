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
    // Store heartbeat interval, unit seconds
    private final int storeHBInterval = 30;
    // Partition heartbeat interval, unit seconds
    private final int partitionHBInterval = 5;
    // Waiting for the leader timeout time, the unit second
    private final int waitLeaderTimeout = 30;
    private final int raftRpcThreadPoolSize = Utils.cpus() * 6;
    // No PD mode, used to develop debugging and use
    private boolean fakePD = false;
    // Fakepd configuration item
    private FakePdOptions fakePdOptions = new FakePdOptions();
    private RaftOptions raftOptions = new RaftOptions();
    // pd server address
    private String pdAddress;
    // External service address
    private String grpcAddress;
    // RAFT's external service address
    private String raftAddress;
    // Storage path, support multiple positions, comma segmentation
    private String dataPath;
    private String raftPath;
    private Map<String, Object> rocksdbConfig;
    // The custom label is passed to PD
    private Map<String, String> labels;
    // RAFT task processor
    private RaftTaskHandler taskHandler;

    private PdProvider pdProvider;

    // Data migration service
    private DataMover dataTransfer;

    @Data
    public static class FakePdOptions {

        private int partitionCount = 0;
        private int shardCount = 0;
        private String storeList;
        private String peersList;
    }

    @Data
    public static class RaftOptions {

        /*
         * Rpc connect timeout in milliseconds
         * The time should be less than electionTimeoutMs, otherwise the election will timeout
         */
        private final int rpcConnectTimeoutMs = 1000;
        private final int electionTimeoutMs = 3000;

        // A follower would become a candidate if it doesn't receive any message
        // from the leader in |election_timeout_ms| milliseconds
        /**
         * Install snapshot RPC request default timeout in milliseconds
         */
        private final int rpcInstallSnapshotTimeout = 60 * 60 * 1000;
        // Waiting for the leader timeout time, the unit second
        private final int waitLeaderTimeout = 30;
        /**
         * The maximum number of entries in AppendEntriesRequest
         */
        private final int maxEntriesSize = 256;
        /**
         * After the data of the raft cluster occurs, after the backlog of data，限速等待时间 单位毫秒
         **/
        private final int overloadRateLimit = 100;
        private final int keepInMemorySegmentCount = 2;
        private final int preAllocateSegmentCount = 1;
        private final int splitPartitionLogIndexMargin = 10;
        /**
         * RPC request default timeout in milliseconds
         */
        private int rpcDefaultTimeout = 5000;
        // A snapshot saving would be triggered every |snapshot_interval_s| seconds
        // if this was reset as a positive number
        // If |snapshot_interval_s| <= 0, the time based snapshot would be disabled.
        //
        // Default: 3600 (1 hour)
        private int snapshotIntervalSecs = 3600;
        // A snapshot saving would be triggered every |snapshot_interval_s| seconds,
        // and at this moment when state machine's lastAppliedIndex value
        // minus lastSnapshotId value is greater than snapshotLogIndexMargin value,
        // the snapshot action will be done really.
        // If |snapshotLogIndexMargin| <= 0, the distance based snapshot would be disable.
        //
        // Default: 0
        private int snapshotLogIndexMargin = 1024;
        private boolean metrics = true;
        /**
         * Internal disruptor buffers size for Node/FSMCaller/LogManager etc.
         */
        private int disruptorBufferSize = 4096;
        /**
         * The maximum replicator pipeline in-flight requests/responses, only valid when enable
         * replicator pipeline.
         */
        private int maxReplicatorInflightMsgs = 256;
        /**
         * The maximum byte size of log allowed by user.
         */
        private long maxLogSize = 100 * 1024 * 1024;
        /**
         * The ratio of exponential approximation for average size of log entry.
         */
        private double aveLogEntrySizeRatio = 0.95;
        private boolean useRocksDBSegmentLogStorage = true;
        private int maxSegmentFileSize = 64 * 1024 * 1024;
    }
}
