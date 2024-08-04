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

import java.util.List;

import org.apache.hugegraph.store.raft.RaftTaskHandler;

import com.alipay.sofa.jraft.conf.Configuration;

import lombok.Data;

/**
 * Partition engine configuration
 */
@Data
public class PartitionEngineOptions {

    // Asynchronous task execution time interval, unit seconds
    private final int taskScheduleTime = 60;
    // The split process, wait for the data to go overtime time
    private final long splitPartitionTimeout = 30 * 60 * 1000;
    HgStoreEngineOptions.RaftOptions raftOptions;
    // RAFT storage path
    private String raftDataPath;
    private String raftSnapShotPath;
    private Integer groupId;
    private String raftAddress;
    private List<String> peerList;
    private Configuration conf;
    // RAFT task processor
    private RaftTaskHandler taskHandler;
}
