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

    // Asynchronous task execution interval, in seconds
    private final int taskScheduleTime = 60;
    // Splitting process, waiting for data alignment timeout
    private final long splitPartitionTimeout = 30 * 60 * 1000;
    HgStoreEngineOptions.RaftOptions raftOptions;
    // raft storage path
    private String raftDataPath;
    private String raftSnapShotPath;
    private Integer groupId;
    private String raftAddress;
    private List<String> peerList;
    private Configuration conf;
    // raft task processor
    private RaftTaskHandler taskHandler;
}
