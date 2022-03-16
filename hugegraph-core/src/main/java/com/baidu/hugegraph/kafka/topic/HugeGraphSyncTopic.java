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

package com.baidu.hugegraph.kafka.topic;

import java.nio.ByteBuffer;

import com.baidu.hugegraph.backend.store.BackendMutation;

/**
 * HugeGraphSyncTopic is used to sync mutations from master to slave.
 * This topic should be produced in Master cluster, and then consumers
 * push the data to slave
 * @author Scorpiour
 * @since 2022-01-18
 */
public class HugeGraphSyncTopic extends TopicBase<String, ByteBuffer> {

    public final static String TOPIC = "hugegraph-sync-command";
    protected BackendMutation mutation;
    
    protected HugeGraphSyncTopic(String key, ByteBuffer value, int partition) {
        super(key, value, TOPIC, partition);
    }

    public BackendMutation getMutation() {
        return this.mutation;
    }
}
