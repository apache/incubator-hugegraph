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

import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.backend.store.raft.rpc.RaftRequests.StoreAction;
import com.baidu.hugegraph.backend.store.raft.rpc.RaftRequests.StoreType;

public class HugeGraphSyncTopicBuilder {

    private StoreAction action;
    private BytesBuffer buffer;
    private StoreType storeType;
    private String graphName;
    private String graphSpace;

    private final static String DELIM = "/";

    public HugeGraphSyncTopicBuilder() {

    }

    private String makeKey() {
        // HUGEGRAPH/{graphSpace}/{graphName}/{storeType}/{actionType}
        return String.join(DELIM, this.graphSpace, this.graphName, this.storeType.name(), this.action.name());

    }

    public HugeGraphSyncTopicBuilder setStoreType(StoreType storeType) {
        this.storeType = storeType;
        return this;
    }

    public HugeGraphSyncTopicBuilder setAction(StoreAction action) {
        this.action = action;
        return this;
    }

    public HugeGraphSyncTopicBuilder setBuffer(BytesBuffer buffer) {
        this.buffer = buffer;
        return this;
    }

    public HugeGraphSyncTopicBuilder setGraphName(String graphName) {
        this.graphName = graphName;
        return this;
    }

    public HugeGraphSyncTopicBuilder setGraphSpace(String graphSpace) {
        this.graphSpace = graphSpace;
        return this;
    }

    public HugeGraphSyncTopic build() {

        String key = this.makeKey();

        HugeGraphSyncTopic topic = new HugeGraphSyncTopic(key, buffer.bytes(), 0);

        return topic;
    }
}
