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
import java.security.InvalidParameterException;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.raft.StoreSerializer;
import com.baidu.hugegraph.kafka.BrokerConfig;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.tinkerpop.gremlin.GraphManager;



public class HugeGraphSyncTopicBuilder {

    private String graphName;
    private String graphSpace;

    private BackendMutation mutation;


    private static final int PARTITION_COUNT = BrokerConfig.getInstance().getPartitionCount();

    private final static String DELIM = "/";

    public HugeGraphSyncTopicBuilder() {

    }

    private String makeKey() {
        // HUGEGRAPH/{graphSpace}/{graphName}
        return String.join(DELIM, this.graphSpace, this.graphName);
    }

    /**
     * 使用graph的hashCode来计算partition，确保一个graph总在同一个partition内
     * @return
     */
    private int calcPartition() {
        int code = this.graphName.hashCode() % PARTITION_COUNT;
        if (code < 0) {
            code = -code;
        }
        return code;
    }

    public HugeGraphSyncTopicBuilder setMutation(BackendMutation mutation) {
        this.mutation = mutation;
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

        byte[] value = StoreSerializer.writeMutation(mutation);
        ByteBuffer buffer = ByteBuffer.wrap(value);
        HugeGraphSyncTopic topic = new HugeGraphSyncTopic(key, buffer, this.calcPartition());

        return topic;
    }

    /**
     * Extract graphSpace and graphName
     * @param record
     * @return [{graphSpace}, {graphName}]
     */
    public static String[] extractGraphs(ConsumerRecord<String, ByteBuffer> record) {
        String[] keys = record.key().split(DELIM);
        if (keys.length != 2) {
            throw new InvalidParameterException("invalid record key: " + record.key());
        }
        return keys;
    }

    public static BackendMutation buildMutation(ByteBuffer recordBuffer) {

        BytesBuffer buffer = BytesBuffer.wrap(recordBuffer);
        BackendMutation mutation = StoreSerializer.readMutation(buffer);

        return mutation;
    }
}
