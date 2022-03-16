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

import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.raft.StoreSerializer;
import com.baidu.hugegraph.kafka.BrokerConfig;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class HugeGraphMutateTopicBuilder {
    
    private String graphName;
    private String graphSpace;

    private ByteBuffer buffer;


    private static final int PARTITION_COUNT = BrokerConfig.getInstance().getPartitionCount();

    private final static String DELIM = "/";

    public HugeGraphMutateTopicBuilder() {

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

    public HugeGraphMutateTopicBuilder setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
        return this;
    }

    public HugeGraphMutateTopicBuilder setGraphName(String graphName) {
        this.graphName = graphName;
        return this;
    }

    public HugeGraphMutateTopicBuilder setGraphSpace(String graphSpace) {
        this.graphSpace = graphSpace;
        return this;
    }

    public HugeGraphMutateTopic build() {
        String key = this.makeKey();
        HugeGraphMutateTopic topic = new HugeGraphMutateTopic(key, buffer, this.calcPartition());
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
            throw new InvalidParameterException("invalid record key of MutateTopic: " + record.key());
        }
        return keys;
    }

    public static BackendMutation buildMutation(ByteBuffer recordBuffer) {

        BytesBuffer buffer = BytesBuffer.wrap(recordBuffer);
        BackendMutation mutation = StoreSerializer.readMutation(buffer);

        return mutation;
    }
}
