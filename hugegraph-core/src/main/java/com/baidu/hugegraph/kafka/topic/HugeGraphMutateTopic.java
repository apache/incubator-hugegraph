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

import com.baidu.hugegraph.HugeGraphParams;

/**
 * HugeGraphMutateTopic is used to apply mutate to storage.
 * In the other word, it is the data synchronized from master.
 * This topic should be consumed in the Slave clusters
 * @author Scorpiour
 * @since 2022-01-22
 */
public class HugeGraphMutateTopic extends TopicBase<String, ByteBuffer> {
    public final static String TOPIC = "hugegraph-mutate-command";
    protected ByteBuffer buffer;
    protected HugeGraphParams graph;

    protected HugeGraphMutateTopic(String key, ByteBuffer value, int partition) {
        super(key, value, TOPIC, partition);
    }

    public HugeGraphParams getGraph() {
        return this.graph;
    }
}
