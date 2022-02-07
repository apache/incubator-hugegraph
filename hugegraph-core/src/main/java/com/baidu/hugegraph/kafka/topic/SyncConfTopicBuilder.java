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

import java.security.InvalidParameterException;

import com.baidu.hugegraph.kafka.BrokerConfig;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SyncConfTopicBuilder {

    private String graphName;
    private String graphSpace;
    private String conf;

    private static final int PARTITION_COUNT = BrokerConfig.getInstance().getPartitionCount();

    private static final String DELIM = "/";

    public SyncConfTopicBuilder() {

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

    public SyncConfTopicBuilder setGraphName(String graphName) {
        this.graphName = graphName;
        return this;
    }

    public SyncConfTopicBuilder setGraphSpace(String graphSpace) {
        this.graphSpace = graphSpace;
        return this;
    }

    public SyncConfTopicBuilder setConfigStr(String conf) {
        this.conf = conf;
        return this;
    }

    public SyncConfTopic build() {
        String key = this.makeKey();
        SyncConfTopic topic = new SyncConfTopic(key, conf, this.calcPartition());
        return topic;
    }

    /**
     * Extract graphSpace and graphName
     * @param record
     * @return [{graphSpace}, {graphName}]
     */
    public static String[] extractGraphs(ConsumerRecord<String, String> record) {
        String[] keys = record.key().split(DELIM);
        if (keys.length != 2) {
            throw new InvalidParameterException("invalid record key: " + record.key());
        }
        return keys;
    }
    
}
