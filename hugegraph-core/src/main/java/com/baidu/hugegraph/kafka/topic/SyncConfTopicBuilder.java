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
import java.util.Map;
import java.util.Optional;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.baidu.hugegraph.kafka.BrokerConfig;
import com.baidu.hugegraph.meta.MetaManager;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.util.Strings;

/**
 * @author Scorpiour
 * @since 2022-01-28
 */
public class SyncConfTopicBuilder {

    private static final int PARTITION_COUNT = BrokerConfig.getInstance().getPartitionCount();

    private static final String PREFIX = "SYNC_CONF";

    private static final String DELIM = "/";

    private String graphSpace;
    private String serviceName;
    private String serviceRawConfig;

    public SyncConfTopicBuilder() {

    }

    private String makeKey() {
        // SYNC_CONF/{graphSpace}/{serviceName}
        return String.join(DELIM, PREFIX, graphSpace, serviceName);
    }

    /**
     * 使用graph的hashCode来计算partition，确保一个graph总在同一个partition内
     * @return
     */
    private int calcPartition() {
        int code = this.graphSpace.hashCode() % PARTITION_COUNT;
        if (code < 0) {
            code = -code;
        }
        return code;
    }

    public SyncConfTopicBuilder setGraphSpace(String graphSpace) {
        this.graphSpace = Optional.ofNullable(graphSpace).orElse("");
        return this;
    }

    public SyncConfTopicBuilder setServiceName(String serviceName) {
        assert Strings.isNotBlank(serviceName);
        this.serviceName = serviceName;
        return this;
    }

    public SyncConfTopicBuilder setRawConfig(String rawConfig) {
        this.serviceRawConfig = rawConfig;
        return this;
    }

    public SyncConfTopic build() {
        String key = this.makeKey();
        SyncConfTopic topic = new SyncConfTopic(key, this.serviceRawConfig, this.calcPartition());

        return topic;
    }

    public static String[] extractGraphInfo(ConsumerRecord<String, String> record) {
        String[] keys = record.key().split(DELIM);
        if (keys.length < 3) {
            throw new InvalidParameterException("invalid record key of SyncConfTopic: " + record.key());
        }
        return keys;
    }
    
}
