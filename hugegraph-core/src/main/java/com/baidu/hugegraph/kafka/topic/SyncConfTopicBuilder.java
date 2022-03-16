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

import java.util.Arrays;
import java.util.List;

import com.baidu.hugegraph.kafka.BrokerConfig;

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

    private String key;
    private String value;

    public SyncConfTopicBuilder() {

    }

    private String makeKey() {
        // SYNC_CONF/{key}
        return String.join(DELIM, PREFIX, this.key);
    }

    /**
     * 使用graph的hashCode来计算partition，确保一个graph总在同一个partition内
     * @return
     */
    private int calcPartition() {
        return 0;
    }

    public SyncConfTopicBuilder setKey(String key) {
        assert Strings.isNotBlank(key);
        this.key = key;
        return this;
    }

    public SyncConfTopicBuilder setValue(String value) {
        this.value = value;
        return this;
    }

    public SyncConfTopic build() {
        String key = this.makeKey();
        SyncConfTopic topic = new SyncConfTopic(key, this.value, this.calcPartition());

        return topic;
    }

    public static List<String> extractKeyValue(ConsumerRecord<String, String> record) {
        String key = record.key();
        String value = record.value();

        int subLen = PREFIX.length();

        String etcdKey = key.substring(subLen + 1);
        return Arrays.asList(etcdKey, value);
    }
    
}
