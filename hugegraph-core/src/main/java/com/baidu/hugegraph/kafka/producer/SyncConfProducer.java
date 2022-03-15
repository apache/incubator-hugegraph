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

package com.baidu.hugegraph.kafka.producer;

import java.util.Map;
import java.util.Properties;

import com.baidu.hugegraph.kafka.BrokerConfig;
import com.baidu.hugegraph.kafka.topic.SyncConfTopic;
import com.baidu.hugegraph.kafka.topic.SyncConfTopicBuilder;
import com.baidu.hugegraph.meta.MetaManager;

/**
 * Sync etcd service conf from master to slave
 */
public class SyncConfProducer extends ProducerClient<String, String> {

    private final MetaManager manager = MetaManager.instance();

    protected SyncConfProducer(Properties props) {
        super(props);

        if (BrokerConfig.getInstance().isMaster()) {
            manager.listenPrefix(MetaManager.META_PATH_HUGEGRAPH, this::listenEtcdChanged);
        }
    }

    private <T> void listenEtcdChanged(T response) {
        Map<String, String> map = manager.extractKVFromResponse(response);
        map.entrySet().forEach((entry) -> {
            SyncConfTopic topic = new SyncConfTopicBuilder().setKey(entry.getKey()).setValue(entry.getValue()).build();
            this.produce(topic);
        });
    }
}
