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

import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.baidu.hugegraph.kafka.BrokerConfig;
import com.baidu.hugegraph.kafka.topic.SyncConfTopic;
import com.baidu.hugegraph.kafka.topic.SyncConfTopicBuilder;
import com.baidu.hugegraph.meta.MetaManager;
import com.google.common.base.Strings;

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
            String key = entry.getKey();
            String value = entry.getValue();

            if (key.contains(MetaManager.META_PATH_TASK)
                || key.contains(MetaManager.META_PATH_TASK_LOCK)
                || key.contains(MetaManager.META_PATH_KAFKA)
                || key.contains(MetaManager.META_PATH_DDS)
            ) {
                return;
            }

            String graphSpace = null;

            // Check if graph is filtered, If it is true skip
            if (key.contains(MetaManager.META_PATH_GRAPH)) {
                List<String> info = manager.extractGraphFromKey(key);
                graphSpace = info.get(0);
                String graph = info.get(1);
                if (BrokerConfig.getInstance().graphFiltered(graphSpace, graph)) {
                    return;
                }
            }

            if (key.contains(MetaManager.META_PATH_GRAPHSPACE)) {
                if (Strings.isNullOrEmpty(graphSpace)) {
                    graphSpace = manager.extractGraphSpaceFromKey(key);
                }
                if (BrokerConfig.getInstance().graphSpaceFiltered(graphSpace)) {
                    return;
                }
            }

            SyncConfTopic topic = new SyncConfTopicBuilder()
                                            .setKey(key)
                                            .setValue(value)
                                            .build();
            this.produce(topic);
        });
    }
}
