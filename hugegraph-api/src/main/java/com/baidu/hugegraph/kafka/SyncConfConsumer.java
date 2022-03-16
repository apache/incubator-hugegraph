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

package com.baidu.hugegraph.kafka;

import java.util.List;
import java.util.Properties;

import com.baidu.hugegraph.kafka.consumer.ConsumerClient;
import com.baidu.hugegraph.kafka.topic.SyncConfTopicBuilder;
import com.baidu.hugegraph.meta.MetaManager;
import com.baidu.hugegraph.syncgateway.SyncMutationClient;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author Scorpiour
 * @since 2022-02-02
 */
public class SyncConfConsumer extends ConsumerClient<String, String> {

    private final SyncMutationClient client = ClientFactory.getInstance().getSyncMutationClient();
    private final MetaManager manager = MetaManager.instance();

    protected SyncConfConsumer(Properties props) {
        super(props);
    }

    protected final BrokerConfig config = BrokerConfig.getInstance();

    @Override
    protected boolean handleRecord(ConsumerRecord<String, String> record) {
        if (BrokerConfig.getInstance().needKafkaSyncStorage()) {
            List<String> etcdKV = SyncConfTopicBuilder.extractKeyValue(record);
            String etcdKey = etcdKV.get(0);
            String etcdVal = etcdKV.get(1);
            if (config.isMaster()) {
                String space = config.getConfPrefix();
                client.sendMutation(space, etcdKey, etcdVal.getBytes());
            } else if (config.isSlave()) {
                 manager.putOrDeleteRaw(etcdKey, etcdVal);
            }
            return true;
        }
        return false;
    }
    
}
