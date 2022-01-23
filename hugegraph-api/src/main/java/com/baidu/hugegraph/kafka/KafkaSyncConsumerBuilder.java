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

import java.util.Properties;

import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.kafka.consumer.StandardConsumerBuilder;

import org.apache.kafka.clients.consumer.ConsumerConfig;

public class KafkaSyncConsumerBuilder extends StandardConsumerBuilder {

    private static GraphManager graphManager;

    public static void setGraphManager(GraphManager manager) {
        KafkaSyncConsumerBuilder.graphManager = manager;
    }
    
    @Override
    public KafkaSyncConsumer build() {
        Properties props = new Properties();

        String bootStrapServer = this.kafkaHost + ":" + this.kafkaPort;

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.valueDeserializer.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.keyDeserializer.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, this.groupInstanceId);
        props.put("topic", topic);
    
        KafkaSyncConsumer consumer = new KafkaSyncConsumer(props);
        consumer.setGraphManager(KafkaSyncConsumerBuilder.graphManager);
        return consumer;
        
    }
}
