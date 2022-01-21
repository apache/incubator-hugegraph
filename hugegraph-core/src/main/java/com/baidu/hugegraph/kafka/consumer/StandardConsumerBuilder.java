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

package com.baidu.hugegraph.kafka.consumer;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class StandardConsumerBuilder extends ConsumerBuilder<String, ByteBuffer> {

    public StandardConsumerBuilder() {
        this.keyDeserializer = StringDeserializer.class;
        this.valueDeserializer = ByteBufferDeserializer.class;
    }

    @Override
    @Deprecated
    public ConsumerBuilder<String ,ByteBuffer> setKeyDeserializerClass(Class<?> clazz) {
        return this;
    }

    @Override
    @Deprecated
    public ConsumerBuilder<String ,ByteBuffer> setValueDeserializerClass(Class<?> clazz) {
        return this;
    }

    @Override
    public StandardConsumer build() {

        Properties props = new Properties();

        String bootStrapServer = this.kafkaHost + ":" + this.kafkaPort;

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.valueDeserializer.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.keyDeserializer.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "hugegraph-sync-consumer-group");
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "hugegraph-sync-consumer-instance-1");
        props.put("topic", topic);

        StandardConsumer consumer = new StandardConsumer(props);
        return consumer;
    }
    
}
