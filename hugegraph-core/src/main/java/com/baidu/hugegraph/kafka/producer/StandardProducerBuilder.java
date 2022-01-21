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

import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Standard producer builder to make producer
 */
public class StandardProducerBuilder extends ProducerBuilder<String, ByteBuffer>  {
    public StandardProducerBuilder() {
        super();
        this.keySerializer = StringSerializer.class;
        this.valueSerializer = ByteBufferSerializer.class;
    }

    /**
     * Disable keySerializerSettings, it should not be used in this class
     */
    @Override
    @Deprecated
    public ProducerBuilder<String, ByteBuffer> setKeySerializerClass(Class<?> clazz) {
        return this;
    }

    @Override
    public ProducerBuilder<String, ByteBuffer> setValueSerializerClass(Class<?> clazz) {
        return this;
    }

    @Override
    public ProducerClient<String, ByteBuffer> build() throws IllegalArgumentException {
        this.validateOptions();

        Properties props = new Properties();

        String bootStrapServer = this.kafkaHost + ":" + this.kafkaPort;

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.valueSerializer.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.keySerializer.getName());
        props.put("topic", topic);
        
        ProducerClient<String, ByteBuffer> producer = new ProducerClient<>(props);

        return producer;

    }

}
