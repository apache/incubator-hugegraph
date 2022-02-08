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

import com.baidu.hugegraph.kafka.producer.ProducerBuilder;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author Scorpiour
 * @since 2022-02-02
 */
public class SyncConfProducerBuilder extends ProducerBuilder<String, String> {

    private static SyncConfProducer producer;
    private static final Object MTX = new Object();

    public SyncConfProducerBuilder() {
        super();

        this.kafkaHost = BrokerConfig.getInstance().getKafkaHost();
        this.kafkaPort = BrokerConfig.getInstance().getKafkaPort();
        this.keySerializer = StringSerializer.class;
        this.valueSerializer = StringSerializer.class;

    }

    @Override
    @Deprecated
    public ProducerBuilder<String, String> setKafkaHost(String host) {
        return this;
    }

    @Override
    @Deprecated
    public ProducerBuilder<String, String> setKafkaPort(String host) {
        return this;
    }

    /**
     * Disable keySerializerSettings, it should not be used in this class
     */
    @Override
    @Deprecated
    public ProducerBuilder<String, String> setKeySerializerClass(Class<?> clazz) {
        return this;
    }

    @Override
    @Deprecated
    public ProducerBuilder<String, String> setValueSerializerClass(Class<?> clazz) {
        return this;
    }

    @Override
    @Deprecated
    public ProducerBuilder<String, String> setTopic(String topic) {
        return this;
    }

    @Override
    public SyncConfProducer build() throws IllegalArgumentException {
        synchronized(SyncConfProducerBuilder.MTX) {
            if (null == SyncConfProducerBuilder.producer) {
                this.validateOptions();

                Properties props = new Properties();

                String bootStrapServer = this.kafkaHost + ":" + this.kafkaPort;

                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.valueSerializer.getName());
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.keySerializer.getName());
                props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

                SyncConfProducer producer = new SyncConfProducer(props);
                SyncConfProducerBuilder.producer = producer;
            }
        }

        return SyncConfProducerBuilder.producer;
    }
    
}
