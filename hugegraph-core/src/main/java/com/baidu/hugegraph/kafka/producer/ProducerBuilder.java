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

import java.util.Properties;

import com.google.common.base.Strings;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Kafka producer builder
 * @author Scorpiour
 * @since 2022-01-18
 */
public class ProducerBuilder<K, V> {

    /**
     * Bootstrap broker host
     */
    protected String kafkaHost = "";
    protected String kafkaPort = "9092";
    protected String topic;
    protected Class<?> keySerializer = StringSerializer.class;
    protected Class<?> valueSerializer = StringSerializer.class;

    protected void validateOptions() throws IllegalArgumentException {
        if (Strings.isNullOrEmpty(kafkaHost)) {
            throw new IllegalArgumentException("Missing kafka host");
        }
        if (Strings.isNullOrEmpty(kafkaPort)) {
            throw new IllegalArgumentException("Missing kafka port");
        }
    }

    public ProducerBuilder<K, V> setKafkaHost(String host) {
        if (!Strings.isNullOrEmpty(host)) {
            this.kafkaHost = host;
        }
        return this;
    }

    public ProducerBuilder<K, V> setKafkaPort(String port) {
        if (!Strings.isNullOrEmpty(port)) {
            this.kafkaPort = port;
        }
        return this;
    }

    public ProducerBuilder<K, V> setKeySerializerClass(Class<?> clazz) {
        this.keySerializer = clazz;
        return this;
    }

    public ProducerBuilder<K, V> setValueSerializerClass(Class<?> clazz) {
        this.valueSerializer = clazz;
        return this;
    }

    public ProducerBuilder<K, V> setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public ProducerClient<K, V> build() throws IllegalArgumentException {

        this.validateOptions();

        Properties props = new Properties();

        String bootStrapServer = this.kafkaHost + ":" + this.kafkaPort;

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.valueSerializer.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.keySerializer.getName());

        ProducerClient<K, V> client = new ProducerClient<>(props);

        return client;
    }
    
}
