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

import java.util.Properties;

import com.google.common.base.Strings;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Kafka consumer builder
 * @author Scorpiour
 * @since 2022-01-18
 */
public class ConsumerBuilder<K, V> {

    /**
     * Bootstrap broker host
     */
    protected String kafkaHost = "";
    protected String kafkaPort = "9092";
    protected String topic;
    protected String groupId;
    protected String groupInstanceId;
    protected Class<?> keyDeserializer = StringDeserializer.class;
    protected Class<?> valueDeserializer = StringDeserializer.class;
    
    public ConsumerBuilder() {

    }

    protected void validateOptions() throws IllegalArgumentException {
        if (Strings.isNullOrEmpty(kafkaHost)) {
            throw new IllegalArgumentException("Missing kafka host");
        }
        if (Strings.isNullOrEmpty(kafkaPort)) {
            throw new IllegalArgumentException("Missing kafka port");
        }
        if (Strings.isNullOrEmpty(topic)) {
            throw new IllegalArgumentException("Missing topic");
        }
    }

    
    public ConsumerBuilder<K, V> setKafkaHost(String host) {
        if (!Strings.isNullOrEmpty(host)) {
            this.kafkaHost = host;
        }
        return this;
    }

    public ConsumerBuilder<K, V> setKafkaPort(String port) {
        if (!Strings.isNullOrEmpty(port)) {
            this.kafkaPort = port;
        }
        return this;
    }

    public ConsumerBuilder<K, V> setKeyDeserializerClass(Class<?> clazz) {
        this.keyDeserializer = clazz;
        return this;
    }

    public ConsumerBuilder<K, V> setValueDeserializerClass(Class<?> clazz) {
        this.valueDeserializer = clazz;
        return this;
    }

    public ConsumerBuilder<K, V> setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public ConsumerBuilder<K, V> setGroupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public ConsumerBuilder<K, V> setGroupInstanceId(String groupInstanceId) {
        this.groupInstanceId = groupInstanceId;
        return this;
    }

    
    public ConsumerClient<K, V> build() throws IllegalArgumentException {

        this.validateOptions();

        Properties props = new Properties();

        String bootStrapServer = this.kafkaHost + ":" + this.kafkaPort;

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.valueDeserializer.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.keyDeserializer.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, this.groupInstanceId);
        props.put("topic", topic);

        ConsumerClient<K, V> client = new ConsumerClient<>(props);

        return client;
    }
    
}
