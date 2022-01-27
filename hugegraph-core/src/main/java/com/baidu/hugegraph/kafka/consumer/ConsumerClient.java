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

import java.time.Duration;
import java.util.Properties;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Kafka consumer encapsulation
 * @author Scorpiour
 * @since 2022-01-18
 */
public class ConsumerClient<K, V> {

    public final String topic;

    protected final KafkaConsumer<K, V> consumer;

    protected ConsumerClient(Properties props) {
        String topic = props.getProperty("topic");
        if (Strings.isNullOrEmpty(topic)) {
            throw new InstantiationError("Topic may not be null");
        }
        this.topic = topic;
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(ImmutableList.of(topic));
    }

    public void consume() {

        ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(1000));
        if (records.count() > 0) {
           for(ConsumerRecord<K, V> record : records.records(this.topic)) {
               System.out.println(String.format("Going to consumer [%s] - %s", record.key().toString(), record.value().toString()));
               
           }
        }
        consumer.commitAsync();
    }

    public void close() {
        if (null != consumer) {
            consumer.close();
        }
    }
}
