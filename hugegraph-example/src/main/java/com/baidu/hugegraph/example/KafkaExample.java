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

package com.baidu.hugegraph.example;

import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.baidu.hugegraph.kafka.consumer.StandardConsumer;
import com.baidu.hugegraph.kafka.consumer.StandardConsumerBuilder;
import com.baidu.hugegraph.kafka.producer.ProducerClient;
import com.baidu.hugegraph.kafka.producer.StandardProducerBuilder;

/**
 * Example of using kafka client
 */
public class KafkaExample {
    private static final ProducerClient<String, ByteBuffer> producer = new StandardProducerBuilder()
                                                .setKafkaHost("127.0.0.1")
                                                .setKafkaPort("9092")
                                                .setTopic("hugegraph-nospace-default")
                                                .build();

    private static StandardConsumer consumer ;


    private static void init() {
        StandardConsumerBuilder builder = new StandardConsumerBuilder();
        builder
            .setKafkaHost("127.0.0.1")
            .setKafkaPort("9092")
            .setTopic("hugegraph-nospace-default");
        
        consumer = builder.build();
    }

    public static void main(String[] args) {

        init();

        try {
            produceExample().get();
        } catch (CancellationException | ExecutionException | InterruptedException e) {

        }

        consumeExample();

        producer.close();
        consumer.close();
    }

    private static Future<?> produceExample() throws InterruptedException, ExecutionException {
        String val = "{ \"key\": \"hello\", \"value\": \"world, this is raw binary test\"}";
        byte[] raw = val.getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(raw);
        return producer.produce("hello", buffer);
 
    }

    private static String consumeExample() {
        consumer.consume();
        return "";
    }

}
