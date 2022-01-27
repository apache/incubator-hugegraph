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
import java.time.Duration;
import java.util.Properties;

import com.baidu.hugegraph.kafka.producer.ProducerClient;
import com.baidu.hugegraph.kafka.producer.StandardProducerBuilder;
import com.baidu.hugegraph.kafka.topic.HugeGraphMutateTopic;
import com.baidu.hugegraph.kafka.topic.HugeGraphMutateTopicBuilder;
import com.baidu.hugegraph.kafka.topic.HugeGraphSyncTopicBuilder;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Used to consume HugeGraphSyncTopic then send the data to slave cluster
 * @author Scorpiour
 * @since 2022-01-25
 */
public class StandardConsumer extends ConsumerClient<String, ByteBuffer> {

    private final ProducerClient<String, ByteBuffer> producer = new StandardProducerBuilder().build();

    protected StandardConsumer(Properties props) {
        super(props);
    }

    @Override
    public void consume() {
        ConsumerRecords<String, ByteBuffer> records = this.consumer.poll(Duration.ofMillis(10000));
        if (records.count() > 0) {
           for(ConsumerRecord<String, ByteBuffer> record : records.records(this.topic)) {
                System.out.println(String.format("Going to consumer [%s]", record.key().toString()));

                String[] graphInfo = HugeGraphSyncTopicBuilder.extractGraphs(record);
                String graphSpace = graphInfo[0];
                String graphName = graphInfo[1];

                HugeGraphMutateTopic topic = new HugeGraphMutateTopicBuilder()
                                                    .setBuffer(record.value())
                                                    .setGraphSpace(graphSpace)
                                                    .setGraphName(graphName)
                                                    .build();

                System.out.println("=========> Scorpiour : resend data to next");
                producer.produce(topic);
           }
        }
        consumer.commitAsync();
    }
    
}
