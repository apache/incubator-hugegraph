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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.kafka.consumer.StandardConsumer;
import com.baidu.hugegraph.kafka.topic.HugeGraphSyncTopicBuilder;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class KafkaSyncConsumer extends StandardConsumer {

    private GraphManager manager;

    protected KafkaSyncConsumer(Properties props) {
        super(props);
    }

    protected void setGraphManager(GraphManager manager) {
        this.manager = manager;
    }

    @Override
    public void consume() {
        if (null == this.manager) {
            return;
        }
        ConsumerRecords<String, ByteBuffer> records = this.consumer.poll(Duration.ofMillis(1000));
        if (records.count() > 0) {
           for(ConsumerRecord<String, ByteBuffer> record : records.records(this.topic)) {
                System.out.println(String.format("Going to consumer [%s]", record.key().toString()));

                String[] graphInfo = HugeGraphSyncTopicBuilder.extractGraphs(record);
                String graphSpace = graphInfo[0];
                String graphName = graphInfo[1];

                HugeGraph graph = manager.graph(graphSpace, graphName);
                BackendMutation mutation = HugeGraphSyncTopicBuilder.buildMutation(record.value());
                graph.applyMutation(mutation);
                graph.tx().commit();
           }
        }
        consumer.commitAsync();
    }
    
}
